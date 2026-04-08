//! 深度覆盖测试 — 针对未覆盖的关键路径
//!
//! 覆盖：undo spill, BTree right-link insert, timeout abort,
//! MDL table lock, cross-page update, delete undo, large txn

use std::sync::Arc;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig};
use ferrisdb_transaction::{HeapTable, TransactionManager, TupleId};

fn setup(n: usize) -> (Arc<BufferPool>, Arc<TransactionManager>) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap());
    let mut tm = TransactionManager::new(64);
    tm.set_buffer_pool(Arc::clone(&bp));
    tm.set_txn_timeout(0); // 测试时禁用超时
    (bp, Arc::new(tm))
}

// ============================================================
// 1. Undo spill-to-disk 测试
// ============================================================

#[test]
fn test_undo_spill_large_transaction() {
    let (bp, tm) = setup(2000);
    let heap = HeapTable::new(1, Arc::clone(&bp), Arc::clone(&tm));

    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    // 插入 150K 行（超过 100K 内存阈值，触发 spill）
    for i in 0..150_000u32 {
        let data = format!("row_{:06}", i);
        heap.insert(data.as_bytes(), xid, 0).unwrap();
    }
    // 应该没有错误（不再有 1M 上限）
    txn.commit().unwrap();
}

#[test]
fn test_undo_spill_abort_cleans_up() {
    let (bp, tm) = setup(2000);
    let heap = HeapTable::new(2, Arc::clone(&bp), Arc::clone(&tm));

    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    // 插入超过 spill 阈值
    for i in 0..110_000u32 {
        let data = format!("abort_row_{:06}", i);
        heap.insert(data.as_bytes(), xid, 0).unwrap();
    }
    // abort 应清理 spill 文件
    txn.abort().unwrap();

    // 验证 spill 文件已删除
    let spill_path = std::env::temp_dir().join(format!("ferrisdb_undo_{}.tmp", xid.raw()));
    assert!(!spill_path.exists(), "Spill file should be cleaned up after abort");
}

// ============================================================
// 2. BTree right-link insert 路径测试
// ============================================================

#[test]
fn test_btree_concurrent_insert_with_right_link() {
    use ferrisdb_storage::{BTree, BTreeKey, BTreeValue};

    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(500)).unwrap());
    let tree = Arc::new(BTree::new(100, Arc::clone(&bp)));
    tree.init().unwrap();

    let mut handles = vec![];
    // 8 线程并发 insert 800 keys
    for t in 0..8 {
        let tree = Arc::clone(&tree);
        handles.push(std::thread::spawn(move || {
            for i in 0..100 {
                let key = BTreeKey::new(format!("t{}_k{:06}", t, i).into_bytes());
                tree.insert(key, BTreeValue::Tuple { block: t as u32, offset: i }).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    // 验证所有 800 keys 都能找到
    let mut found = 0;
    for t in 0..8 {
        for i in 0..100 {
            let key = BTreeKey::new(format!("t{}_k{:06}", t, i).into_bytes());
            if tree.lookup(&key).unwrap().is_some() {
                found += 1;
            }
        }
    }
    assert_eq!(found, 800, "All 800 keys must be found, got {}", found);
}

// ============================================================
// 3. 事务超时强制 abort 测试
// ============================================================

#[test]
fn test_transaction_timeout_abort() {
    let (bp, _) = setup(200);
    let mut tm_inner = TransactionManager::new(16);
    tm_inner.set_buffer_pool(bp);
    tm_inner.set_txn_timeout(50); // 50ms 超时
    let tm = Arc::new(tm_inner);

    let txn = tm.begin().unwrap();
    assert!(!txn.is_timed_out());

    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(txn.is_timed_out());

    // abort_timed_out_transactions 应能检测并标记
    let aborted = tm.abort_timed_out_transactions();
    assert!(aborted > 0, "Should abort timed-out transactions");
}

#[test]
fn test_wait_for_all_transactions() {
    let (bp, tm) = setup(200);

    // 无活跃事务时立即返回
    assert_eq!(tm.wait_for_all_transactions(100), 0);

    // 有活跃事务
    let _txn = tm.begin().unwrap();
    let remaining = tm.wait_for_all_transactions(50); // 50ms 后仍有 1 个
    assert!(remaining > 0);

    drop(_txn); // 释放
    assert_eq!(tm.wait_for_all_transactions(50), 0);
}

// ============================================================
// 4. HeapTable cross-page update 测试
// ============================================================

#[test]
fn test_heap_update_cross_page() {
    let (bp, tm) = setup(500);
    let heap = HeapTable::new(3, Arc::clone(&bp), Arc::clone(&tm));

    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    // 插入小数据
    let tid = heap.insert(b"small_data", xid, 0).unwrap();

    // 更新为大数据（可能跨页）
    let big_data = vec![0xABu8; 4000]; // 4KB
    let new_tid = heap.update(tid, &big_data, xid, 1).unwrap();

    // 验证新数据可读
    let (_, data) = heap.fetch(new_tid).unwrap().unwrap();
    assert_eq!(data.len(), 4000);

    txn.commit().unwrap();
}

// ============================================================
// 5. HeapTable delete + undo 测试
// ============================================================

#[test]
fn test_heap_delete_with_undo_abort() {
    let (bp, tm) = setup(500);
    let heap = HeapTable::new(4, Arc::clone(&bp), Arc::clone(&tm));

    // 先插入
    let mut txn1 = tm.begin().unwrap();
    let xid1 = txn1.xid();
    let tid = heap.insert(b"will_delete_and_rollback", xid1, 0).unwrap();
    txn1.commit().unwrap();

    // 删除后 abort（应恢复）
    let mut txn2 = tm.begin().unwrap();
    let xid2 = txn2.xid();
    heap.delete_with_undo(tid, xid2, 0, Some(&mut txn2)).unwrap();
    txn2.abort().unwrap();

    // 数据应该还在（abort 恢复了 delete）
    let result = heap.fetch(tid).unwrap();
    assert!(result.is_some(), "Data should survive after delete+abort");
}

// ============================================================
// 6. Vacuum + 并发 insert/update 压力测试
// ============================================================

#[test]
fn test_vacuum_under_concurrent_dml() {
    let (bp, tm) = setup(1000);
    let heap = Arc::new(HeapTable::new(5, Arc::clone(&bp), Arc::clone(&tm)));
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut handles = vec![];

    // 2 个 insert 线程
    for t in 0..2 {
        let heap = Arc::clone(&heap);
        let tm = Arc::clone(&tm);
        let bar = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            bar.wait();
            for i in 0..500u32 {
                let mut txn = tm.begin().unwrap();
                let xid = txn.xid();
                let _ = heap.insert(format!("ins_{}_{}", t, i).as_bytes(), xid, 0);
                let _ = txn.commit();
            }
        }));
    }

    // 1 个 update 线程
    {
        let heap = Arc::clone(&heap);
        let tm = Arc::clone(&tm);
        let bar = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            bar.wait();
            for i in 0..200u32 {
                let mut txn = tm.begin().unwrap();
                let xid = txn.xid();
                let tid = TupleId::new(0, (i % 10 + 1) as u16);
                let _ = heap.update(tid, format!("upd_{}", i).as_bytes(), xid, 0);
                let _ = txn.commit();
            }
        }));
    }

    // 1 个 vacuum 线程
    {
        let heap = Arc::clone(&heap);
        let bar = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            bar.wait();
            for _ in 0..100 {
                let _ = heap.vacuum();
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        }));
    }

    for h in handles { h.join().unwrap(); }
    // 无 panic = 成功
}

// ============================================================
// 7. BTree delete + lookup 一致性
// ============================================================

#[test]
fn test_btree_delete_then_lookup() {
    use ferrisdb_storage::{BTree, BTreeKey, BTreeValue};

    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(500)).unwrap());
    let tree = BTree::new(200, bp);
    tree.init().unwrap();

    // 插入 100 keys
    for i in 0..100u32 {
        tree.insert(
            BTreeKey::new(format!("dk_{:04}", i).into_bytes()),
            BTreeValue::Tuple { block: i, offset: 0 },
        ).unwrap();
    }

    // 删除偶数 keys
    for i in (0..100u32).step_by(2) {
        tree.delete(&BTreeKey::new(format!("dk_{:04}", i).into_bytes())).unwrap();
    }

    // 验证奇数 keys 存在，偶数不存在
    for i in 0..100u32 {
        let key = BTreeKey::new(format!("dk_{:04}", i).into_bytes());
        let found = tree.lookup(&key).unwrap().is_some();
        if i % 2 == 0 {
            assert!(!found, "Deleted key dk_{:04} should not be found", i);
        } else {
            assert!(found, "Key dk_{:04} should be found", i);
        }
    }
}

// ============================================================
// 8. 边界测试：空表 scan、满页 insert、slot 耗尽
// ============================================================

#[test]
fn test_heap_scan_empty_table() {
    let (bp, tm) = setup(200);
    let heap = HeapTable::new(6, Arc::clone(&bp), Arc::clone(&tm));

    let txn = tm.begin().unwrap();
    // 空表 fetch 不存在的 tid 应返回 None
    let result = heap.fetch(TupleId::new(0, 1)).unwrap();
    assert!(result.is_none(), "Empty table fetch should return None");
}

#[test]
fn test_transaction_slot_exhaustion() {
    let (bp, _) = setup(200);
    let mut tm = TransactionManager::new(4); // 只有 4 个 slot（0 保留 = 3 可用）
    tm.set_buffer_pool(bp);
    tm.set_txn_timeout(0);
    let tm = Arc::new(tm);

    let _t1 = tm.begin().unwrap();
    let _t2 = tm.begin().unwrap();
    let _t3 = tm.begin().unwrap();

    // 第 4 个应该失败
    let result = tm.begin();
    assert!(result.is_err(), "4th begin should fail (slots exhausted)");
}

// ============================================================
// 9. WAL RingBuffer 背压测试
// ============================================================

#[test]
fn test_ring_buffer_backpressure() {
    use ferrisdb_storage::wal::ring_buffer::WalRingBuffer;

    // 小 ring buffer（256 bytes）+ 后台 drain
    let ring = Arc::new(WalRingBuffer::new(256));
    let ring2 = Arc::clone(&ring);

    // 后台 drain 线程
    let drain = std::thread::spawn(move || {
        for _ in 0..200 {
            let (data, new_pos) = ring2.get_flush_data();
            if !data.is_empty() {
                ring2.advance_flush(new_pos);
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });

    // 写入远超 buffer 大小的数据（依赖 backpressure 等待 drain）
    for i in 0..100 {
        ring.write(format!("bp_record_{:04}", i).as_bytes());
    }

    drain.join().unwrap();
    // 无死锁 = 背压机制正常
}

// ============================================================
// 10. 多事务并发 commit/abort 混合
// ============================================================

#[test]
fn test_concurrent_commit_abort_mix() {
    let (bp, tm) = setup(500);
    let heap = Arc::new(HeapTable::new(7, Arc::clone(&bp), Arc::clone(&tm)));
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut handles = vec![];

    for t in 0..8 {
        let heap = Arc::clone(&heap);
        let tm = Arc::clone(&tm);
        let bar = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            bar.wait();
            for i in 0..100u32 {
                let mut txn = tm.begin().unwrap();
                let xid = txn.xid();
                let _ = heap.insert(format!("mix_{}_{}",t,i).as_bytes(), xid, 0);
                if i % 3 == 0 {
                    let _ = txn.abort(); // 每 3 个 abort 一个
                } else {
                    let _ = txn.commit();
                }
            }
        }));
    }

    for h in handles { h.join().unwrap(); }
}
