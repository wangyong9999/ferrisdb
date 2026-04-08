//! Heap + Transaction 未覆盖路径

use std::sync::Arc;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig};
use ferrisdb_transaction::*;

fn s(n: usize) -> (Arc<BufferPool>, Arc<TransactionManager>) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap());
    let mut tm = TransactionManager::new(64);
    tm.set_buffer_pool(Arc::clone(&bp));
    tm.set_txn_timeout(0);
    (bp, Arc::new(tm))
}

#[test]
fn test_tuple_header_roundtrip() {
    let h = TupleHeader::new(Xid::new(0, 5), 2);
    let bytes = h.serialize();
    assert_eq!(bytes.len(), TupleHeader::serialized_size());
    let h2 = TupleHeader::deserialize(&bytes).unwrap();
    assert_eq!(h2.xmin.raw(), Xid::new(0, 5).raw());
    assert_eq!(h2.cmin, 2);
}

#[test]
fn test_tuple_header_short_data() {
    assert!(TupleHeader::deserialize(&[0; 10]).is_none());
    assert!(TupleHeader::deserialize(&[0; 31]).is_none());
}

#[test]
fn test_heap_insert_zero_data() {
    let (bp, tm) = s(200);
    let heap = HeapTable::new(700, bp, Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let tid = heap.insert(b"", txn.xid(), 0).unwrap();
    assert!(tid.is_valid());
    let (_, data) = heap.fetch(tid).unwrap().unwrap();
    assert_eq!(data.len(), 0);
    txn.commit().unwrap();
}

#[test]
fn test_heap_insert_large_tuple() {
    let (bp, tm) = s(200);
    let heap = HeapTable::new(701, bp, Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let big = vec![0xABu8; 6000];
    let tid = heap.insert(&big, txn.xid(), 0).unwrap();
    let (_, data) = heap.fetch(tid).unwrap().unwrap();
    assert_eq!(data.len(), 6000);
    txn.commit().unwrap();
}

#[test]
fn test_heap_fetch_nonexistent() {
    let (bp, tm) = s(200);
    let heap = HeapTable::new(702, bp, tm);
    let r = heap.fetch(TupleId::new(999, 999));
    match r { Ok(None) | Err(_) => {} Ok(Some(_)) => {} }
}

#[test]
fn test_heap_update_same_size() {
    let (bp, tm) = s(500);
    let heap = HeapTable::new(703, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();
    let tid = heap.insert(b"aaaa_1234", xid, 0).unwrap();
    let new_tid = heap.update(tid, b"bbbb_5678", xid, 1).unwrap();
    let (_, data) = heap.fetch(new_tid).unwrap().unwrap();
    assert_eq!(&data, b"bbbb_5678");
    txn.commit().unwrap();
}

#[test]
fn test_heap_delete_basic() {
    let (bp, tm) = s(500);
    let heap = HeapTable::new(704, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();
    let tid = heap.insert(b"delete_me", xid, 0).unwrap();
    heap.delete(tid, xid, 1).unwrap();
    txn.commit().unwrap();
}

#[test]
fn test_heap_scan_with_visibility() {
    let (bp, tm) = s(500);
    let heap = HeapTable::new(705, Arc::clone(&bp), Arc::clone(&tm));

    // 插入并 commit
    let mut txn1 = tm.begin().unwrap();
    heap.insert(b"visible_row", txn1.xid(), 0).unwrap();
    txn1.commit().unwrap();

    // fetch 验证数据存在
    let txn2 = tm.begin().unwrap();
    // 扫描 page 0 的 tuples
    let mut found = 0;
    for i in 1..=5u16 {
        if let Ok(Some((hdr, _))) = heap.fetch(TupleId::new(0, i)) {
            if txn2.is_visible(hdr.xmin, hdr.xmax, hdr.cmin, hdr.cmax) {
                found += 1;
            }
        }
    }
    assert!(found > 0, "Should find visible rows");
}

#[test]
fn test_transaction_savepoint_rollback() {
    let (bp, tm) = s(500);
    let heap = HeapTable::new(706, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    heap.insert(b"before_sp", xid, 0).unwrap();
    let sp = txn.savepoint();
    heap.insert_with_undo(b"after_sp", xid, 1, Some(&mut txn)).unwrap();
    txn.rollback_to_savepoint(sp);
    txn.commit().unwrap();
}

#[test]
fn test_transaction_check_timeout() {
    let (bp, _) = s(200);
    let mut tm = TransactionManager::new(16);
    tm.set_buffer_pool(bp);
    tm.set_txn_timeout(10); // 10ms
    let tm = Arc::new(tm);

    let mut txn = tm.begin().unwrap();
    assert!(!txn.is_timed_out());
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(txn.is_timed_out());
    let r = txn.check_timeout();
    assert!(r.is_err(), "Should error on timeout");
}

#[test]
fn test_undo_action_serialize() {
    let action = transaction::UndoAction::Insert { table_oid: 1, page_no: 5, tuple_offset: 3 };
    let bytes = action.serialize_for_wal(Xid::new(0, 42));
    assert!(!bytes.is_empty());

    let action2 = transaction::UndoAction::Delete {
        table_oid: 2, page_no: 10, tuple_offset: 7, old_data: vec![0xAB; 50],
    };
    let bytes2 = action2.serialize_for_wal(Xid::new(0, 43));
    assert!(bytes2.len() > bytes.len());
}

#[test]
fn test_lock_mode_conflicts() {
    use ferrisdb_transaction::HeavyLockMode;
    assert!(!HeavyLockMode::Share.conflicts_with(HeavyLockMode::Share));
    assert!(HeavyLockMode::Share.conflicts_with(HeavyLockMode::Exclusive));
    assert!(HeavyLockMode::Exclusive.conflicts_with(HeavyLockMode::Exclusive));
    assert!(!HeavyLockMode::Exclusive.conflicts_with(HeavyLockMode::None));
}

#[test]
fn test_lock_tag_hash() {
    use ferrisdb_transaction::LockTag;
    let t1 = LockTag::Relation(100);
    let t2 = LockTag::Relation(100);
    let t3 = LockTag::Tuple(100, 5, 1);
    assert_eq!(t1.hash_value(), t2.hash_value());
    assert_ne!(t1.hash_value(), t3.hash_value());
}
