//! Heap 集成测试
//!
//! 测试真实的 Heap 存储操作。

use std::sync::Arc;
use tempfile::TempDir;

fn make_txn_mgr() -> Arc<ferrisdb_transaction::TransactionManager> {
    Arc::new(ferrisdb_transaction::TransactionManager::new(256))
}

fn make_txn_mgr_with_bp(bp: Arc<ferrisdb_storage::BufferPool>) -> Arc<ferrisdb_transaction::TransactionManager> {
    let mut mgr = ferrisdb_transaction::TransactionManager::new(256);
    mgr.set_buffer_pool(bp);
    Arc::new(mgr)
}

#[test]
fn test_heap_insert_and_fetch() {
    use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
    use ferrisdb_transaction::{HeapTable, TupleHeader, TupleId};
    use ferrisdb_core::Xid;

    // 创建临时目录
    let temp_dir = TempDir::new().unwrap();

    // 创建 Storage Manager
    let smgr = Arc::new(StorageManager::new(temp_dir.path()));
    smgr.init().unwrap();

    // 创建 Buffer Pool
    let mut config = BufferPoolConfig::new(100);
    let mut buffer_pool = BufferPool::new(config).unwrap();
    buffer_pool.set_smgr(smgr);
    let buffer_pool = Arc::new(buffer_pool);

    // 创建 HeapTable
    let table = HeapTable::new(1, buffer_pool, make_txn_mgr());

    // 插入元组
    let data1 = b"Hello, World!";
    let tid1 = table.insert(data1, Xid::new(0, 1), 0).unwrap();

    // 验证元组 ID 有效
    assert!(tid1.is_valid());

    // 读取元组
    let (header1, read_data1) = table.fetch(tid1).unwrap().unwrap();
    assert_eq!(read_data1, data1);
    assert_eq!(header1.xmin.raw(), Xid::new(0, 1).raw());

    // 插入更多元组
    let data2 = b"Second tuple";
    let tid2 = table.insert(data2, Xid::new(0, 2), 0).unwrap();

    let (_, read_data2) = table.fetch(tid2).unwrap().unwrap();
    assert_eq!(read_data2, data2);

    // 第一个元组仍然可读
    let (_, read_data1_again) = table.fetch(tid1).unwrap().unwrap();
    assert_eq!(read_data1_again, data1);
}

#[test]
fn test_heap_update() {
    use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
    use ferrisdb_transaction::{HeapTable, TupleHeader};
    use ferrisdb_core::Xid;

    let temp_dir = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(temp_dir.path()));
    smgr.init().unwrap();

    let mut buffer_pool = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
    buffer_pool.set_smgr(smgr);
    let buffer_pool = Arc::new(buffer_pool);

    let table = HeapTable::new(2, buffer_pool, make_txn_mgr());

    // 插入
    let data1 = b"Original data";
    let tid1 = table.insert(data1, Xid::new(0, 1), 0).unwrap();

    // 更新
    let data2 = b"Updated data";
    let tid2 = table.update(tid1, data2, Xid::new(0, 2), 0).unwrap();

    // fetch(tid1) follows ctid chain to latest version (tid2)
    let (_, fetched_via_old) = table.fetch(tid1).unwrap().unwrap();
    assert_eq!(fetched_via_old, data2, "fetch should follow ctid to latest version");

    // fetch(tid2) directly gets new data
    let (_, read_data2) = table.fetch(tid2).unwrap().unwrap();
    assert_eq!(read_data2, data2);
}

#[test]
fn test_heap_delete() {
    use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
    use ferrisdb_transaction::{HeapTable, TupleHeader};
    use ferrisdb_core::Xid;

    let temp_dir = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(temp_dir.path()));
    smgr.init().unwrap();

    let mut buffer_pool = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
    buffer_pool.set_smgr(smgr);
    let buffer_pool = Arc::new(buffer_pool);

    let table = HeapTable::new(3, buffer_pool, make_txn_mgr());

    // 插入
    let data = b"To be deleted";
    let tid = table.insert(data, Xid::new(0, 1), 0).unwrap();

    // 删除
    table.delete(tid, Xid::new(0, 2), 0).unwrap();

    // 验证元组已被标记为删除
    let (header, _) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(header.xmax.raw(), Xid::new(0, 2).raw());
}

#[test]
fn test_heap_scan() {
    use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
    use ferrisdb_transaction::{HeapTable, HeapScan};
    use ferrisdb_core::Xid;

    let temp_dir = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(temp_dir.path()));
    smgr.init().unwrap();

    let mut buffer_pool = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
    buffer_pool.set_smgr(smgr);
    let buffer_pool = Arc::new(buffer_pool);

    let table = HeapTable::new(4, buffer_pool, make_txn_mgr());

    // 插入多个元组
    let data1 = b"Tuple 1";
    let data2 = b"Tuple 2";
    let data3 = b"Tuple 3";

    table.insert(data1, Xid::new(0, 1), 0).unwrap();
    table.insert(data2, Xid::new(0, 2), 0).unwrap();
    table.insert(data3, Xid::new(0, 3), 0).unwrap();

    // 扫描所有元组
    let mut scan = HeapScan::new(&table);
    let mut count = 0;

    while let Ok(Some((tid, header, data))) = scan.next() {
        count += 1;
        // 验证数据
        assert!(data == data1 || data == data2 || data == data3);
    }

    assert_eq!(count, 3);
}

#[test]
fn test_heap_large_data() {
    use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
    use ferrisdb_transaction::HeapTable;
    use ferrisdb_core::Xid;

    let temp_dir = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(temp_dir.path()));
    smgr.init().unwrap();

    let mut buffer_pool = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
    buffer_pool.set_smgr(smgr);
    let buffer_pool = Arc::new(buffer_pool);

    let table = HeapTable::new(5, buffer_pool, make_txn_mgr());

    // 插入大量数据
    for i in 0..100 {
        let data = format!("Data row {}", i);
        let tid = table.insert(data.as_bytes(), Xid::new(0, (i + 1) as u32), 0).unwrap();
        assert!(tid.is_valid());
    }

    // 验证数据
    let first_tid = table.insert(b"First", Xid::new(0, 101), 0).unwrap();
    let (_, data) = table.fetch(first_tid).unwrap().unwrap();
    assert_eq!(data, b"First");
}

// =============================================================================
// Undo Rollback Integration Tests
// =============================================================================

#[test]
fn test_abort_insert_rollback() {
    use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
    use ferrisdb_transaction::HeapTable;
    use ferrisdb_core::Xid;

    let temp_dir = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(temp_dir.path()));
    smgr.init().unwrap();

    let mut bp = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
    bp.set_smgr(smgr);
    let bp = Arc::new(bp);

    let txn_mgr = make_txn_mgr_with_bp(Arc::clone(&bp));
    let table = HeapTable::new(10, Arc::clone(&bp), Arc::clone(&txn_mgr));

    // Begin transaction, insert, then abort
    let mut txn = txn_mgr.begin().unwrap();
    let xid = txn.xid();

    let tid = table.insert_with_undo(b"should disappear", xid, 0, Some(&mut txn)).unwrap();

    // Before abort: tuple exists
    let result = table.fetch(tid).unwrap();
    assert!(result.is_some());

    // Abort → undo should mark tuple as dead
    txn.abort().unwrap();

    // After abort: tuple should be marked dead (fetch returns None or dead header)
    let result = table.fetch(tid).unwrap();
    // mark_dead sets the item pointer to Dead, so get_tuple returns None
    assert!(result.is_none(), "Insert should be rolled back after abort");
}

#[test]
fn test_abort_delete_rollback() {
    use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
    use ferrisdb_transaction::HeapTable;
    use ferrisdb_core::Xid;

    let temp_dir = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(temp_dir.path()));
    smgr.init().unwrap();

    let mut bp = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
    bp.set_smgr(smgr);
    let bp = Arc::new(bp);

    let txn_mgr = make_txn_mgr_with_bp(Arc::clone(&bp));
    let table = HeapTable::new(11, Arc::clone(&bp), Arc::clone(&txn_mgr));

    // First insert a tuple with committed transaction
    let insert_xid = Xid::new(0, 50);  // fake committed xid
    let tid = table.insert(b"precious data", insert_xid, 0).unwrap();

    // Now begin a new transaction, delete, then abort
    let mut txn = txn_mgr.begin().unwrap();
    let xid = txn.xid();

    table.delete_with_undo(tid, xid, 0, Some(&mut txn)).unwrap();

    // Before abort: tuple has xmax set
    let (hdr, _) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(hdr.xmax.raw(), xid.raw());

    // Abort → undo should restore old data (clearing xmax)
    txn.abort().unwrap();

    // After abort: tuple should have original header restored (xmax cleared)
    let (hdr_after, data_after) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(data_after, b"precious data");
    assert_eq!(hdr_after.xmax.raw(), Xid::INVALID.raw(), "xmax should be cleared after abort");
}

#[test]
fn test_abort_inplace_update_rollback() {
    use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
    use ferrisdb_transaction::HeapTable;
    use ferrisdb_core::Xid;

    let temp_dir = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(temp_dir.path()));
    smgr.init().unwrap();

    let mut bp = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
    bp.set_smgr(smgr);
    let bp = Arc::new(bp);

    let txn_mgr = make_txn_mgr_with_bp(Arc::clone(&bp));
    let table = HeapTable::new(12, Arc::clone(&bp), Arc::clone(&txn_mgr));

    // Insert with same-sized data to trigger HOT (in-place) update
    let original = b"AAAA_original";
    let insert_xid = Xid::new(0, 50);
    let tid = table.insert(original, insert_xid, 0).unwrap();

    // Begin transaction, do in-place update (same size), then abort
    let mut txn = txn_mgr.begin().unwrap();
    let xid = txn.xid();

    let updated = b"BBBB_replaced";
    assert_eq!(original.len(), updated.len()); // same size → HOT update
    let new_tid = table.update_with_undo(tid, updated, xid, 0, Some(&mut txn)).unwrap();
    assert_eq!(new_tid, tid); // HOT update returns same TupleId

    // Before abort: data should be updated
    let (_, data_before) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(data_before, updated);

    // Abort → undo should restore original data
    txn.abort().unwrap();

    // After abort: original data restored
    let (hdr, data_after) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(data_after, original, "Data should be restored after abort");
    assert_eq!(hdr.xmin.raw(), insert_xid.raw(), "xmin should be original inserter");
}

#[test]
fn test_savepoint_rollback() {
    use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
    use ferrisdb_transaction::HeapTable;
    use ferrisdb_core::Xid;

    let temp_dir = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(temp_dir.path()));
    smgr.init().unwrap();

    let mut bp = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
    bp.set_smgr(smgr);
    let bp = Arc::new(bp);

    let txn_mgr = make_txn_mgr_with_bp(Arc::clone(&bp));
    let table = HeapTable::new(20, Arc::clone(&bp), Arc::clone(&txn_mgr));

    let mut txn = txn_mgr.begin().unwrap();
    let xid = txn.xid();

    // Insert row 1
    let tid1 = table.insert_with_undo(b"row 1 - keep", xid, 0, Some(&mut txn)).unwrap();

    // Create savepoint
    let sp = txn.savepoint();

    // Insert row 2 (after savepoint)
    let tid2 = table.insert_with_undo(b"row 2 - discard", xid, 0, Some(&mut txn)).unwrap();

    // Both visible before rollback
    assert!(table.fetch(tid1).unwrap().is_some());
    assert!(table.fetch(tid2).unwrap().is_some());

    // Rollback to savepoint — row 2 should be undone
    txn.rollback_to_savepoint(sp);

    // Row 1 still visible, row 2 marked dead
    assert!(table.fetch(tid1).unwrap().is_some(), "Row 1 should survive savepoint rollback");
    assert!(table.fetch(tid2).unwrap().is_none(), "Row 2 should be undone by savepoint rollback");

    // Can still commit (row 1 persists)
    txn.commit().unwrap();
}
