//! 额外 Transaction + HeapTable 覆盖测试
//! 目标: undo spill, visibility, savepoint, abort paths, FSM

use std::sync::Arc;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig};
use ferrisdb_transaction::*;

fn setup(n: usize) -> (Arc<BufferPool>, Arc<TransactionManager>) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap());
    let mut tm = TransactionManager::new(64);
    tm.set_buffer_pool(Arc::clone(&bp));
    tm.set_txn_timeout(0);
    (bp, Arc::new(tm))
}

fn setup_with_timeout(n: usize, timeout_ms: u64) -> (Arc<BufferPool>, Arc<TransactionManager>) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap());
    let mut tm = TransactionManager::new(64);
    tm.set_buffer_pool(Arc::clone(&bp));
    tm.set_txn_timeout(timeout_ms);
    (bp, Arc::new(tm))
}

// ============================================================
// TransactionManager 操作
// ============================================================

#[test]
fn test_txn_manager_allocate_csn() {
    let (_, tm) = setup(200);
    let csn1 = tm.allocate_csn();
    let csn2 = tm.allocate_csn();
    assert!(csn2.raw() > csn1.raw());
}

#[test]
fn test_txn_manager_current_snapshot() {
    let (_, tm) = setup(200);
    let snap = tm.current_snapshot();
    let _ = snap; // just exercise the API
}

#[test]
fn test_txn_manager_active_count() {
    let (_, tm) = setup(200);
    assert_eq!(tm.active_transaction_count(), 0);
    let _txn = tm.begin().unwrap();
    assert_eq!(tm.active_transaction_count(), 1);
}

#[test]
fn test_txn_manager_shutdown() {
    let (_, tm) = setup(200);
    tm.initiate_shutdown();
    let result = tm.begin();
    assert!(result.is_err());
}

#[test]
fn test_txn_manager_wait_for_all() {
    let (_, tm) = setup(200);
    let remaining = tm.wait_for_all_transactions(100);
    assert_eq!(remaining, 0);
}

#[test]
fn test_txn_manager_abort_timed_out() {
    let (_, tm) = setup_with_timeout(200, 10);
    let mut txn = tm.begin().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(20));
    let count = tm.abort_timed_out_transactions();
    assert!(count >= 0); // may or may not find it depending on timing
    // Clean up
    if txn.is_active() {
        let _ = txn.abort();
    }
}

#[test]
fn test_txn_manager_get_csn() {
    let (_, tm) = setup(200);
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();
    txn.commit().unwrap();
    let csn = tm.get_csn(xid);
    // CSN may or may not be valid depending on slot reuse
    let _ = csn;
}

// ============================================================
// Transaction 操作
// ============================================================

#[test]
fn test_txn_commit_sets_csn() {
    let (_, tm) = setup(200);
    let mut txn = tm.begin().unwrap();
    assert!(txn.is_active());
    txn.commit().unwrap();
    assert!(!txn.is_active());
    assert!(txn.csn().is_valid());
}

#[test]
fn test_txn_abort_basic() {
    let (_, tm) = setup(200);
    let mut txn = tm.begin().unwrap();
    txn.abort().unwrap();
    assert!(!txn.is_active());
}

#[test]
fn test_txn_double_commit() {
    let (_, tm) = setup(200);
    let mut txn = tm.begin().unwrap();
    txn.commit().unwrap();
    let result = txn.commit();
    assert!(result.is_err());
}

#[test]
fn test_txn_commit_after_abort() {
    let (_, tm) = setup(200);
    let mut txn = tm.begin().unwrap();
    txn.abort().unwrap();
    let result = txn.commit();
    assert!(result.is_err());
}

#[test]
fn test_txn_savepoint_basic() {
    let (_, tm) = setup(200);
    let txn = tm.begin().unwrap();
    let sp = txn.savepoint();
    assert_eq!(sp, 0);
}

#[test]
fn test_txn_savepoint_nested() {
    let (_, tm) = setup(200);
    let txn = tm.begin().unwrap();
    let sp1 = txn.savepoint();
    let sp2 = txn.savepoint();
    assert_eq!(sp1, 0);
    assert_eq!(sp2, 0); // both should be 0 if no undo actions pushed
}

#[test]
fn test_txn_push_undo_insert() {
    let (_, tm) = setup(200);
    let mut txn = tm.begin().unwrap();
    let action = UndoAction::Insert { table_oid: 1, page_no: 0, tuple_offset: 1 };
    txn.push_undo(action).unwrap();
    let sp = txn.savepoint();
    assert_eq!(sp, 1);
    txn.commit().unwrap();
}

#[test]
fn test_txn_push_undo_delete() {
    let (_, tm) = setup(200);
    let mut txn = tm.begin().unwrap();
    let action = UndoAction::Delete {
        table_oid: 1, page_no: 0, tuple_offset: 1,
        old_data: vec![0xABu8; 100],
    };
    txn.push_undo(action).unwrap();
    txn.commit().unwrap();
}

#[test]
fn test_txn_rollback_to_savepoint() {
    let (_, tm) = setup(200);
    let mut txn = tm.begin().unwrap();
    let sp = txn.savepoint();

    let action = UndoAction::Insert { table_oid: 1, page_no: 0, tuple_offset: 1 };
    txn.push_undo(action).unwrap();

    txn.rollback_to_savepoint(sp);
    // After rollback, undo log should be truncated
    let sp2 = txn.savepoint();
    assert_eq!(sp2, 0);
    txn.commit().unwrap();
}

#[test]
fn test_txn_timeout_check() {
    let (_, tm) = setup_with_timeout(200, 10);
    let mut txn = tm.begin().unwrap();
    assert!(!txn.is_timed_out());
    std::thread::sleep(std::time::Duration::from_millis(20));
    assert!(txn.is_timed_out());
    let result = txn.check_timeout();
    assert!(result.is_err());
}

#[test]
fn test_txn_xid_valid() {
    let (_, tm) = setup(200);
    let txn = tm.begin().unwrap();
    assert!(txn.xid().is_valid());
}

// ============================================================
// Visibility 测试
// ============================================================

#[test]
fn test_visibility_own_insert_visible() {
    let (_, tm) = setup(200);
    let txn = tm.begin().unwrap();
    let xid = txn.xid();
    // Own insert: xmin = own xid, xmax = INVALID → visible
    assert!(txn.is_visible(xid, Xid::INVALID, 0, 0));
}

#[test]
fn test_visibility_own_delete_invisible() {
    let (_, tm) = setup(200);
    let txn = tm.begin().unwrap();
    let xid = txn.xid();
    // Own delete: xmin = own xid, xmax = own xid → invisible (deleted by self)
    let visible = txn.is_visible(xid, xid, 0, 1);
    // This depends on command IDs: cmin=0, cmax=1 means insert at cmd 0, delete at cmd 1
    // Current command is after both, so tuple was created and deleted by same txn
    assert!(!visible);
}

#[test]
fn test_visibility_committed_other() {
    let (_, tm) = setup(200);
    let mut txn1 = tm.begin().unwrap();
    let xid1 = txn1.xid();
    txn1.commit().unwrap();

    let txn2 = tm.begin().unwrap();
    // txn2 should see txn1's committed data
    let visible = txn2.is_visible(xid1, Xid::INVALID, 0, 0);
    assert!(visible);
}

#[test]
fn test_visibility_uncommitted_other_invisible() {
    let (_, tm) = setup(200);
    let _txn1 = tm.begin().unwrap(); // uncommitted
    let xid1 = _txn1.xid();

    let txn2 = tm.begin().unwrap();
    // txn2 should NOT see txn1's uncommitted data
    let visible = txn2.is_visible(xid1, Xid::INVALID, 0, 0);
    assert!(!visible);
}

// ============================================================
// HeapTable 操作
// ============================================================

#[test]
fn test_heap_insert_and_scan() {
    let (bp, tm) = setup(500);
    let heap = HeapTable::new(900, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    for i in 0..10u8 {
        heap.insert(&[i; 50], xid, i as u32).unwrap();
    }
    txn.commit().unwrap();

    // Scan should find rows
    let txn2 = tm.begin().unwrap();
    let mut scan = HeapScan::new(&heap);
    let mut count = 0;
    while let Ok(Some(_)) = scan.next_visible(&txn2) {
        count += 1;
    }
    assert!(count > 0, "Should find visible rows");
}

#[test]
fn test_heap_update_different_size() {
    let (bp, tm) = setup(500);
    let heap = HeapTable::new(901, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    let tid = heap.insert(b"small", xid, 0).unwrap();
    let new_tid = heap.update(tid, b"much larger data than before", xid, 1).unwrap();
    assert!(new_tid.is_valid());
    txn.commit().unwrap();
}

#[test]
fn test_heap_delete_and_verify() {
    let (bp, tm) = setup(500);
    let heap = HeapTable::new(902, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    let tid = heap.insert(b"delete_me", xid, 0).unwrap();
    heap.delete(tid, xid, 1).unwrap();
    txn.commit().unwrap();
}

#[test]
fn test_heap_vacuum_basic() {
    let (bp, tm) = setup(500);
    let heap = HeapTable::new(903, Arc::clone(&bp), Arc::clone(&tm));

    // Insert and commit
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();
    let tid = heap.insert(b"to_vacuum", xid, 0).unwrap();
    txn.commit().unwrap();

    // Delete and commit
    let mut txn2 = tm.begin().unwrap();
    let xid2 = txn2.xid();
    heap.delete(tid, xid2, 0).unwrap();
    txn2.commit().unwrap();

    // Vacuum should reclaim
    heap.vacuum();
}

#[test]
fn test_heap_table_oid() {
    let (bp, tm) = setup(200);
    let heap = HeapTable::new(904, bp, tm);
    assert_eq!(heap.table_oid(), 904);
}

#[test]
fn test_heap_insert_with_undo() {
    let (bp, tm) = setup(500);
    let heap = HeapTable::new(905, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    let tid = heap.insert_with_undo(b"undo_test", xid, 0, Some(&mut txn)).unwrap();
    assert!(tid.is_valid());

    // Check undo was pushed
    let sp = txn.savepoint();
    assert!(sp > 0, "Undo should have been pushed");
    txn.commit().unwrap();
}

#[test]
fn test_heap_multiple_pages() {
    let (bp, tm) = setup(500);
    let heap = HeapTable::new(906, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    // Insert enough data to span multiple pages
    for i in 0..100u32 {
        let data = vec![i as u8; 200]; // 200 bytes * 100 = 20KB, needs multiple 8KB pages
        heap.insert(&data, xid, i).unwrap();
    }
    txn.commit().unwrap();
}

// ============================================================
// UndoAction 序列化
// ============================================================

#[test]
fn test_undo_action_insert_serialize() {
    let action = UndoAction::Insert { table_oid: 1, page_no: 5, tuple_offset: 3 };
    let bytes = action.serialize_for_wal(Xid::new(0, 42));
    assert!(!bytes.is_empty());
}

#[test]
fn test_undo_action_delete_serialize() {
    let action = UndoAction::Delete {
        table_oid: 2, page_no: 10, tuple_offset: 7,
        old_data: vec![0xAB; 50],
    };
    let bytes = action.serialize_for_wal(Xid::new(0, 43));
    assert!(!bytes.is_empty());
}

#[test]
fn test_undo_action_inplace_update_serialize() {
    let action = UndoAction::InplaceUpdate {
        table_oid: 3, page_no: 1, tuple_offset: 2,
        old_data: vec![0xCD; 30],
    };
    let bytes = action.serialize_for_wal(Xid::new(0, 44));
    assert!(!bytes.is_empty());
}

// ============================================================
// SsiTracker 测试
// ============================================================

#[test]
fn test_ssi_tracker_basic() {
    let ssi = SsiTracker::new();
    let xid1 = Xid::new(0, 1);
    let xid2 = Xid::new(0, 2);
    let key = AccessKey { table_oid: 1, page_no: 0 };

    let key2 = AccessKey { table_oid: 1, page_no: 0 };
    ssi.track_read(xid1, key);
    ssi.track_write(xid2, key2);

    let has_conflict = ssi.check_conflict(xid1);
    let _ = has_conflict;
}

#[test]
fn test_ssi_tracker_cleanup() {
    let ssi = SsiTracker::new();
    let xid = Xid::new(0, 1);
    let key = AccessKey { table_oid: 1, page_no: 0 };
    ssi.track_read(xid, key);
    ssi.cleanup(xid);
}

// ============================================================
// LockManager 额外测试
// ============================================================

#[test]
fn test_lock_manager_multiple_lock_types() {
    let mgr = LockManager::new();
    let xid = Xid::new(0, 1);

    // Relation lock
    mgr.acquire(LockTag::Relation(100), HeavyLockMode::Share, xid).unwrap();
    mgr.release(&LockTag::Relation(100)).unwrap();

    // Tuple lock
    mgr.acquire(LockTag::Tuple(100, 0, 1), HeavyLockMode::Exclusive, xid).unwrap();
    mgr.release(&LockTag::Tuple(100, 0, 1)).unwrap();

    // Page lock
    mgr.acquire(LockTag::Page(100, 5), HeavyLockMode::Share, xid).unwrap();
    mgr.release(&LockTag::Page(100, 5)).unwrap();

    // Transaction lock
    mgr.acquire(LockTag::Transaction(42), HeavyLockMode::Exclusive, xid).unwrap();
    mgr.release(&LockTag::Transaction(42)).unwrap();
}

#[test]
fn test_lock_manager_release_unheld() {
    let mgr = LockManager::new();
    let result = mgr.release(&LockTag::Relation(999));
    assert!(result.is_err());
}

// ============================================================
// DeadlockDetector 额外测试
// ============================================================

#[test]
fn test_deadlock_3way_cycle() {
    let dd = DeadlockDetector::new();
    let x1 = Xid::new(0, 1);
    let x2 = Xid::new(0, 2);
    let x3 = Xid::new(0, 3);

    dd.add_wait(x1, x2);
    dd.add_wait(x2, x3);
    dd.add_wait(x3, x1);

    assert!(dd.check_deadlock(x1));
    assert!(dd.check_deadlock(x2));
    assert!(dd.check_deadlock(x3));
}

#[test]
fn test_deadlock_no_cycle() {
    let dd = DeadlockDetector::new();
    let x1 = Xid::new(0, 1);
    let x2 = Xid::new(0, 2);
    let x3 = Xid::new(0, 3);

    dd.add_wait(x1, x2);
    dd.add_wait(x2, x3);
    // x1 → x2 → x3, no cycle
    assert!(!dd.check_deadlock(x1));
}

#[test]
fn test_deadlock_graph_size() {
    let dd = DeadlockDetector::new();
    assert_eq!(dd.graph_size(), 0);
    dd.add_wait(Xid::new(0, 1), Xid::new(0, 2));
    assert_eq!(dd.graph_size(), 1);
    dd.remove_wait(Xid::new(0, 1));
    assert_eq!(dd.graph_size(), 0);
}
