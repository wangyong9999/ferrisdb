//! Transaction, MVCC, Lock, and WAL integration tests

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager, WalWriter};
use ferrisdb_transaction::{TransactionManager, TransactionState, HeapTable, HeapScan};

fn setup() -> (Arc<BufferPool>, Arc<TransactionManager>) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let mut tm = TransactionManager::new(64);
    tm.set_buffer_pool(Arc::clone(&bp));
    (bp, Arc::new(tm))
}

fn setup_with_wal() -> (Arc<BufferPool>, Arc<TransactionManager>, Arc<WalWriter>, TempDir) {
    let td = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let wal = Arc::new(WalWriter::new(&wal_dir));

    let mut bp = BufferPool::new(BufferPoolConfig::new(200)).unwrap();
    bp.set_smgr(smgr);
    let bp = Arc::new(bp);

    let mut tm = TransactionManager::new(64);
    tm.set_buffer_pool(Arc::clone(&bp));
    tm.set_wal_writer(Arc::clone(&wal));
    (bp, Arc::new(tm), wal, td)
}

// ==================== Transaction Lifecycle ====================

#[test]
fn test_txn_begin() {
    let (_, tm) = setup();
    let txn = tm.begin().unwrap();
    assert!(txn.is_active());
    assert_eq!(txn.state(), TransactionState::InProgress);
}

#[test]
fn test_txn_commit() {
    let (_, tm) = setup();
    let mut txn = tm.begin().unwrap();
    txn.commit().unwrap();
    assert!(!txn.is_active());
    assert_eq!(txn.state(), TransactionState::Committed);
}

#[test]
fn test_txn_abort() {
    let (_, tm) = setup();
    let mut txn = tm.begin().unwrap();
    txn.abort().unwrap();
    assert_eq!(txn.state(), TransactionState::Aborted);
}

#[test]
fn test_txn_double_commit() {
    let (_, tm) = setup();
    let mut txn = tm.begin().unwrap();
    txn.commit().unwrap();
    assert!(txn.commit().is_err());
}

#[test]
fn test_txn_commit_after_abort() {
    let (_, tm) = setup();
    let mut txn = tm.begin().unwrap();
    txn.abort().unwrap();
    assert!(txn.commit().is_err());
}

#[test]
fn test_txn_csn_allocated() {
    let (_, tm) = setup();
    let mut txn = tm.begin().unwrap();
    txn.commit().unwrap();
    assert!(txn.csn().is_valid());
}

#[test]
fn test_txn_csn_monotonic() {
    let (_, tm) = setup();
    let mut t1 = tm.begin().unwrap();
    t1.commit().unwrap();
    let csn1 = t1.csn();

    let mut t2 = tm.begin().unwrap();
    t2.commit().unwrap();
    let csn2 = t2.csn();

    assert!(csn2.raw() > csn1.raw(), "CSN should be monotonically increasing");
}

// ==================== Multiple Transactions ====================

#[test]
fn test_multiple_txn_slots() {
    let (_, tm) = setup();
    let mut txns = Vec::new();
    for _ in 0..10 {
        txns.push(tm.begin().unwrap());
    }
    // All active
    for t in &txns {
        assert!(t.is_active());
    }
    // Commit all
    for t in &mut txns {
        t.commit().unwrap();
    }
}

#[test]
fn test_txn_slot_reuse() {
    let (_, tm) = setup();
    for _ in 0..100 {
        let mut txn = tm.begin().unwrap();
        txn.commit().unwrap();
    }
    // Should not exhaust slots
}

// ==================== MVCC Visibility ====================

#[test]
fn test_visibility_own_insert() {
    let (_, tm) = setup();
    let txn = tm.begin().unwrap();
    // Own insert, no delete → visible
    assert!(txn.is_visible(txn.xid(), Xid::INVALID, 0, 0));
}

#[test]
fn test_visibility_own_delete() {
    let (_, tm) = setup();
    let txn = tm.begin().unwrap();
    // Own insert, own delete → not visible
    assert!(!txn.is_visible(txn.xid(), txn.xid(), 0, 1));
}

#[test]
fn test_visibility_uncommitted_insert() {
    let (_, tm) = setup();
    let t1 = tm.begin().unwrap();
    let t2 = tm.begin().unwrap();
    // T1 inserts, T2 shouldn't see it (T1 is active)
    assert!(!t2.is_visible(t1.xid(), Xid::INVALID, 0, 0));
}

// ==================== Savepoint ====================

#[test]
fn test_savepoint_basic() {
    let (bp, tm) = setup();
    let table = HeapTable::new(100, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    let _tid1 = table.insert_with_undo(b"before sp", xid, 0, Some(&mut txn)).unwrap();
    let sp = txn.savepoint();
    let tid2 = table.insert_with_undo(b"after sp", xid, 0, Some(&mut txn)).unwrap();

    txn.rollback_to_savepoint(sp);
    assert!(table.fetch(tid2).unwrap().is_none(), "Should be rolled back");
}

#[test]
fn test_savepoint_nested() {
    let (bp, tm) = setup();
    let table = HeapTable::new(101, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    let _t1 = table.insert_with_undo(b"level0", xid, 0, Some(&mut txn)).unwrap();
    let sp1 = txn.savepoint();
    let _t2 = table.insert_with_undo(b"level1", xid, 0, Some(&mut txn)).unwrap();
    let sp2 = txn.savepoint();
    let t3 = table.insert_with_undo(b"level2", xid, 0, Some(&mut txn)).unwrap();

    txn.rollback_to_savepoint(sp2);
    assert!(table.fetch(t3).unwrap().is_none());

    txn.rollback_to_savepoint(sp1);
    // Level 1 should also be gone now
}

// ==================== Undo Rollback ====================

#[test]
fn test_abort_rollback_insert() {
    let (bp, tm) = setup();
    let table = HeapTable::new(110, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    let tid = table.insert_with_undo(b"will vanish", xid, 0, Some(&mut txn)).unwrap();
    txn.abort().unwrap();
    assert!(table.fetch(tid).unwrap().is_none());
}

#[test]
fn test_abort_rollback_delete() {
    let (bp, tm) = setup();
    let table = HeapTable::new(111, Arc::clone(&bp), Arc::clone(&tm));

    // Committed insert
    let tid = table.insert(b"keep me", Xid::new(0, 50), 0).unwrap();

    // Abort delete
    let mut txn = tm.begin().unwrap();
    table.delete_with_undo(tid, txn.xid(), 0, Some(&mut txn)).unwrap();
    txn.abort().unwrap();

    // Should still exist with original xmax=INVALID
    let (hdr, data) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(data, b"keep me");
    assert_eq!(hdr.xmax.raw(), Xid::INVALID.raw());
}

#[test]
fn test_abort_rollback_update() {
    let (bp, tm) = setup();
    let table = HeapTable::new(112, Arc::clone(&bp), Arc::clone(&tm));

    let original = b"AAAA_original";
    let tid = table.insert(original, Xid::new(0, 50), 0).unwrap();

    let mut txn = tm.begin().unwrap();
    let updated = b"BBBB_replaced";
    table.update_with_undo(tid, updated, txn.xid(), 0, Some(&mut txn)).unwrap();
    txn.abort().unwrap();

    let (_, data) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(data, original);
}

// ==================== Timeout ====================

#[test]
fn test_txn_timeout() {
    let (bp, _) = setup();
    let mut tm = TransactionManager::new(64);
    tm.set_buffer_pool(bp);
    tm.set_txn_timeout(1); // 1ms timeout
    let tm = Arc::new(tm);

    let txn = tm.begin().unwrap();
    std::thread::sleep(std::time::Duration::from_millis(5));
    assert!(txn.is_timed_out());
}

#[test]
fn test_txn_no_timeout() {
    let (_, tm) = setup();
    let txn = tm.begin().unwrap();
    assert!(!txn.is_timed_out()); // default = no timeout
}

// ==================== WAL Integration ====================

#[test]
fn test_commit_writes_wal() {
    let (_bp, tm, wal, _td) = setup_with_wal();
    let offset_before = wal.offset();
    let mut txn = tm.begin().unwrap();
    txn.commit().unwrap();
    let offset_after = wal.offset();
    assert!(offset_after > offset_before, "Commit should write WAL record");
}

#[test]
fn test_abort_writes_wal() {
    let (_bp, tm, wal, _td) = setup_with_wal();
    let offset_before = wal.offset();
    let mut txn = tm.begin().unwrap();
    txn.abort().unwrap();
    let offset_after = wal.offset();
    assert!(offset_after > offset_before, "Abort should write WAL record");
}

// ==================== Concurrent Transactions ====================

#[test]
fn test_concurrent_txn_commit() {
    let (_, tm) = setup();
    let tm = Arc::new(tm);
    let mut handles = vec![];

    for _ in 0..8 {
        let tm = Arc::clone(&tm);
        handles.push(std::thread::spawn(move || {
            for _ in 0..50 {
                let mut txn = tm.begin().unwrap();
                txn.commit().unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

// ==================== Deadlock Detection ====================

#[test]
fn test_deadlock_detector() {
    use ferrisdb_transaction::DeadlockDetector;
    let detector = DeadlockDetector::new();

    let x1 = Xid::new(0, 1);
    let x2 = Xid::new(0, 2);
    let x3 = Xid::new(0, 3);

    // Chain: x1 → x2 → x3 (no cycle)
    detector.add_wait(x1, x2);
    detector.add_wait(x2, x3);
    assert!(!detector.check_deadlock(x1));

    // Add x3 → x1 (cycle)
    detector.add_wait(x3, x1);
    assert!(detector.check_deadlock(x1));
}

#[test]
fn test_deadlock_removal() {
    use ferrisdb_transaction::DeadlockDetector;
    let detector = DeadlockDetector::new();

    let x1 = Xid::new(0, 1);
    let x2 = Xid::new(0, 2);

    detector.add_wait(x1, x2);
    detector.add_wait(x2, x1);
    assert!(detector.check_deadlock(x1));

    detector.remove_wait(x1);
    assert!(!detector.check_deadlock(x2)); // cycle broken
}

// ==================== Lock Modes ====================

#[test]
fn test_lock_conflict_matrix() {
    use ferrisdb_transaction::HeavyLockMode;
    // S vs S = no conflict
    assert!(!HeavyLockMode::Share.conflicts_with(HeavyLockMode::Share));
    // S vs X = conflict
    assert!(HeavyLockMode::Share.conflicts_with(HeavyLockMode::Exclusive));
    // X vs X = conflict
    assert!(HeavyLockMode::Exclusive.conflicts_with(HeavyLockMode::Exclusive));
    // IS vs AX = conflict
    assert!(HeavyLockMode::IntentShare.conflicts_with(HeavyLockMode::AccessExclusive));
}

// ==================== HeapScan Visibility ====================

#[test]
fn test_scan_sees_committed() {
    let (bp, tm) = setup();
    let table = HeapTable::new(200, Arc::clone(&bp), Arc::clone(&tm));

    // Insert as committed xid
    table.insert(b"visible", Xid::new(0, 50), 0).unwrap();

    let mut scan = HeapScan::new(&table);
    let mut count = 0;
    while let Ok(Some(_)) = scan.next() {
        count += 1;
    }
    assert_eq!(count, 1);
}

#[test]
fn test_fetch_visible() {
    let (bp, tm) = setup();
    let table = HeapTable::new(201, Arc::clone(&bp), Arc::clone(&tm));

    let tid = table.insert(b"test", Xid::new(0, 50), 0).unwrap();

    let txn = tm.begin().unwrap();
    // Should see committed data (xid 50 is not in active list if slot was released)
    let result = table.fetch(tid).unwrap();
    assert!(result.is_some());
    let _ = txn;
}

// ==================== Lock Manager Integration ====================

#[test]
fn test_lock_manager_acquire_release() {
    use ferrisdb_transaction::{LockManager, LockTag, HeavyLockMode};
    let mgr = LockManager::new();
    let tag = LockTag::Relation(42);
    let xid = Xid::new(0, 1);
    mgr.acquire(tag, HeavyLockMode::Share, xid).unwrap();
    mgr.release(&tag).unwrap();
}

#[test]
fn test_lock_manager_exclusive_blocks_shared() {
    use ferrisdb_transaction::{LockManager, LockTag, HeavyLockMode};
    let mgr = LockManager::new();
    let tag = LockTag::Relation(43);
    let xid = Xid::new(0, 1);
    mgr.acquire(tag, HeavyLockMode::Exclusive, xid).unwrap();
    assert!(!mgr.try_acquire(&tag, HeavyLockMode::Share));
    mgr.release(&tag).unwrap();
    assert!(mgr.try_acquire(&tag, HeavyLockMode::Share));
}

#[test]
fn test_lock_manager_tuple_lock() {
    use ferrisdb_transaction::{LockManager, LockTag, HeavyLockMode};
    let mgr = LockManager::new();
    let tag = LockTag::Tuple(100, 5, 3);
    let xid = Xid::new(0, 1);
    mgr.acquire(tag, HeavyLockMode::Exclusive, xid).unwrap();
    mgr.release(&tag).unwrap();
}

#[test]
fn test_lock_manager_multiple_relations() {
    use ferrisdb_transaction::{LockManager, LockTag, HeavyLockMode};
    let mgr = LockManager::new();
    let xid = Xid::new(0, 1);
    for rel in 1..10u32 {
        mgr.acquire(LockTag::Relation(rel), HeavyLockMode::Share, xid).unwrap();
    }
    assert_eq!(mgr.lock_count(), 9);
    for rel in 1..10u32 {
        mgr.release(&LockTag::Relation(rel)).unwrap();
    }
}

// ==================== Undo Zone ====================

#[test]
fn test_undo_zone_allocate() {
    use ferrisdb_transaction::UndoZone;
    let zone = UndoZone::new(0, 0);
    let (_page, offset) = zone.allocate(100).unwrap();
    assert_eq!(offset, 0);
    let (_page2, offset2) = zone.allocate(200).unwrap();
    assert_eq!(offset2, 100);
}

#[test]
fn test_undo_zone_reset() {
    use ferrisdb_transaction::UndoZone;
    let zone = UndoZone::new(0, 0);
    zone.set_xid(Xid::new(0, 42));
    zone.allocate(500).unwrap();
    zone.reset();
    assert!(!zone.xid().is_valid());
}

#[test]
fn test_undo_zone_page_boundary() {
    use ferrisdb_transaction::UndoZone;
    let zone = UndoZone::new(0, 0);
    zone.allocate(8000).unwrap();
    let (page, _) = zone.allocate(500).unwrap();
    assert_eq!(page, 1);
}

#[test]
fn test_undo_record_types() {
    use ferrisdb_transaction::{UndoRecord, UndoRecordType};
    let xid = Xid::new(0, 5);
    let r1 = UndoRecord::new_insert(xid, 100, (1, 0));
    assert_eq!(r1.record_type(), UndoRecordType::Insert);
    let r2 = UndoRecord::new_delete(xid, 100, (1, 0), b"old");
    assert_eq!(r2.record_type(), UndoRecordType::Delete);
}

#[test]
fn test_undo_record_serialize() {
    use ferrisdb_transaction::UndoRecord;
    let rec = UndoRecord::new_insert(Xid::new(0, 7), 200, (5, 3));
    let bytes = rec.serialize();
    assert!(bytes.len() >= 32);
}
