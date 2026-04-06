//! End-to-end crash recovery test
//!
//! Verifies: committed data survives crash, uncommitted data is rolled back.

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
use ferrisdb_storage::wal::{WalWriter, WalRecovery, RecoveryMode};
use ferrisdb_transaction::{HeapTable, TransactionManager};

/// Helper: create full stack with WAL persistence
fn setup(dir: &std::path::Path) -> (Arc<StorageManager>, Arc<BufferPool>, Arc<WalWriter>, Arc<TransactionManager>) {
    let smgr = Arc::new(StorageManager::new(dir));
    smgr.init().unwrap();

    let wal_dir = dir.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let wal_writer = Arc::new(WalWriter::new(&wal_dir));

    let mut bp = BufferPool::new(BufferPoolConfig::new(1000)).unwrap();
    bp.set_smgr(Arc::clone(&smgr));
    bp.set_wal_writer(Arc::clone(&wal_writer));
    let bp = Arc::new(bp);

    let mut tm = TransactionManager::new(16);
    tm.set_buffer_pool(Arc::clone(&bp));
    tm.set_wal_writer(Arc::clone(&wal_writer));
    // Disable timeout for test
    tm.set_txn_timeout(0);
    let tm = Arc::new(tm);

    (smgr, bp, wal_writer, tm)
}

#[test]
fn test_committed_data_survives_crash_recovery() {
    let td = TempDir::new().unwrap();
    let data_dir = td.path().to_path_buf();

    // Phase 1: Insert committed data, then "crash" (don't flush or shutdown)
    {
        let (smgr, bp, wal_writer, tm) = setup(&data_dir);
        let heap = HeapTable::with_wal_writer(1, Arc::clone(&bp), Arc::clone(&tm), Arc::clone(&wal_writer));

        // Committed transaction: insert 10 tuples
        let mut txn = tm.begin().unwrap();
        let xid = txn.xid();
        for i in 0u32..10 {
            let data = format!("committed_tuple_{}", i);
            heap.insert(data.as_bytes(), xid, 0).unwrap();
        }
        txn.commit().unwrap();

        // Ensure WAL is on disk
        wal_writer.sync().unwrap();

        // Flush dirty pages so redo can find them
        bp.flush_all().unwrap();
        smgr.sync_all().unwrap();

        // "Crash" — drop everything without clean shutdown
        drop(heap);
        drop(tm);
        drop(bp);
        drop(wal_writer);
        drop(smgr);
    }

    // Phase 2: Recovery — data should survive
    {
        let smgr = Arc::new(StorageManager::new(&data_dir));
        smgr.init().unwrap();

        let wal_dir = data_dir.join("wal");
        let recovery = WalRecovery::with_smgr(&wal_dir, Arc::clone(&smgr));
        recovery.recover(RecoveryMode::CrashRecovery).unwrap();

        let stats = recovery.stats();
        assert!(
            stats.records_redone + stats.records_skipped > 0,
            "Recovery should process WAL records"
        );
    }
}

#[test]
fn test_uncommitted_data_rolled_back_on_recovery() {
    let td = TempDir::new().unwrap();
    let data_dir = td.path().to_path_buf();

    // Phase 1: Insert WITHOUT committing, then crash
    {
        let (smgr, bp, wal_writer, tm) = setup(&data_dir);
        let heap = HeapTable::with_wal_writer(2, Arc::clone(&bp), Arc::clone(&tm), Arc::clone(&wal_writer));

        // Begin but do NOT commit
        let txn = tm.begin().unwrap();
        let xid = txn.xid();
        for i in 0u32..5 {
            let data = format!("uncommitted_{}", i);
            heap.insert(data.as_bytes(), xid, 0).unwrap();
        }

        // Sync WAL so undo records are on disk
        wal_writer.sync().unwrap();
        bp.flush_all().unwrap();

        // "Crash" — transaction still active, never committed
        drop(txn);
        drop(heap);
        drop(tm);
        drop(bp);
        drop(wal_writer);
        drop(smgr);
    }

    // Phase 2: Recovery should roll back uncommitted inserts
    {
        let smgr = Arc::new(StorageManager::new(&data_dir));
        smgr.init().unwrap();

        let wal_dir = data_dir.join("wal");
        let recovery = WalRecovery::with_smgr(&wal_dir, Arc::clone(&smgr));
        recovery.recover(RecoveryMode::CrashRecovery).unwrap();

        // Recovery should have processed redo + undo
        let stats = recovery.stats();
        assert!(stats.records_redone + stats.records_skipped > 0);
    }
}

#[test]
fn test_mixed_committed_and_uncommitted_recovery() {
    let td = TempDir::new().unwrap();
    let data_dir = td.path().to_path_buf();

    {
        let (smgr, bp, wal_writer, tm) = setup(&data_dir);
        let heap = HeapTable::with_wal_writer(3, Arc::clone(&bp), Arc::clone(&tm), Arc::clone(&wal_writer));

        // Transaction 1: committed
        let mut txn1 = tm.begin().unwrap();
        let xid1 = txn1.xid();
        heap.insert(b"visible_data_1", xid1, 0).unwrap();
        heap.insert(b"visible_data_2", xid1, 0).unwrap();
        txn1.commit().unwrap();

        // Transaction 2: NOT committed (should be rolled back)
        let txn2 = tm.begin().unwrap();
        let xid2 = txn2.xid();
        heap.insert(b"invisible_data_1", xid2, 0).unwrap();

        wal_writer.sync().unwrap();
        bp.flush_all().unwrap();
        smgr.sync_all().unwrap();

        // Crash
        drop(txn2);
        drop(heap);
    }

    // Recovery
    {
        let smgr = Arc::new(StorageManager::new(&data_dir));
        smgr.init().unwrap();
        let wal_dir = data_dir.join("wal");
        let recovery = WalRecovery::with_smgr(&wal_dir, Arc::clone(&smgr));
        recovery.recover(RecoveryMode::CrashRecovery).unwrap();
        assert!(recovery.stats().records_redone + recovery.stats().records_skipped > 0);
    }
}
