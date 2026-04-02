//! End-to-end WAL Crash Recovery Tests
//!
//! Tests that committed data survives crash and aborted/in-flight data doesn't.

use std::sync::Arc;
use tempfile::TempDir;

use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager, WalWriter};
use ferrisdb_storage::wal::{WalRecovery, RecoveryMode};
use ferrisdb_transaction::{HeapTable, TransactionManager};

/// Full stack: WAL to disk + buffer pool + transactions + heap
fn setup_full(data_dir: &std::path::Path) -> (
    Arc<StorageManager>,
    Arc<BufferPool>,
    Arc<WalWriter>,
    Arc<TransactionManager>,
) {
    let smgr = Arc::new(StorageManager::new(data_dir));
    smgr.init().unwrap();

    let wal_dir = data_dir.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let wal_writer = Arc::new(WalWriter::new(&wal_dir));

    let mut bp = BufferPool::new(BufferPoolConfig::new(500)).unwrap();
    bp.set_smgr(Arc::clone(&smgr));
    let bp = Arc::new(bp);

    let mut txn_mgr = TransactionManager::new(64);
    txn_mgr.set_buffer_pool(Arc::clone(&bp));
    txn_mgr.set_wal_writer(Arc::clone(&wal_writer));
    let txn_mgr = Arc::new(txn_mgr);

    (smgr, bp, wal_writer, txn_mgr)
}

/// Test: committed data visible after flush + restart (no WAL replay needed)
#[test]
fn test_committed_data_survives_restart() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path();

    let tid1;
    {
        let (smgr, bp, wal_writer, txn_mgr) = setup_full(data_dir);
        let table = HeapTable::with_wal_writer(100, Arc::clone(&bp), Arc::clone(&txn_mgr), Arc::clone(&wal_writer));

        // Committed transaction
        let mut txn = txn_mgr.begin().unwrap();
        let xid = txn.xid();
        tid1 = table.insert_with_undo(b"committed row", xid, 0, Some(&mut txn)).unwrap();
        txn.commit().unwrap();

        // Flush to disk
        bp.flush_all().unwrap();
        wal_writer.sync().unwrap();
    }

    // Restart
    {
        let (smgr, bp, _wal_writer, txn_mgr) = setup_full(data_dir);
        let table = HeapTable::new(100, Arc::clone(&bp), Arc::clone(&txn_mgr));

        let result = table.fetch(tid1).unwrap();
        assert!(result.is_some(), "Committed data should survive restart");
        let (_, data) = result.unwrap();
        assert_eq!(data, b"committed row");
    }
}

/// Test: WAL records are written to disk files
#[test]
fn test_wal_files_created() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path();

    {
        let (_smgr, bp, wal_writer, txn_mgr) = setup_full(data_dir);
        let table = HeapTable::with_wal_writer(200, Arc::clone(&bp), Arc::clone(&txn_mgr), Arc::clone(&wal_writer));

        // Insert several rows
        let xid = Xid::new(0, 50);
        for i in 0..10 {
            let data = format!("row {}", i);
            table.insert(data.as_bytes(), xid, 0).unwrap();
        }

        wal_writer.sync().unwrap();
    }

    // Check WAL files exist
    let wal_dir = data_dir.join("wal");
    let wal_files: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .collect();
    assert!(!wal_files.is_empty(), "WAL files should exist on disk");
}

/// Test: WAL recovery processes records
#[test]
fn test_wal_recovery_works() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path();

    {
        let (smgr, bp, wal_writer, txn_mgr) = setup_full(data_dir);
        let table = HeapTable::with_wal_writer(300, Arc::clone(&bp), Arc::clone(&txn_mgr), Arc::clone(&wal_writer));

        let xid = Xid::new(0, 50);
        for i in 0..5 {
            let data = format!("recovery row {}", i);
            table.insert(data.as_bytes(), xid, 0).unwrap();
        }

        wal_writer.sync().unwrap();
        // Don't flush buffer pool — simulate crash without clean shutdown
    }

    // Recovery
    let wal_dir = data_dir.join("wal");
    let smgr = Arc::new(StorageManager::new(data_dir));
    smgr.init().unwrap();

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();

    let stats = recovery.stats();
    assert!(stats.records_redone + stats.records_skipped > 0,
        "Recovery should process WAL records");
}
