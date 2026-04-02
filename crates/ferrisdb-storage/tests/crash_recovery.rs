//! Crash Recovery Integration Tests
//!
//! Tests WAL recovery and data persistence.

use std::sync::Arc;
use tempfile::TempDir;

use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager, HeapPage};
use ferrisdb_storage::wal::{WalWriter, WalRecovery, RecoveryMode, RecoveryStage};
use ferrisdb_storage::wal::WalHeapInsert;
use ferrisdb_storage::PageId;
use ferrisdb_storage::page::PAGE_SIZE;

/// Test that WAL recovery processes heap insert records
#[test]
fn test_wal_recovery_heap_insert() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path();

    let smgr = Arc::new(StorageManager::new(data_dir));
    smgr.init().unwrap();

    let wal_dir = data_dir.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = WalWriter::new(&wal_dir);

    // Write a heap insert WAL record
    let page_id = PageId::new(0, 0, 100, 0);
    let tuple_data = {
        let mut buf = vec![0u8; 32 + 13];
        buf[0..8].copy_from_slice(&Xid::new(0, 1).raw().to_le_bytes());
        buf[32..].copy_from_slice(b"Hello, World!");
        buf
    };
    let wal_rec = WalHeapInsert::new(page_id, 1, &tuple_data);
    writer.write(&wal_rec.serialize_with_data(&tuple_data)).unwrap();
    drop(writer);

    // Recover
    let recovery = WalRecovery::with_smgr(&wal_dir, Arc::clone(&smgr));
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();

    assert_eq!(recovery.stage(), RecoveryStage::Completed);
    let stats = recovery.stats();
    assert!(stats.records_redone + stats.records_skipped > 0,
        "Should process records: redone={}, skipped={}", stats.records_redone, stats.records_skipped);
}

/// Test that data persisted via buffer pool flush survives restart
#[test]
fn test_data_survives_flush_and_restart() {
    let temp_dir = TempDir::new().unwrap();
    let data_dir = temp_dir.path();

    // Phase 1: Write data and flush
    {
        let smgr = Arc::new(StorageManager::new(data_dir));
        smgr.init().unwrap();

        let mut bp = BufferPool::new(BufferPoolConfig::new(200)).unwrap();
        bp.set_smgr(Arc::clone(&smgr));
        let bp = Arc::new(bp);

        let tag = ferrisdb_core::BufferTag::new(ferrisdb_core::PdbId::new(0), 100, 0);
        let pinned = bp.pin(&tag).unwrap();
        let page_ptr = pinned.page_data();
        let _lock = pinned.lock_exclusive();

        let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, PAGE_SIZE)) };
        page.init();

        let mut tuple = vec![0u8; 32 + 15];
        tuple[0..8].copy_from_slice(&Xid::new(0, 1).raw().to_le_bytes());
        tuple[32..].copy_from_slice(b"persistent data");
        let offset = page.insert_tuple(&tuple).unwrap();
        assert!(offset > 0);

        drop(_lock);
        pinned.mark_dirty();
        drop(pinned);
        bp.flush_all().unwrap();
    }

    // Phase 2: Restart and verify
    {
        let smgr = Arc::new(StorageManager::new(data_dir));
        smgr.init().unwrap();

        let mut bp = BufferPool::new(BufferPoolConfig::new(200)).unwrap();
        bp.set_smgr(Arc::clone(&smgr));
        let bp = Arc::new(bp);

        let tag = ferrisdb_core::BufferTag::new(ferrisdb_core::PdbId::new(0), 100, 0);
        let pinned = bp.pin(&tag).unwrap();
        let page_ptr = pinned.page_data();
        let _lock = pinned.lock_shared();

        let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, PAGE_SIZE)) };
        let data = page.get_tuple(1);
        assert!(data.is_some(), "Tuple should survive flush+restart");
        assert_eq!(&data.unwrap()[32..], b"persistent data");
    }
}

/// Test empty WAL recovery completes cleanly
#[test]
fn test_recovery_empty_wal() {
    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let smgr = Arc::new(StorageManager::new(temp_dir.path()));
    smgr.init().unwrap();

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    assert_eq!(recovery.stage(), RecoveryStage::NotStarted);

    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
    assert_eq!(recovery.stats().records_redone, 0);
}
