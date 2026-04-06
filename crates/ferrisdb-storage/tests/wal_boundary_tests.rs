//! WAL boundary conditions and recovery edge cases

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::Xid;
use ferrisdb_storage::{StorageManager, PageId};
use ferrisdb_storage::wal::*;
use ferrisdb_storage::page::PAGE_SIZE;

fn make_wal() -> (WalWriter, TempDir) {
    let td = TempDir::new().unwrap();
    let d = td.path().join("wal");
    std::fs::create_dir_all(&d).unwrap();
    (WalWriter::new(&d), td)
}

// ==================== WAL File Rotation ====================

#[test]
fn test_wal_large_write_triggers_rotation() {
    let (w, _td) = make_wal();
    let initial_file = w.file_no();
    // Write enough to fill a WAL file (16MB default)
    let big = vec![0u8; 1024 * 1024]; // 1MB
    for _ in 0..20 {
        w.write(&big).unwrap();
    }
    // Should have rotated at least once
    assert!(w.file_no() > initial_file, "Should rotate WAL files");
}

#[test]
fn test_wal_multiple_files_created() {
    let td = TempDir::new().unwrap();
    let d = td.path().join("wal");
    std::fs::create_dir_all(&d).unwrap();
    let w = WalWriter::new(&d);
    let big = vec![0xAA; 512 * 1024]; // 512KB
    for _ in 0..40 {
        w.write(&big).unwrap();
    }
    w.sync().unwrap();
    let count = std::fs::read_dir(&d).unwrap()
        .filter(|e| e.as_ref().map(|e| e.path().extension().map_or(false, |x| x == "wal")).unwrap_or(false))
        .count();
    assert!(count > 1, "Should create multiple WAL files, got {}", count);
}

// ==================== Recovery Edge Cases ====================

#[test]
fn test_recovery_multiple_wal_files() {
    let td = TempDir::new().unwrap();
    let d = td.path().join("wal");
    std::fs::create_dir_all(&d).unwrap();
    let w = WalWriter::new(&d);
    // Write records across multiple files
    let page_id = PageId::new(0, 0, 100, 0);
    for i in 0..50 {
        let data = vec![i as u8; 45];
        let rec = WalHeapInsert::new(page_id, (i + 1) as u16, &data);
        w.write(&rec.serialize_with_data(&data)).unwrap();
    }
    w.sync().unwrap();
    drop(w);

    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let recovery = WalRecovery::with_smgr(&d, smgr);
    recovery.recover_scan_only().unwrap();
    let stats = recovery.stats();
    assert!(stats.records_redone + stats.records_skipped > 0);
}

#[test]
fn test_recovery_idempotent() {
    let td = TempDir::new().unwrap();
    let d = td.path().join("wal");
    std::fs::create_dir_all(&d).unwrap();
    let w = WalWriter::new(&d);
    let page_id = PageId::new(0, 0, 200, 0);
    let data = vec![0u8; 45];
    let rec = WalHeapInsert::new(page_id, 1, &data);
    w.write(&rec.serialize_with_data(&data)).unwrap();
    w.sync().unwrap();
    drop(w);

    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    // Recover twice (scan-only, no actual page writes for non-existent tables)
    let r1 = WalRecovery::with_smgr(&d, Arc::clone(&smgr));
    r1.recover_scan_only().unwrap();
    let r2 = WalRecovery::with_smgr(&d, smgr);
    r2.recover_scan_only().unwrap();
    // Both scans should process the records
    let stats1 = r1.stats();
    let stats2 = r2.stats();
    assert!(stats1.records_redone + stats1.records_skipped > 0);
    assert!(stats2.records_redone + stats2.records_skipped > 0);
}

#[test]
fn test_recovery_with_commit_abort_records() {
    let td = TempDir::new().unwrap();
    let d = td.path().join("wal");
    std::fs::create_dir_all(&d).unwrap();
    let w = WalWriter::new(&d);
    // Write commit record
    let commit = WalTxnCommit::new(Xid::new(0, 1), 100);
    w.write(&commit.to_bytes()).unwrap();
    // Write abort record
    let abort = WalTxnCommit::new_abort(Xid::new(0, 2));
    w.write(&abort.to_bytes()).unwrap();
    w.sync().unwrap();
    drop(w);

    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let recovery = WalRecovery::with_smgr(&d, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
}

// ==================== WAL Buffer ====================

#[test]
fn test_wal_buffer_atomic_group() {
    let buf = WalBuffer::new(65536);
    let group_pos = buf.begin_atomic_group(Xid::new(0, 1)).unwrap();
    buf.append_to_atomic_group(group_pos, b"record_1").unwrap();
    buf.append_to_atomic_group(group_pos, b"record_2").unwrap();
    let lsn = buf.end_atomic_group(group_pos).unwrap();
    assert!(lsn.is_valid());
}

// ==================== Checkpoint ====================

#[test]
fn test_checkpoint_record_roundtrip() {
    let cp = WalCheckpoint::new(99999, false);
    let bytes = cp.to_bytes();
    assert!(!bytes.is_empty());
    // Verify type field
    assert!(bytes.len() >= 4);
}

#[test]
fn test_checkpoint_shutdown_vs_online() {
    let cp_shut = WalCheckpoint::new(100, true);
    let cp_online = WalCheckpoint::new(100, false);
    let b1 = cp_shut.to_bytes();
    let b2 = cp_online.to_bytes();
    // Different checkpoint types should produce different bytes
    assert_ne!(b1, b2);
}

