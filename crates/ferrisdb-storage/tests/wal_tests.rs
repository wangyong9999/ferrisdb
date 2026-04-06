//! WAL subsystem comprehensive tests

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::Xid;
use ferrisdb_storage::wal::*;
use ferrisdb_storage::{StorageManager, PageId};

fn make_writer() -> (WalWriter, TempDir) {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    (WalWriter::new(&wal_dir), td)
}

// ==================== WalWriter Basic ====================

#[test]
fn test_wal_writer_create() {
    let (w, _td) = make_writer();
    assert!(w.current_lsn().is_valid());
}

#[test]
fn test_wal_writer_write_returns_lsn() {
    let (w, _td) = make_writer();
    let lsn = w.write(b"hello wal").unwrap();
    assert!(lsn.is_valid());
}

#[test]
fn test_wal_writer_offset_advances() {
    let (w, _td) = make_writer();
    let before = w.offset();
    w.write(b"some data").unwrap();
    assert!(w.offset() > before);
}

#[test]
fn test_wal_writer_multiple_writes() {
    let (w, _td) = make_writer();
    let l1 = w.write(b"first").unwrap();
    let l2 = w.write(b"second").unwrap();
    let l3 = w.write(b"third").unwrap();
    assert!(l2.raw() > l1.raw());
    assert!(l3.raw() > l2.raw());
}

#[test]
fn test_wal_writer_sync() {
    let (w, _td) = make_writer();
    w.write(b"sync me").unwrap();
    w.sync().unwrap(); // Should not panic
}

#[test]
fn test_wal_writer_flushed_lsn() {
    let (w, _td) = make_writer();
    w.write(b"data").unwrap();
    assert_eq!(w.flushed_lsn().raw(), 0); // Not synced yet
    w.sync().unwrap();
    assert!(w.flushed_lsn().raw() > 0);
}

#[test]
fn test_wal_writer_wait_for_lsn() {
    let (w, _td) = make_writer();
    let lsn = w.write(b"wait for me").unwrap();
    w.wait_for_lsn(lsn).unwrap(); // Should sync and return
    assert!(w.flushed_lsn().raw() >= lsn.raw());
}

#[test]
fn test_wal_writer_file_created() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let w = WalWriter::new(&wal_dir);
    w.write(b"create file").unwrap();
    w.sync().unwrap();

    let files: Vec<_> = std::fs::read_dir(&wal_dir).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .collect();
    assert!(!files.is_empty());
}

// ==================== WalFlusher ====================

#[test]
fn test_wal_flusher_start_stop() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let w = Arc::new(WalWriter::new(&wal_dir));
    let flusher = w.start_flusher(10);
    w.write(b"flush me").unwrap();
    std::thread::sleep(std::time::Duration::from_millis(50));
    flusher.stop();
    assert!(w.flushed_lsn().raw() > 0, "Flusher should have synced");
}

// ==================== WAL Record Types ====================

#[test]
fn test_wal_record_header() {
    let hdr = WalRecordHeader::new(WalRecordType::HeapInsert, 42);
    let size = { hdr.size }; // Copy out of packed struct to avoid UB
    assert_eq!(size, 42);
    assert_eq!(hdr.record_type(), Some(WalRecordType::HeapInsert));
}

#[test]
fn test_wal_record_type_conversion() {
    for rtype in [
        WalRecordType::HeapInsert, WalRecordType::HeapDelete,
        WalRecordType::TxnCommit, WalRecordType::TxnAbort,
        WalRecordType::CheckpointOnline,
    ] {
        let val = rtype as u16;
        let restored = WalRecordType::try_from(val);
        assert!(restored.is_ok(), "Should convert {:?} ({})", rtype, val);
    }
}

#[test]
fn test_wal_heap_insert_serialize() {
    let page_id = PageId::new(0, 0, 100, 5);
    let data = vec![0u8; 45]; // header + user data
    let rec = WalHeapInsert::new(page_id, 1, &data);
    let bytes = rec.serialize_with_data(&data);
    assert!(!bytes.is_empty());
}

#[test]
fn test_wal_txn_commit_serialize() {
    let xid = Xid::new(0, 42);
    let rec = WalTxnCommit::new(xid, 1000);
    let bytes = rec.to_bytes();
    assert!(!bytes.is_empty());
    assert!(bytes.len() >= 4); // At least header
}

#[test]
fn test_wal_txn_abort_serialize() {
    let xid = Xid::new(0, 7);
    let rec = WalTxnCommit::new_abort(xid);
    let bytes = rec.to_bytes();
    assert!(!bytes.is_empty());
}

#[test]
fn test_wal_checkpoint_serialize() {
    let cp = WalCheckpoint::new(12345, true);
    let bytes = cp.to_bytes();
    assert!(!bytes.is_empty());
}

// ==================== WAL Recovery ====================

#[test]
fn test_recovery_empty_dir() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
}

#[test]
fn test_recovery_with_records() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    // Write WAL records
    let w = WalWriter::new(&wal_dir);
    let page_id = PageId::new(0, 0, 100, 0);
    let data = vec![0u8; 45];
    let rec = WalHeapInsert::new(page_id, 1, &data);
    w.write(&rec.serialize_with_data(&data)).unwrap();
    w.sync().unwrap();
    drop(w);

    // Recover (scan-only — 测试 WAL 解析，不写入不存在的表数据)
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover_scan_only().unwrap();

    let stats = recovery.stats();
    assert!(stats.records_redone + stats.records_skipped > 0);
}

#[test]
fn test_recovery_scan_only() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let w = WalWriter::new(&wal_dir);
    w.write(b"\x00\x00\x01\x00test").unwrap(); // Some data
    w.sync().unwrap();
    drop(w);

    let recovery = WalRecovery::new(&wal_dir);
    let stats = recovery.recover_scan_only().unwrap();
    let _ = stats;
}

// ==================== WalBuffer ====================

#[test]
fn test_wal_buffer_write() {
    let buf = WalBuffer::new(65536);
    let lsn = buf.write(b"buffer data").unwrap();
    assert!(lsn.is_valid());
}

#[test]
fn test_wal_buffer_multiple_writes() {
    let buf = WalBuffer::new(65536);
    for i in 0..100 {
        let data = format!("record_{}", i);
        buf.write(data.as_bytes()).unwrap();
    }
    assert!(buf.write_pos() > 0);
}

#[test]
fn test_wal_buffer_full() {
    let buf = WalBuffer::new(64); // Very small
    let big_data = vec![0u8; 100];
    let result = buf.write(&big_data);
    assert!(result.is_err(), "Should fail when buffer full");
}

#[test]
fn test_wal_buffer_lsn_monotonic() {
    let buf = WalBuffer::new(65536);
    let l1 = buf.write(b"first").unwrap();
    let l2 = buf.write(b"second").unwrap();
    assert!(l2.raw() > l1.raw());
}

// ==================== Concurrent WAL ====================

#[test]
fn test_concurrent_wal_writes() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let w = Arc::new(WalWriter::new(&wal_dir));

    let mut handles = vec![];
    for t in 0..4 {
        let w = Arc::clone(&w);
        handles.push(std::thread::spawn(move || {
            for i in 0..50 {
                let data = format!("t{}_{}", t, i);
                w.write(data.as_bytes()).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    let expected_min = 4 * 50 * 4; // At least 4*50*4 bytes written
    assert!(w.offset() as usize > expected_min);
}
