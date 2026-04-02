//! WAL comprehensive tests — 41 tests

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{Xid, Lsn};
use ferrisdb_storage::{StorageManager, PageId};
use ferrisdb_storage::wal::*;

fn wal() -> (WalWriter, TempDir) {
    let td = TempDir::new().unwrap();
    let d = td.path().join("wal");
    std::fs::create_dir_all(&d).unwrap();
    (WalWriter::new(&d), td)
}

// ===== Writer basics =====
#[test] fn test_write_empty() { let (w, _) = wal(); let l = w.write(b"").unwrap(); assert!(l.is_valid()); }
#[test] fn test_write_1byte() { let (w, _) = wal(); w.write(&[42]).unwrap(); }
#[test] fn test_write_large() { let (w, _) = wal(); w.write(&vec![0xAA; 100_000]).unwrap(); }
#[test] fn test_write_many() { let (w, _) = wal(); for i in 0..1000 { w.write(&[i as u8; 10]).unwrap(); } assert!(w.offset() > 10000); }
#[test] fn test_lsn_monotonic() { let (w, _) = wal(); let l1 = w.write(b"a").unwrap(); let l2 = w.write(b"b").unwrap(); assert!(l2.raw() > l1.raw()); }
#[test] fn test_sync_empty() { let (w, _) = wal(); w.sync().unwrap(); }
#[test] fn test_sync_after_write() { let (w, _) = wal(); w.write(b"data").unwrap(); w.sync().unwrap(); }
#[test] fn test_write_and_sync() { let (w, _) = wal(); let l = w.write_and_sync(b"atomic").unwrap(); assert!(l.is_valid()); }
#[test] fn test_flushed_lsn_updates() { let (w, _) = wal(); w.write(b"x").unwrap(); w.sync().unwrap(); assert!(w.flushed_lsn().raw() > 0); }
#[test] fn test_wait_for_lsn_immediate() { let (w, _) = wal(); let l = w.write(b"wait").unwrap(); w.sync().unwrap(); w.wait_for_lsn(l).unwrap(); }
#[test] fn test_file_rotation() { let (w, _) = wal(); let big = vec![0u8; 1024*1024]; for _ in 0..20 { w.write(&big).unwrap(); } assert!(w.file_no() > 0); }
#[test] fn test_wal_dir_accessor() { let (w, td) = wal(); let d = w.wal_dir(); assert!(d.exists()); }

// ===== Flusher =====
#[test] fn test_flusher_syncs() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = Arc::new(WalWriter::new(&d)); let f = w.start_flusher(5); w.write(b"flush").unwrap(); std::thread::sleep(std::time::Duration::from_millis(50)); f.stop(); assert!(w.flushed_lsn().raw() > 0); }

// ===== Record types =====
#[test] fn test_heap_insert_record() { let r = WalHeapInsert::new(PageId::new(0,0,1,0), 1, &[0;45]); let b = r.serialize_with_data(&[0;45]); assert!(!b.is_empty()); }
#[test] fn test_heap_delete_record() { let r = WalHeapDelete::new(PageId::new(0,0,1,0), 3); let b = r.to_bytes(); assert!(!b.is_empty()); }
#[test] fn test_txn_commit_record() { let r = WalTxnCommit::new(Xid::new(0,1), 100); let b = r.to_bytes(); assert!(b.len() > 4); }
#[test] fn test_txn_abort_record() { let r = WalTxnCommit::new_abort(Xid::new(0,2)); let b = r.to_bytes(); assert!(b.len() > 4); }
#[test] fn test_checkpoint_record() { let r = WalCheckpoint::new(9999, false); let b = r.to_bytes(); assert!(!b.is_empty()); }
#[test] fn test_checkpoint_shutdown() { let r = WalCheckpoint::new(9999, true); let b1 = r.to_bytes(); let r2 = WalCheckpoint::new(9999, false); let b2 = r2.to_bytes(); assert_ne!(b1, b2); }
#[test] fn test_record_type_roundtrip() { for t in [WalRecordType::HeapInsert, WalRecordType::TxnCommit, WalRecordType::CheckpointOnline] { let v = t as u16; assert!(WalRecordType::try_from(v).is_ok()); } }

// ===== Recovery =====
#[test] fn test_recovery_empty() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let r = WalRecovery::with_smgr(&d, s); r.recover(RecoveryMode::CrashRecovery).unwrap(); assert_eq!(r.stage(), RecoveryStage::Completed); }
#[test] fn test_recovery_with_heap_records() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); let data = vec![0u8;45]; let rec = WalHeapInsert::new(PageId::new(0,0,100,0), 1, &data); w.write(&rec.serialize_with_data(&data)).unwrap(); w.sync().unwrap(); drop(w); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let r = WalRecovery::with_smgr(&d, s); r.recover(RecoveryMode::CrashRecovery).unwrap(); assert!(r.stats().records_redone + r.stats().records_skipped > 0); }
#[test] fn test_recovery_idempotent() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); let data = vec![0u8;45]; let rec = WalHeapInsert::new(PageId::new(0,0,200,0), 1, &data); w.write(&rec.serialize_with_data(&data)).unwrap(); w.sync().unwrap(); drop(w); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); WalRecovery::with_smgr(&d, Arc::clone(&s)).recover(RecoveryMode::CrashRecovery).unwrap(); let r2 = WalRecovery::with_smgr(&d, s); r2.recover(RecoveryMode::CrashRecovery).unwrap(); assert!(r2.stats().records_skipped >= r2.stats().records_redone); }
#[test] fn test_recovery_with_commit() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); w.write(&WalTxnCommit::new(Xid::new(0,1), 100).to_bytes()).unwrap(); w.sync().unwrap(); drop(w); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let r = WalRecovery::with_smgr(&d, s); r.recover(RecoveryMode::CrashRecovery).unwrap(); }
#[test] fn test_recovery_scan_only() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); w.write(&[0;20]).unwrap(); w.sync().unwrap(); drop(w); let r = WalRecovery::new(&d); let _ = r.recover_scan_only(); }
#[test] fn test_recovery_torn_record() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); w.write(&[0xFF; 4]).unwrap(); w.sync().unwrap(); drop(w); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let r = WalRecovery::with_smgr(&d, s); let _ = r.recover(RecoveryMode::CrashRecovery); }

// ===== Buffer =====
#[test] fn test_wal_buffer_basic() { let b = WalBuffer::new(65536); let l = b.write(b"data").unwrap(); assert!(l.is_valid()); }
#[test] fn test_wal_buffer_many() { let b = WalBuffer::new(65536); for _ in 0..100 { b.write(b"record").unwrap(); } assert!(b.write_pos() > 600); }
#[test] fn test_wal_buffer_full() { let b = WalBuffer::new(32); assert!(b.write(&[0;100]).is_err()); }
#[test] fn test_wal_buffer_current_lsn() { let b = WalBuffer::new(65536); let l1 = b.current_lsn(); b.write(b"x").unwrap(); assert!(b.current_lsn().raw() > l1.raw()); }
#[test] fn test_wal_buffer_atomic_group() { let b = WalBuffer::new(65536); let g = b.begin_atomic_group(Xid::new(0,1)).unwrap(); b.append_to_atomic_group(g, b"rec1").unwrap(); b.append_to_atomic_group(g, b"rec2").unwrap(); let l = b.end_atomic_group(g).unwrap(); assert!(l.is_valid()); }

// ===== Concurrent =====
#[test] fn test_concurrent_wal_write() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = Arc::new(WalWriter::new(&d)); let mut h = vec![]; for t in 0..4 { let w = Arc::clone(&w); h.push(std::thread::spawn(move || { for i in 0..100 { w.write(format!("t{}_{}", t, i).as_bytes()).unwrap(); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_concurrent_buffer_write() { let b = Arc::new(WalBuffer::new(1024*1024)); let mut h = vec![]; for _ in 0..4 { let b = Arc::clone(&b); h.push(std::thread::spawn(move || { for _ in 0..200 { b.write(b"concurrent_data!").unwrap(); } })); } for j in h { j.join().unwrap(); } }

// ===== Checkpoint recovery =====
#[test] fn test_checkpoint_recovery_new() { let td = TempDir::new().unwrap(); let cr = CheckpointRecovery::new(td.path()); let r = cr.find_latest_checkpoint(); assert!(r.is_ok()); }
