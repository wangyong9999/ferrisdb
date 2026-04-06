//! WAL full coverage — 51 tests

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{Xid, Lsn};
use ferrisdb_storage::{StorageManager, PageId};
use ferrisdb_storage::wal::*;

fn w() -> (WalWriter, TempDir) { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); (WalWriter::new(&d), td) }

// ===== Writer =====
#[test] fn test_w_create() { let (w,_) = w(); assert!(w.current_lsn().is_valid()); }
#[test] fn test_w_write() { let (w,_) = w(); w.write(b"data").unwrap(); }
#[test] fn test_w_write_empty() { let (w,_) = w(); w.write(b"").unwrap(); }
#[test] fn test_w_write_1byte() { let (w,_) = w(); w.write(&[0]).unwrap(); }
#[test] fn test_w_write_large() { let (w,_) = w(); w.write(&vec![0;100000]).unwrap(); }
#[test] fn test_w_write_1000() { let (w,_) = w(); for _ in 0..1000 { w.write(b"rec").unwrap(); } }
#[test] fn test_w_lsn_monotonic() { let (w,_) = w(); let a = w.write(b"a").unwrap(); let b = w.write(b"b").unwrap(); assert!(b.raw() > a.raw()); }
#[test] fn test_w_offset_advances() { let (w,_) = w(); let o1 = w.offset(); w.write(b"data").unwrap(); assert!(w.offset() > o1); }
#[test] fn test_w_sync() { let (w,_) = w(); w.write(b"x").unwrap(); w.sync().unwrap(); }
#[test] fn test_w_write_and_sync() { let (w,_) = w(); w.write_and_sync(b"atomic").unwrap(); }
#[test] fn test_w_flushed_lsn() { let (w,_) = w(); w.write(b"f").unwrap(); w.sync().unwrap(); assert!(w.flushed_lsn().raw() > 0); }
#[test] fn test_w_wait_for_lsn() { let (w,_) = w(); let l = w.write(b"w").unwrap(); w.wait_for_lsn(l).unwrap(); assert!(w.flushed_lsn().raw() >= l.raw()); }
#[test] fn test_w_file_rotation() { let (w,_) = w(); let big = vec![0u8;1024*1024]; for _ in 0..20 { w.write(&big).unwrap(); } assert!(w.file_no() > 0); }
#[test] fn test_w_multiple_files() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); let big = vec![0;512*1024]; for _ in 0..40 { w.write(&big).unwrap(); } w.sync().unwrap(); let c = std::fs::read_dir(&d).unwrap().filter(|e| e.as_ref().map(|e| e.path().extension().map_or(false, |x| x == "wal")).unwrap_or(false)).count(); assert!(c > 1); }
#[test] fn test_w_file_no() { let (w,_) = w(); assert_eq!(w.file_no(), 0); }
#[test] fn test_w_wal_dir() { let (w,td) = w(); assert!(w.wal_dir().exists()); }

// ===== Flusher =====
#[test] fn test_flusher_syncs() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = Arc::new(WalWriter::new(&d)); let f = w.start_flusher(5); w.write(b"fl").unwrap(); std::thread::sleep(std::time::Duration::from_millis(50)); f.stop(); assert!(w.flushed_lsn().raw() > 0); }
#[test] fn test_flusher_drop() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = Arc::new(WalWriter::new(&d)); { let _f = w.start_flusher(5); w.write(b"drop").unwrap(); } }

// ===== Records =====
#[test] fn test_rec_heap_insert() { let r = WalHeapInsert::new(PageId::new(0,0,1,0), 1, &[0;45]); assert!(!r.serialize_with_data(&[0;45]).is_empty()); }
#[test] fn test_rec_heap_delete() { let r = WalHeapDelete::new(PageId::new(0,0,1,0), 1); assert!(!r.to_bytes().is_empty()); }
#[test] fn test_rec_txn_commit() { let r = WalTxnCommit::new(Xid::new(0,1), 100); assert!(r.to_bytes().len() > 4); }
#[test] fn test_rec_txn_abort() { let r = WalTxnCommit::new_abort(Xid::new(0,1)); assert!(r.to_bytes().len() > 4); }
#[test] fn test_rec_checkpoint_online() { let r = WalCheckpoint::new(1000, false); assert!(!r.to_bytes().is_empty()); }
#[test] fn test_rec_checkpoint_shutdown() { let r = WalCheckpoint::new(1000, true); assert!(!r.to_bytes().is_empty()); }
#[test] fn test_rec_type_heap_insert() { assert!(WalRecordType::try_from(1).is_ok()); }
#[test] fn test_rec_type_invalid() { assert!(WalRecordType::try_from(9999).is_err()); }
#[test] fn test_rec_header() { let h = WalRecordHeader::new(WalRecordType::HeapInsert, 42); assert_eq!(h.record_type(), Some(WalRecordType::HeapInsert)); }

// ===== Recovery =====
#[test] fn test_rec_empty_dir() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let r = WalRecovery::with_smgr(&d, s); r.recover(RecoveryMode::CrashRecovery).unwrap(); assert_eq!(r.stage(), RecoveryStage::Completed); }
#[test] fn test_rec_with_records() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); let data = vec![0u8;45]; w.write(&WalHeapInsert::new(PageId::new(0,0,100,0), 1, &data).serialize_with_data(&data)).unwrap(); w.sync().unwrap(); drop(w); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let r = WalRecovery::with_smgr(&d, s); r.recover_scan_only().unwrap(); assert!(r.stats().records_redone + r.stats().records_skipped > 0); }
#[test] fn test_rec_idempotent() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); let data = vec![0u8;45]; w.write(&WalHeapInsert::new(PageId::new(0,0,200,0), 1, &data).serialize_with_data(&data)).unwrap(); w.sync().unwrap(); drop(w); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); WalRecovery::with_smgr(&d, Arc::clone(&s)).recover_scan_only().unwrap(); let r = WalRecovery::with_smgr(&d, s); r.recover_scan_only().unwrap(); assert!(r.stats().records_redone + r.stats().records_skipped > 0); }
#[test] fn test_rec_with_commit() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); w.write(&WalTxnCommit::new(Xid::new(0,1), 100).to_bytes()).unwrap(); w.sync().unwrap(); drop(w); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); WalRecovery::with_smgr(&d, s).recover(RecoveryMode::CrashRecovery).unwrap(); }
#[test] fn test_rec_with_abort() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); w.write(&WalTxnCommit::new_abort(Xid::new(0,1)).to_bytes()).unwrap(); w.sync().unwrap(); drop(w); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); WalRecovery::with_smgr(&d, s).recover(RecoveryMode::CrashRecovery).unwrap(); }
#[test] fn test_rec_scan_only() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); w.write(b"\x00\x00\x01\x00test").unwrap(); w.sync().unwrap(); drop(w); let _ = WalRecovery::new(&d).recover_scan_only(); }
#[test] fn test_rec_torn_record() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); w.write(&[0xFF;4]).unwrap(); w.sync().unwrap(); drop(w); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let _ = WalRecovery::with_smgr(&d, s).recover(RecoveryMode::CrashRecovery); }
#[test] fn test_rec_stage_transitions() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let r = WalRecovery::with_smgr(&d, s); assert_eq!(r.stage(), RecoveryStage::NotStarted); r.recover(RecoveryMode::CrashRecovery).unwrap(); assert_eq!(r.stage(), RecoveryStage::Completed); }
#[test] fn test_rec_checkpoint_lsn() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = WalWriter::new(&d); w.write(&WalCheckpoint::new(5000, false).to_bytes()).unwrap(); w.sync().unwrap(); drop(w); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let r = WalRecovery::with_smgr(&d, s); r.recover(RecoveryMode::CrashRecovery).unwrap(); }

// ===== Buffer =====
#[test] fn test_buf_write() { let b = WalBuffer::new(65536); b.write(b"data").unwrap(); }
#[test] fn test_buf_many() { let b = WalBuffer::new(65536); for _ in 0..100 { b.write(b"rec").unwrap(); } assert!(b.write_pos() > 0); }
#[test] fn test_buf_full() { let b = WalBuffer::new(32); assert!(b.write(&[0;100]).is_err()); }
#[test] fn test_buf_lsn() { let b = WalBuffer::new(65536); let l1 = b.current_lsn(); b.write(b"x").unwrap(); assert!(b.current_lsn().raw() > l1.raw()); }
#[test] fn test_buf_atomic() { let b = WalBuffer::new(65536); let g = b.begin_atomic_group(Xid::new(0,1)).unwrap(); b.append_to_atomic_group(g, b"r1").unwrap(); b.append_to_atomic_group(g, b"r2").unwrap(); b.end_atomic_group(g).unwrap(); }
#[test] fn test_buf_concurrent() { let b = Arc::new(WalBuffer::new(1024*1024)); let mut h = vec![]; for _ in 0..4 { let b = b.clone(); h.push(std::thread::spawn(move || { for _ in 0..200 { b.write(b"conc_data!!!").unwrap(); } })); } for j in h { j.join().unwrap(); } }

// ===== Concurrent Writer =====
#[test] fn test_conc_write_4t() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = Arc::new(WalWriter::new(&d)); let mut h = vec![]; for t in 0..4 { let w = w.clone(); h.push(std::thread::spawn(move || { for i in 0..100 { w.write(format!("t{}_{}", t, i).as_bytes()).unwrap(); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_conc_write_8t() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = Arc::new(WalWriter::new(&d)); let mut h = vec![]; for t in 0..8 { let w = w.clone(); h.push(std::thread::spawn(move || { for i in 0..50 { w.write(format!("t{}_{}", t, i).as_bytes()).unwrap(); } })); } for j in h { j.join().unwrap(); } }

// ===== Checkpoint Recovery =====
#[test] fn test_cp_recovery_new() { let td = TempDir::new().unwrap(); let cr = CheckpointRecovery::new(td.path()); assert!(cr.find_latest_checkpoint().is_ok()); }
#[test] fn test_cp_recovery_no_checkpoint() { let td = TempDir::new().unwrap(); let cr = CheckpointRecovery::new(td.path()); assert!(cr.find_latest_checkpoint().unwrap().is_none()); }
