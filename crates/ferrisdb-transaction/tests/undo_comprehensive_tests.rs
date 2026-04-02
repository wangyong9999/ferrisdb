//! Undo system comprehensive tests — zone lifecycle, WAL persistence, recovery

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager, WalWriter};
use ferrisdb_transaction::*;

fn setup(buf: usize) -> (Arc<BufferPool>, Arc<TransactionManager>) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(buf)).unwrap());
    let mut tm = TransactionManager::new(128);
    tm.set_buffer_pool(Arc::clone(&bp));
    (bp, Arc::new(tm))
}

fn setup_wal(buf: usize) -> (Arc<BufferPool>, Arc<TransactionManager>, Arc<WalWriter>, TempDir) {
    let td = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let wd = td.path().join("wal"); std::fs::create_dir_all(&wd).unwrap();
    let w = Arc::new(WalWriter::new(&wd));
    let mut bp = BufferPool::new(BufferPoolConfig::new(buf)).unwrap();
    bp.set_smgr(smgr);
    let bp = Arc::new(bp);
    let mut tm = TransactionManager::new(128);
    tm.set_buffer_pool(Arc::clone(&bp));
    tm.set_wal_writer(Arc::clone(&w));
    (bp, Arc::new(tm), w, td)
}

// ===== Undo Zone =====
#[test] fn test_zone_basic() { let z = UndoZone::new(0, 0); assert_eq!(z.zone_id(), 0); }
#[test] fn test_zone_xid() { let z = UndoZone::new(0, 0); z.set_xid(Xid::new(0, 5)); assert_eq!(z.xid().raw(), Xid::new(0, 5).raw()); }
#[test] fn test_zone_alloc_sequential() { let z = UndoZone::new(0, 0); let (_, o1) = z.allocate(100).unwrap(); let (_, o2) = z.allocate(200).unwrap(); assert_eq!(o1, 0); assert_eq!(o2, 100); }
#[test] fn test_zone_alloc_page_boundary() { let z = UndoZone::new(0, 0); z.allocate(8000).unwrap(); let (p, _) = z.allocate(500).unwrap(); assert_eq!(p, 1); }
#[test] fn test_zone_reset_clears() { let z = UndoZone::new(0, 0); z.set_xid(Xid::new(0, 1)); z.allocate(100).unwrap(); z.reset(); assert!(!z.xid().is_valid()); }
#[test] fn test_zone_multiple_allocs() { let z = UndoZone::new(0, 0); for _ in 0..100 { z.allocate(50).unwrap(); } }

// ===== Undo Record =====
#[test] fn test_record_insert() { let r = UndoRecord::new_insert(Xid::new(0,1), 100, (1,0)); assert_eq!(r.record_type(), UndoRecordType::Insert); }
#[test] fn test_record_delete() { let r = UndoRecord::new_delete(Xid::new(0,1), 100, (1,0), b"old"); assert_eq!(r.record_type(), UndoRecordType::Delete); assert_eq!(r.data, b"old"); }
#[test] fn test_record_update() { let r = UndoRecord::new_update(Xid::new(0,1), 100, (1,0), (2,0), b"data"); assert_eq!(r.record_type(), UndoRecordType::Update); }
#[test] fn test_record_commit() { let r = UndoRecord::new_commit(Xid::new(0,1), ferrisdb_core::Csn::from_raw(42)); assert_eq!(r.record_type(), UndoRecordType::Commit); assert_eq!(r.csn().raw(), 42); }
#[test] fn test_record_serialize() { let r = UndoRecord::new_insert(Xid::new(0,1), 200, (5,3)); let b = r.serialize(); assert!(b.len() >= 32); }
#[test] fn test_record_xid() { let r = UndoRecord::new_insert(Xid::new(0,42), 1, (0,0)); assert_eq!(r.xid().raw(), Xid::new(0,42).raw()); }

// ===== Abort Rollback =====
#[test] fn test_abort_insert_undo() { let (bp, tm) = setup(200); let t = HeapTable::new(1, Arc::clone(&bp), Arc::clone(&tm)); let mut txn = tm.begin().unwrap(); let xid = txn.xid(); let tid = t.insert_with_undo(b"gone", xid, 0, Some(&mut txn)).unwrap(); txn.abort().unwrap(); assert!(t.fetch(tid).unwrap().is_none()); }
#[test] fn test_abort_delete_undo() { let (bp, tm) = setup(200); let t = HeapTable::new(2, Arc::clone(&bp), Arc::clone(&tm)); let tid = t.insert(b"keep", Xid::new(0,50), 0).unwrap(); let mut txn = tm.begin().unwrap(); t.delete_with_undo(tid, txn.xid(), 0, Some(&mut txn)).unwrap(); txn.abort().unwrap(); let (h, d) = t.fetch(tid).unwrap().unwrap(); assert_eq!(d, b"keep"); assert_eq!(h.xmax.raw(), Xid::INVALID.raw()); }
#[test] fn test_abort_update_undo() { let (bp, tm) = setup(200); let t = HeapTable::new(3, Arc::clone(&bp), Arc::clone(&tm)); let tid = t.insert(b"original_val!", Xid::new(0,50), 0).unwrap(); let mut txn = tm.begin().unwrap(); t.update_with_undo(tid, b"replaced_val!", txn.xid(), 0, Some(&mut txn)).unwrap(); txn.abort().unwrap(); let (_, d) = t.fetch(tid).unwrap().unwrap(); assert_eq!(d, b"original_val!"); }
#[test] fn test_savepoint_undo() { let (bp, tm) = setup(200); let t = HeapTable::new(4, Arc::clone(&bp), Arc::clone(&tm)); let mut txn = tm.begin().unwrap(); let xid = txn.xid(); let t1 = t.insert_with_undo(b"keep", xid, 0, Some(&mut txn)).unwrap(); let sp = txn.savepoint(); let t2 = t.insert_with_undo(b"discard", xid, 0, Some(&mut txn)).unwrap(); txn.rollback_to_savepoint(sp); assert!(t.fetch(t1).unwrap().is_some()); assert!(t.fetch(t2).unwrap().is_none()); txn.commit().unwrap(); }

// ===== WAL Undo Persistence =====
#[test] fn test_undo_writes_wal() { let (bp, tm, w, _td) = setup_wal(200); let t = HeapTable::with_wal_writer(10, Arc::clone(&bp), Arc::clone(&tm), Arc::clone(&w)); let before = w.offset(); let mut txn = tm.begin().unwrap(); t.insert_with_undo(b"wal_undo", txn.xid(), 0, Some(&mut txn)).unwrap(); txn.commit().unwrap(); assert!(w.offset() > before, "Undo+commit should write WAL"); }
#[test] fn test_abort_writes_wal() { let (bp, tm, w, _td) = setup_wal(200); let t = HeapTable::with_wal_writer(11, Arc::clone(&bp), Arc::clone(&tm), Arc::clone(&w)); let mut txn = tm.begin().unwrap(); t.insert_with_undo(b"wal_abort", txn.xid(), 0, Some(&mut txn)).unwrap(); let before = w.offset(); txn.abort().unwrap(); assert!(w.offset() > before, "Abort should write WAL record"); }

// ===== Concurrent Abort =====
#[test] fn test_concurrent_abort() { let (bp, tm) = setup(500); let t = Arc::new(HeapTable::new(20, Arc::clone(&bp), Arc::clone(&tm))); let tm2 = Arc::clone(&tm); let mut h = vec![]; for _ in 0..4 { let t = Arc::clone(&t); let tm = Arc::clone(&tm2); h.push(std::thread::spawn(move || { for _ in 0..20 { let mut txn = tm.begin().unwrap(); let _ = t.insert_with_undo(b"abort_me", txn.xid(), 0, Some(&mut txn)); txn.abort().unwrap(); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_concurrent_commit_abort_mix() { let (bp, tm) = setup(500); let t = Arc::new(HeapTable::new(21, Arc::clone(&bp), Arc::clone(&tm))); let tm2 = Arc::clone(&tm); let mut h = vec![]; for i in 0..4 { let t = Arc::clone(&t); let tm = Arc::clone(&tm2); h.push(std::thread::spawn(move || { for j in 0..20 { let mut txn = tm.begin().unwrap(); let _ = t.insert_with_undo(format!("mix_{}_{}", i, j).as_bytes(), txn.xid(), 0, Some(&mut txn)); if j % 2 == 0 { txn.commit().unwrap(); } else { txn.abort().unwrap(); } } })); } for j in h { j.join().unwrap(); } }
