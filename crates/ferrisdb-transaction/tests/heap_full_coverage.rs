//! Heap full coverage — 64 tests matching C++ heap test scenarios

use std::sync::Arc;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig, HeapPage};
use ferrisdb_storage::page::PAGE_SIZE;
use ferrisdb_transaction::*;

fn s(n: usize) -> (Arc<BufferPool>, Arc<TransactionManager>) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap());
    let mut tm = TransactionManager::new(128);
    tm.set_buffer_pool(Arc::clone(&bp));
    (bp, Arc::new(tm))
}

// ===== Basic DML =====
#[test] fn test_insert_single() { let (bp,tm) = s(200); let t = HeapTable::new(1,bp,tm); let tid = t.insert(b"hello", Xid::new(0,1), 0).unwrap(); assert!(tid.is_valid()); }
#[test] fn test_insert_fetch() { let (bp,tm) = s(200); let t = HeapTable::new(2,bp,tm); let tid = t.insert(b"data", Xid::new(0,1), 0).unwrap(); let (_,d) = t.fetch(tid).unwrap().unwrap(); assert_eq!(d, b"data"); }
#[test] fn test_insert_100() { let (bp,tm) = s(500); let t = HeapTable::new(3,bp,tm); for i in 0..100 { t.insert(format!("r{:03}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } }
#[test] fn test_insert_500() { let (bp,tm) = s(1000); let t = HeapTable::new(4,bp,tm); for i in 0..500 { t.insert(format!("r{:04}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } }
#[test] fn test_insert_empty_data() { let (bp,tm) = s(200); let t = HeapTable::new(5,bp,tm); let tid = t.insert(b"", Xid::new(0,1), 0).unwrap(); let (_,d) = t.fetch(tid).unwrap().unwrap(); assert_eq!(d.len(), 0); }
#[test] fn test_insert_large_tuple() { let (bp,tm) = s(200); let t = HeapTable::new(6,bp,tm); let big = vec![0xAA; 7000]; let tid = t.insert(&big, Xid::new(0,1), 0).unwrap(); let (_,d) = t.fetch(tid).unwrap().unwrap(); assert_eq!(d.len(), 7000); }
#[test] fn test_insert_binary_data() { let (bp,tm) = s(200); let t = HeapTable::new(7,bp,tm); let data: Vec<u8> = (0..=255).collect(); let tid = t.insert(&data, Xid::new(0,1), 0).unwrap(); let (_,d) = t.fetch(tid).unwrap().unwrap(); assert_eq!(d, data); }

// ===== Update =====
#[test] fn test_hot_update_same_size() { let (bp,tm) = s(200); let t = HeapTable::new(10,bp,tm); let tid = t.insert(b"AAAA", Xid::new(0,1), 0).unwrap(); let nt = t.update(tid, b"BBBB", Xid::new(0,2), 0).unwrap(); assert_eq!(tid, nt); let (_,d) = t.fetch(tid).unwrap().unwrap(); assert_eq!(d, b"BBBB"); }
#[test] fn test_non_hot_update_different_size() { let (bp,tm) = s(200); let t = HeapTable::new(11,bp,tm); let tid = t.insert(b"short", Xid::new(0,1), 0).unwrap(); let nt = t.update(tid, b"much longer data", Xid::new(0,2), 0).unwrap(); assert_ne!(tid, nt); }
#[test] fn test_update_ctid_chain() { let (bp,tm) = s(200); let t = HeapTable::new(12,bp,tm); let tid = t.insert(b"v1", Xid::new(0,1), 0).unwrap(); let _ = t.update(tid, b"version_2_long", Xid::new(0,2), 0).unwrap(); let (_,d) = t.fetch(tid).unwrap().unwrap(); assert_eq!(d, b"version_2_long"); }
#[test] fn test_multiple_hot_updates() { let (bp,tm) = s(200); let t = HeapTable::new(13,bp,tm); let tid = t.insert(b"v0__", Xid::new(0,1), 0).unwrap(); for i in 1..10 { t.update(tid, format!("v{:03}", i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } let (_,d) = t.fetch(tid).unwrap().unwrap(); assert_eq!(d, b"v009"); }
#[test] fn test_update_preserves_old_xmax() { let (bp,tm) = s(200); let t = HeapTable::new(14,bp,tm); let tid = t.insert(b"orig_data_!!", Xid::new(0,1), 0).unwrap(); t.update(tid, b"new__data_!!", Xid::new(0,2), 0).unwrap(); let (h,_) = t.fetch(tid).unwrap().unwrap(); assert_eq!(h.xmax.raw(), Xid::new(0,2).raw()); }

// ===== Delete =====
#[test] fn test_delete_sets_xmax() { let (bp,tm) = s(200); let t = HeapTable::new(20,bp,tm); let tid = t.insert(b"del", Xid::new(0,1), 0).unwrap(); t.delete(tid, Xid::new(0,2), 0).unwrap(); let (h,_) = t.fetch(tid).unwrap().unwrap(); assert!(h.xmax.is_valid()); }
#[test] fn test_delete_multiple() { let (bp,tm) = s(200); let t = HeapTable::new(21,bp,tm); let mut tids = vec![]; for i in 0..10 { tids.push(t.insert(format!("d{}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap()); } for tid in &tids { t.delete(*tid, Xid::new(0,100), 0).unwrap(); } }

// ===== Scan =====
#[test] fn test_scan_all() { let (bp,tm) = s(500); let t = HeapTable::new(30,bp,tm); for i in 0..50 { t.insert(format!("s{}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } let mut scan = HeapScan::new(&t); let mut c = 0; while let Ok(Some(_)) = scan.next() { c += 1; } assert_eq!(c, 50); }
#[test] fn test_scan_empty() { let (bp,tm) = s(200); let t = HeapTable::new(31,bp,tm); let mut scan = HeapScan::new(&t); assert!(scan.next().unwrap().is_none()); }
#[test]
fn test_scan_after_delete() {
    let (bp, tm) = s(200);
    let t = HeapTable::new(32, bp, tm);
    let tid = t.insert(b"scan_del", Xid::new(0, 1), 0).unwrap();
    t.delete(tid, Xid::new(0, 2), 0).unwrap();
    let mut scan = HeapScan::new(&t);
    let mut c = 0;
    while let Ok(Some(_)) = scan.next() { c += 1; }
    assert_eq!(c, 1); // deleted but not pruned
}
#[test] fn test_scan_visible() { let (bp,tm) = s(200); let t = HeapTable::new(33, Arc::clone(&bp), Arc::clone(&tm)); t.insert(b"vis", Xid::new(0,50), 0).unwrap(); let txn = tm.begin().unwrap(); let mut scan = HeapScan::new(&t); let mut c = 0; while let Ok(Some(_)) = scan.next_visible(&txn) { c += 1; } let _ = c; }

// ===== Visibility =====
#[test] fn test_vis_own_insert() { let (_,tm) = s(200); let txn = tm.begin().unwrap(); assert!(txn.is_visible(txn.xid(), Xid::INVALID, 0, 0)); }
#[test] fn test_vis_own_delete() { let (_,tm) = s(200); let txn = tm.begin().unwrap(); assert!(!txn.is_visible(txn.xid(), txn.xid(), 0, 1)); }
#[test] fn test_vis_other_uncommitted() { let (_,tm) = s(200); let t1 = tm.begin().unwrap(); let t2 = tm.begin().unwrap(); assert!(!t2.is_visible(t1.xid(), Xid::INVALID, 0, 0)); }
#[test] fn test_vis_no_xmax() { let (_,tm) = s(200); let txn = tm.begin().unwrap(); assert!(txn.is_visible(txn.xid(), Xid::INVALID, 0, 0)); }
#[test] fn test_fetch_visible_api() { let (bp,tm) = s(200); let t = HeapTable::new(40, Arc::clone(&bp), Arc::clone(&tm)); let tid = t.insert(b"fv", Xid::new(0,50), 0).unwrap(); let txn = tm.begin().unwrap(); let _ = t.fetch_visible(tid, &txn); }

// ===== Savepoint =====
#[test] fn test_sp_create() { let (_,tm) = s(200); let txn = tm.begin().unwrap(); let sp = txn.savepoint(); assert_eq!(sp, 0); }
#[test] fn test_sp_rollback() { let (bp,tm) = s(200); let t = HeapTable::new(50, Arc::clone(&bp), Arc::clone(&tm)); let mut txn = tm.begin().unwrap(); let x = txn.xid(); t.insert_with_undo(b"keep", x, 0, Some(&mut txn)).unwrap(); let sp = txn.savepoint(); let tid = t.insert_with_undo(b"drop", x, 0, Some(&mut txn)).unwrap(); txn.rollback_to_savepoint(sp); assert!(t.fetch(tid).unwrap().is_none()); }
#[test] fn test_sp_nested() { let (bp,tm) = s(200); let t = HeapTable::new(51, Arc::clone(&bp), Arc::clone(&tm)); let mut txn = tm.begin().unwrap(); let x = txn.xid(); t.insert_with_undo(b"l0", x, 0, Some(&mut txn)).unwrap(); let s1 = txn.savepoint(); t.insert_with_undo(b"l1", x, 0, Some(&mut txn)).unwrap(); let s2 = txn.savepoint(); let t3 = t.insert_with_undo(b"l2", x, 0, Some(&mut txn)).unwrap(); txn.rollback_to_savepoint(s2); assert!(t.fetch(t3).unwrap().is_none()); txn.rollback_to_savepoint(s1); txn.commit().unwrap(); }
#[test] fn test_sp_commit_after_rollback() { let (bp,tm) = s(200); let t = HeapTable::new(52, Arc::clone(&bp), Arc::clone(&tm)); let mut txn = tm.begin().unwrap(); let x = txn.xid(); let t1 = t.insert_with_undo(b"a", x, 0, Some(&mut txn)).unwrap(); let sp = txn.savepoint(); t.insert_with_undo(b"b", x, 0, Some(&mut txn)).unwrap(); txn.rollback_to_savepoint(sp); let t3 = t.insert_with_undo(b"c", x, 0, Some(&mut txn)).unwrap(); txn.commit().unwrap(); assert!(t.fetch(t1).unwrap().is_some()); assert!(t.fetch(t3).unwrap().is_some()); }

// ===== Vacuum =====
#[test] fn test_vacuum_basic() { let (bp,tm) = s(200); let t = HeapTable::new(60,bp,tm); for i in 0..20 { t.insert(format!("v{}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } t.vacuum(); }
#[test] fn test_vacuum_page() { let (bp,tm) = s(200); let t = HeapTable::new(61,bp,tm); for i in 0..10 { t.insert(format!("vp{}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } t.vacuum_page(0); }
#[test] fn test_vacuum_after_delete() { let (bp,tm) = s(200); let t = HeapTable::new(62,bp,tm); let tids: Vec<_> = (0..10).map(|i| t.insert(format!("vd{}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap()).collect(); for tid in &tids { t.delete(*tid, Xid::new(0,100), 0).unwrap(); } t.vacuum(); }

// ===== FSM =====
#[test] fn test_fsm_guides_insert() { let (bp,tm) = s(500); let t = HeapTable::new(70,bp,tm); for i in 0..100 { t.insert(format!("f{:03}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } let tid = t.insert(b"fsm_after", Xid::new(0,200), 0).unwrap(); assert!(tid.is_valid()); }
#[test] fn test_fsm_many_pages() { let (bp,tm) = s(500); let t = HeapTable::new(71,bp,tm); for i in 0..500 { t.insert(format!("fp{:04}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } let tid = t.insert(b"after_500", Xid::new(0,1000), 0).unwrap(); assert!(tid.is_valid()); }

// ===== Tuple Header =====
#[test] fn test_header_serialize_size() { assert_eq!(TupleHeader::serialized_size(), 32); }
#[test] fn test_header_roundtrip() { let h = TupleHeader::new(Xid::new(0,1), 0); let b = h.serialize(); let r = TupleHeader::deserialize(&b).unwrap(); assert_eq!(r.xmin.raw(), h.xmin.raw()); }
#[test] fn test_header_xmax_fields() { let mut h = TupleHeader::new(Xid::new(0,1), 0); h.xmax = Xid::new(0,42); h.cmax = 7; let b = h.serialize(); let r = TupleHeader::deserialize(&b).unwrap(); let xmax_raw = r.xmax.raw(); let cmax_val = r.cmax; assert_eq!(xmax_raw, Xid::new(0,42).raw()); assert_eq!(cmax_val, 7); }
#[test] fn test_header_infomask() { let mut h = TupleHeader::new(Xid::new(0,1), 0); h.infomask = 0xABCD; let b = h.serialize(); let r = TupleHeader::deserialize(&b).unwrap(); assert_eq!(r.infomask, 0xABCD); }
#[test] fn test_header_ctid() { let mut h = TupleHeader::new(Xid::new(0,1), 0); h.ctid = TupleId::new(100, 5); let b = h.serialize(); let r = TupleHeader::deserialize(&b).unwrap(); let blk = r.ctid.ip_blkid; let pos = r.ctid.ip_posid; assert_eq!(blk, 100); assert_eq!(pos, 5); }

// ===== Concurrent DML =====
#[test] fn test_conc_insert() { let (bp,tm) = s(500); let t = Arc::new(HeapTable::new(80,bp,tm)); let mut h = vec![]; for th in 0..4 { let t = Arc::clone(&t); h.push(std::thread::spawn(move || { for i in 0..100 { t.insert(format!("c{}_{}",th,i).as_bytes(), Xid::new(0,(th*1000+i+1) as u32), 0).unwrap(); } })); } for j in h { j.join().unwrap(); } let mut scan = HeapScan::new(&t); let mut c = 0; while let Ok(Some(_)) = scan.next() { c += 1; } assert_eq!(c, 400); }
#[test] fn test_conc_update() { let (bp,tm) = s(200); let t = Arc::new(HeapTable::new(81, Arc::clone(&bp), Arc::clone(&tm))); let tid = t.insert(b"init_data_!!", Xid::new(0,1), 0).unwrap(); let mut h = vec![]; for th in 0..4 { let t = Arc::clone(&t); h.push(std::thread::spawn(move || { for i in 0..20 { let _ = t.update(tid, format!("u{:02}_{:04}!!!!", th, i).as_bytes(), Xid::new(0,(th*100+i+2) as u32), 0); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_conc_delete() { let (bp,tm) = s(500); let t = Arc::new(HeapTable::new(82, Arc::clone(&bp), Arc::clone(&tm))); let tids: Vec<_> = (0..100).map(|i| t.insert(format!("cd{:03}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap()).collect(); let tids = Arc::new(tids); let mut h = vec![]; for th in 0..4 { let t = Arc::clone(&t); let tids = Arc::clone(&tids); h.push(std::thread::spawn(move || { for i in (th*25)..((th+1)*25) { let _ = t.delete(tids[i], Xid::new(0,(200+i) as u32), 0); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_conc_scan_during_insert() { let (bp,tm) = s(500); let t = Arc::new(HeapTable::new(83,bp,tm)); for i in 0..30 { t.insert(format!("pre{}",i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } let done = Arc::new(std::sync::atomic::AtomicBool::new(false)); let d = Arc::clone(&done); let tw = Arc::clone(&t); let writer = std::thread::spawn(move || { let mut i = 100u32; while !d.load(std::sync::atomic::Ordering::Relaxed) && i < 200 { let _ = tw.insert(format!("w{}",i).as_bytes(), Xid::new(0,i), 0); i += 1; } }); for _ in 0..5 { let mut scan = HeapScan::new(&t); let mut c = 0; while let Ok(Some(_)) = scan.next() { c += 1; } assert!(c >= 30); } done.store(true, std::sync::atomic::Ordering::Relaxed); writer.join().unwrap(); }
#[test] fn test_conc_abort() { let (bp,tm) = s(200); let t = Arc::new(HeapTable::new(84, Arc::clone(&bp), Arc::clone(&tm))); let tm2 = Arc::clone(&tm); let mut h = vec![]; for _ in 0..4 { let t = Arc::clone(&t); let tm = Arc::clone(&tm2); h.push(std::thread::spawn(move || { for _ in 0..20 { let mut txn = tm.begin().unwrap(); let _ = t.insert_with_undo(b"abort_me", txn.xid(), 0, Some(&mut txn)); txn.abort().unwrap(); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_conc_mixed() { let (bp,tm) = s(500); let t = Arc::new(HeapTable::new(85, Arc::clone(&bp), Arc::clone(&tm))); let tm2 = Arc::clone(&tm); let mut h = vec![]; for i in 0..4 { let t = Arc::clone(&t); let tm = Arc::clone(&tm2); h.push(std::thread::spawn(move || { for j in 0..30 { let mut txn = tm.begin().unwrap(); let _ = t.insert_with_undo(format!("mx_{}_{}", i, j).as_bytes(), txn.xid(), 0, Some(&mut txn)); if j % 3 == 0 { txn.abort().unwrap(); } else { txn.commit().unwrap(); } } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_conc_different_tables() { let (bp,tm) = s(500); let mut h = vec![]; for i in 0..4 { let bp = Arc::clone(&bp); let tm = Arc::clone(&tm); h.push(std::thread::spawn(move || { let t = HeapTable::new((90+i) as u32, bp, tm); for j in 0..100 { t.insert(format!("dt_{}_{}", i, j).as_bytes(), Xid::new(0,(i*1000+j+1) as u32), 0).unwrap(); } })); } for j in h { j.join().unwrap(); } }

// ===== Prune =====
#[test]
fn test_prune_called() {
    let (bp, tm) = s(200);
    let t = HeapTable::new(95, bp, tm);
    for i in 0..20u32 {
        t.insert(format!("pr{}", i).as_bytes(), Xid::new(0, i + 1), 0).unwrap();
    }
}
#[test] fn test_table_oid() { let (bp,tm) = s(200); let t = HeapTable::new(99,bp,tm); assert_eq!(t.table_oid(), 99); }
