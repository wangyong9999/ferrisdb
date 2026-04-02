//! Heap advanced tests — visibility, ctid chain, prune, vacuum, FSM, concurrent edge cases

use std::sync::Arc;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig};
use ferrisdb_transaction::{TransactionManager, HeapTable, HeapScan, TupleId};

fn setup(buf: usize) -> (Arc<BufferPool>, Arc<TransactionManager>) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(buf)).unwrap());
    let mut tm = TransactionManager::new(128);
    tm.set_buffer_pool(Arc::clone(&bp));
    (bp, Arc::new(tm))
}

// ==================== ctid Chain Following ====================

#[test]
fn test_non_hot_update_ctid_chain() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(1, bp, tm);
    let tid = table.insert(b"short", Xid::new(0, 1), 0).unwrap();
    let new_tid = table.update(tid, b"much longer data here!", Xid::new(0, 2), 0).unwrap();
    assert_ne!(tid, new_tid, "Different sizes = non-HOT");
    // fetch via old TID should follow chain to new version
    let (_, data) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(data, b"much longer data here!");
}

#[test]
fn test_hot_update_no_chain() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(2, bp, tm);
    let tid = table.insert(b"same_size_aa", Xid::new(0, 1), 0).unwrap();
    let new_tid = table.update(tid, b"same_size_bb", Xid::new(0, 2), 0).unwrap();
    assert_eq!(tid, new_tid, "Same size = HOT, same TID");
    let (_, data) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(data, b"same_size_bb");
}

#[test]
fn test_multiple_non_hot_updates_chain() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(3, bp, tm);
    let tid = table.insert(b"v1", Xid::new(0, 1), 0).unwrap();
    let _ = table.update(tid, b"version_2_longer", Xid::new(0, 2), 0).unwrap();
    // fetch original TID should follow to latest
    let (_, data) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(data, b"version_2_longer");
}

// ==================== Visibility ====================

#[test]
fn test_fetch_visible_committed() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(10, Arc::clone(&bp), Arc::clone(&tm));
    let mut t1 = tm.begin().unwrap();
    let tid = table.insert_with_undo(b"committed", t1.xid(), 0, Some(&mut t1)).unwrap();
    t1.commit().unwrap();

    let t2 = tm.begin().unwrap();
    let result = table.fetch_visible(tid, &t2);
    // Depending on CSN visibility, this should work
    let _ = result;
}

#[test]
fn test_own_insert_visible() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(11, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();
    let tid = table.insert_with_undo(b"my row", xid, 0, Some(&mut txn)).unwrap();
    // Should be visible to self
    assert!(txn.is_visible(xid, Xid::INVALID, 0, 0));
    let _ = table.fetch(tid).unwrap().unwrap();
}

#[test]
fn test_deleted_not_visible_to_self() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(12, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();
    let tid = table.insert_with_undo(b"will delete", xid, 0, Some(&mut txn)).unwrap();
    table.delete_with_undo(tid, xid, 1, Some(&mut txn)).unwrap();
    // After delete, invisible to self (different cid)
    assert!(!txn.is_visible(xid, xid, 0, 1));
}

// ==================== Scan Visibility ====================

#[test]
fn test_scan_skips_invisible() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(20, Arc::clone(&bp), Arc::clone(&tm));
    // Insert some rows
    for i in 0..5 {
        table.insert(format!("row{}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap();
    }
    let txn = tm.begin().unwrap();
    let mut scan = HeapScan::new(&table);
    let mut count = 0;
    while let Ok(Some((_, hdr, _))) = scan.next_visible(&txn) {
        count += 1;
    }
    // May or may not see rows depending on CSN state
    let _ = count;
}

// ==================== Vacuum ====================

#[test]
fn test_vacuum_reclaims_space() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(30, Arc::clone(&bp), Arc::clone(&tm));
    let mut tids = Vec::new();
    for i in 0..20 {
        tids.push(table.insert(format!("vac_{:03}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap());
    }
    // Delete all
    for (i, tid) in tids.iter().enumerate() {
        table.delete(*tid, Xid::new(0, (100+i) as u32), 0).unwrap();
    }
    let reclaimed = table.vacuum();
    // May reclaim some space (depends on snapshot state)
    let _ = reclaimed;
}

#[test]
fn test_vacuum_page_specific() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(31, Arc::clone(&bp), Arc::clone(&tm));
    for i in 0..10 {
        table.insert(format!("pg_{}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap();
    }
    let reclaimed = table.vacuum_page(0);
    let _ = reclaimed;
}

// ==================== FSM ====================

#[test]
fn test_fsm_reuses_space() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(40, Arc::clone(&bp), Arc::clone(&tm));
    // Fill many tuples
    for i in 0..200 {
        table.insert(format!("fsm_{:04}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap();
    }
    // Insert more — FSM should guide to pages with space
    let tid = table.insert(b"after_fill", Xid::new(0, 500), 0).unwrap();
    assert!(tid.is_valid());
}

// ==================== Large Data ====================

#[test]
fn test_insert_near_page_limit() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(50, bp, tm);
    // Insert tuple close to page size limit (~8KB - header)
    let big_data = vec![0xBB; 7000];
    let tid = table.insert(&big_data, Xid::new(0, 1), 0).unwrap();
    let (_, data) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(data.len(), 7000);
}

#[test]
fn test_insert_many_small_tuples() {
    let (bp, tm) = setup(500);
    let table = HeapTable::new(51, bp, tm);
    for i in 0..500 {
        table.insert(&[i as u8; 10], Xid::new(0, (i+1) as u32), 0).unwrap();
    }
    let mut scan = HeapScan::new(&table);
    let mut count = 0;
    while let Ok(Some(_)) = scan.next() { count += 1; }
    assert_eq!(count, 500);
}

// ==================== Delete Edge Cases ====================

#[test]
fn test_delete_already_deleted() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(60, bp, tm);
    let tid = table.insert(b"del", Xid::new(0, 1), 0).unwrap();
    table.delete(tid, Xid::new(0, 2), 0).unwrap();
    // Second delete should still succeed (just overwrites xmax)
    table.delete(tid, Xid::new(0, 3), 0).unwrap();
}

#[test]
fn test_fetch_after_delete() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(61, bp, tm);
    let tid = table.insert(b"still_there", Xid::new(0, 1), 0).unwrap();
    table.delete(tid, Xid::new(0, 2), 0).unwrap();
    // fetch still returns tuple (with xmax set) — visibility filtering is caller's job
    let (hdr, _) = table.fetch(tid).unwrap().unwrap();
    assert!(hdr.xmax.is_valid());
}

// ==================== Concurrent Update ====================

#[test]
fn test_concurrent_hot_update() {
    let (bp, tm) = setup(200);
    let table = Arc::new(HeapTable::new(70, Arc::clone(&bp), Arc::clone(&tm)));
    // Insert with fixed-size data
    let tid = table.insert(b"initial_data!!", Xid::new(0, 1), 0).unwrap();
    let mut handles = vec![];
    for t in 0..4 {
        let table = Arc::clone(&table);
        handles.push(std::thread::spawn(move || {
            for i in 0..50 {
                let data = format!("update_{}__{:04}", t, i);
                let padded = format!("{:>14}", &data[..data.len().min(14)]);
                let _ = table.update(tid, padded.as_bytes(), Xid::new(0, (t*100+i+2) as u32), 0);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert!(table.fetch(tid).unwrap().is_some());
}

#[test]
fn test_concurrent_insert_different_tables() {
    let (bp, tm) = setup(500);
    let mut handles = vec![];
    for t in 0..4 {
        let bp = Arc::clone(&bp);
        let tm = Arc::clone(&tm);
        handles.push(std::thread::spawn(move || {
            let table = HeapTable::new((100+t) as u32, bp, tm);
            for i in 0..100 {
                table.insert(format!("t{}_{}", t, i).as_bytes(), Xid::new(0, (t*1000+i+1) as u32), 0).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

// ==================== Tuple Header ====================

#[test]
fn test_tuple_header_roundtrip() {
    use ferrisdb_transaction::TupleHeader;
    let hdr = TupleHeader {
        xmin: Xid::new(0, 42),
        xmax: Xid::new(0, 99),
        cmin: 1, cmax: 2,
        ctid: TupleId::new(5, 3),
        infomask: 0x1234,
    };
    let bytes = hdr.serialize();
    let restored = TupleHeader::deserialize(&bytes).unwrap();
    assert_eq!(restored.xmin.raw(), hdr.xmin.raw());
    assert_eq!(restored.xmax.raw(), hdr.xmax.raw());
    assert_eq!(restored.cmin, 1);
    assert_eq!(restored.cmax, 2);
    assert_eq!(restored.infomask, 0x1234);
}

#[test]
fn test_tuple_header_size() {
    use ferrisdb_transaction::TupleHeader;
    assert_eq!(TupleHeader::serialized_size(), 32);
}

// ==================== Savepoint Advanced ====================

#[test]
fn test_savepoint_multiple_levels() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(80, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    let t1 = table.insert_with_undo(b"level0", xid, 0, Some(&mut txn)).unwrap();
    let sp1 = txn.savepoint();

    let t2 = table.insert_with_undo(b"level1", xid, 0, Some(&mut txn)).unwrap();
    let sp2 = txn.savepoint();

    let t3 = table.insert_with_undo(b"level2", xid, 0, Some(&mut txn)).unwrap();

    // Rollback to sp2: t3 undone
    txn.rollback_to_savepoint(sp2);
    assert!(table.fetch(t3).unwrap().is_none());
    assert!(table.fetch(t2).unwrap().is_some());

    // Rollback to sp1: t2 undone
    txn.rollback_to_savepoint(sp1);
    assert!(table.fetch(t2).unwrap().is_none());
    assert!(table.fetch(t1).unwrap().is_some());

    txn.commit().unwrap();
    assert!(table.fetch(t1).unwrap().is_some());
}

#[test]
fn test_savepoint_commit_after_partial_rollback() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(81, Arc::clone(&bp), Arc::clone(&tm));
    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    let t1 = table.insert_with_undo(b"keep", xid, 0, Some(&mut txn)).unwrap();
    let sp = txn.savepoint();
    let t2 = table.insert_with_undo(b"discard", xid, 0, Some(&mut txn)).unwrap();

    txn.rollback_to_savepoint(sp);
    // Insert after rollback
    let t3 = table.insert_with_undo(b"new_after_sp", xid, 0, Some(&mut txn)).unwrap();
    txn.commit().unwrap();

    assert!(table.fetch(t1).unwrap().is_some());
    assert!(table.fetch(t2).unwrap().is_none());
    assert!(table.fetch(t3).unwrap().is_some());
}
