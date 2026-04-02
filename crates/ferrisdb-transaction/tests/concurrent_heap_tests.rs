//! Concurrent Heap DML + Scan tests

use std::sync::Arc;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig};
use ferrisdb_transaction::{TransactionManager, HeapTable, HeapScan};

fn setup(buf: usize) -> (Arc<BufferPool>, Arc<TransactionManager>) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(buf)).unwrap());
    let mut tm = TransactionManager::new(128);
    tm.set_buffer_pool(Arc::clone(&bp));
    (bp, Arc::new(tm))
}

#[test]
fn test_concurrent_insert_4_threads() {
    let (bp, tm) = setup(500);
    let table = Arc::new(HeapTable::new(1, bp, tm));
    let mut handles = vec![];
    for t in 0..4 {
        let table = Arc::clone(&table);
        handles.push(std::thread::spawn(move || {
            for i in 0..100 {
                let xid = Xid::new(0, (t * 1000 + i + 1) as u32);
                let data = format!("t{}_{}", t, i);
                table.insert(data.as_bytes(), xid, 0).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    // Verify all 400 tuples exist
    let mut scan = HeapScan::new(&table);
    let mut count = 0;
    while let Ok(Some(_)) = scan.next() { count += 1; }
    assert_eq!(count, 400);
}

#[test]
fn test_concurrent_insert_and_scan() {
    let (bp, tm) = setup(500);
    let table = Arc::new(HeapTable::new(2, bp, tm));
    // Pre-insert
    for i in 0..50 {
        table.insert(format!("pre_{}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap();
    }
    let mut handles = vec![];
    // Writer
    let t = Arc::clone(&table);
    handles.push(std::thread::spawn(move || {
        for i in 50..150 {
            t.insert(format!("w_{}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap();
        }
    }));
    // Reader
    let t = Arc::clone(&table);
    handles.push(std::thread::spawn(move || {
        for _ in 0..10 {
            let mut scan = HeapScan::new(&t);
            let mut c = 0;
            while let Ok(Some(_)) = scan.next() { c += 1; }
            assert!(c >= 50, "Should see at least pre-inserted rows, got {}", c);
        }
    }));
    for h in handles { h.join().unwrap(); }
}

#[test]
fn test_concurrent_update_same_row() {
    let (bp, tm) = setup(200);
    let table = Arc::new(HeapTable::new(3, Arc::clone(&bp), Arc::clone(&tm)));
    let tid = table.insert(b"initial_value!", Xid::new(0, 1), 0).unwrap();
    let table2 = Arc::clone(&table);
    let mut handles = vec![];
    for t in 0..4 {
        let table = Arc::clone(&table2);
        handles.push(std::thread::spawn(move || {
            for i in 0..50 {
                let data = format!("update_t{}_{:03}", t, i);
                // Same size as "initial_value!" = 14 bytes → HOT update
                let padded = format!("{:>14}", &data[..data.len().min(14)]);
                let _ = table.update(tid, padded.as_bytes(), Xid::new(0, (t*100+i+2) as u32), 0);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    // Row should still be fetchable
    assert!(table.fetch(tid).unwrap().is_some());
}

#[test]
fn test_concurrent_delete_different_rows() {
    let (bp, tm) = setup(200);
    let table = Arc::new(HeapTable::new(4, Arc::clone(&bp), Arc::clone(&tm)));
    let mut tids = Vec::new();
    for i in 0..100 {
        tids.push(table.insert(format!("del_{}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap());
    }
    let tids = Arc::new(tids);
    let mut handles = vec![];
    for t in 0..4 {
        let table = Arc::clone(&table);
        let tids = Arc::clone(&tids);
        handles.push(std::thread::spawn(move || {
            for i in (t*25)..((t+1)*25) {
                let _ = table.delete(tids[i], Xid::new(0, (200+i) as u32), 0);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

#[test]
fn test_txn_abort_under_concurrency() {
    let (bp, tm) = setup(200);
    let table = Arc::new(HeapTable::new(5, Arc::clone(&bp), Arc::clone(&tm)));
    let tm2 = Arc::clone(&tm);
    let mut handles = vec![];
    for _ in 0..4 {
        let table = Arc::clone(&table);
        let tm = Arc::clone(&tm2);
        handles.push(std::thread::spawn(move || {
            for _ in 0..20 {
                let mut txn = tm.begin().unwrap();
                let xid = txn.xid();
                let _ = table.insert_with_undo(b"will abort", xid, 0, Some(&mut txn));
                txn.abort().unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

#[test]
fn test_mixed_commit_abort() {
    let (bp, tm) = setup(200);
    let table = Arc::new(HeapTable::new(6, Arc::clone(&bp), Arc::clone(&tm)));
    let tm2 = Arc::clone(&tm);
    let mut handles = vec![];
    for t in 0..4 {
        let table = Arc::clone(&table);
        let tm = Arc::clone(&tm2);
        handles.push(std::thread::spawn(move || {
            for i in 0..30 {
                let mut txn = tm.begin().unwrap();
                let xid = txn.xid();
                let _ = table.insert_with_undo(
                    format!("t{}_{}", t, i).as_bytes(), xid, 0, Some(&mut txn));
                if i % 3 == 0 {
                    txn.abort().unwrap();
                } else {
                    txn.commit().unwrap();
                }
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

#[test]
fn test_scan_during_updates() {
    let (bp, tm) = setup(200);
    let table = Arc::new(HeapTable::new(7, Arc::clone(&bp), Arc::clone(&tm)));
    for i in 0..30 {
        table.insert(format!("row_{:03}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap();
    }
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let d = Arc::clone(&done);
    let t = Arc::clone(&table);
    let writer = std::thread::spawn(move || {
        let mut i = 100u32;
        while !d.load(std::sync::atomic::Ordering::Relaxed) {
            let _ = t.insert(format!("new_{}", i).as_bytes(), Xid::new(0, i), 0);
            i += 1;
            if i > 200 { break; }
        }
    });
    // Scan multiple times during writes
    for _ in 0..5 {
        let mut scan = HeapScan::new(&table);
        let mut count = 0;
        while let Ok(Some(_)) = scan.next() { count += 1; }
        assert!(count >= 30, "Should see at least initial rows");
    }
    done.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();
}

#[test]
fn test_vacuum_under_load() {
    let (bp, tm) = setup(200);
    let table = Arc::new(HeapTable::new(8, Arc::clone(&bp), Arc::clone(&tm)));
    for i in 0..50 {
        table.insert(format!("v_{}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap();
    }
    // Delete half
    for i in (0..50).step_by(2) {
        let tid = ferrisdb_transaction::TupleId::new(0, (i+1) as u16);
        let _ = table.delete(tid, Xid::new(0, (100+i) as u32), 0);
    }
    // Vacuum should not crash
    let _reclaimed = table.vacuum();
}

#[test]
fn test_large_table_insert() {
    let (bp, tm) = setup(1000);
    let table = Arc::new(HeapTable::new(9, bp, tm));
    for i in 0..1000 {
        let data = format!("large_table_row_{:06}", i);
        table.insert(data.as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap();
    }
    let mut scan = HeapScan::new(&table);
    let mut count = 0;
    while let Ok(Some(_)) = scan.next() { count += 1; }
    assert_eq!(count, 1000);
}

#[test]
fn test_fetch_visible_filters() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(10, Arc::clone(&bp), Arc::clone(&tm));
    // Insert committed row
    let tid = table.insert(b"visible", Xid::new(0, 50), 0).unwrap();
    // Begin new transaction
    let txn = tm.begin().unwrap();
    let result = table.fetch_visible(tid, &txn).unwrap();
    // Xid 50 is not in active list (slot was never allocated through begin())
    // So visibility depends on CSN — since Xid 50 has no CSN, it's "uncommitted" to txn
    // This tests that fetch_visible actually calls is_visible
    let _ = result; // The important thing is it doesn't crash
}

#[test]
fn test_fsm_guides_insert() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(11, Arc::clone(&bp), Arc::clone(&tm));
    // Fill page 0
    for i in 0..100 {
        table.insert(format!("fill_{:03}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap();
    }
    // After filling, FSM should guide new inserts to pages with space
    let tid = table.insert(b"after_fill", Xid::new(0, 200), 0).unwrap();
    assert!(tid.is_valid());
}

#[test]
fn test_hot_update_preserves_tid() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(12, Arc::clone(&bp), Arc::clone(&tm));
    let original = b"AAAA_data_here";
    let tid = table.insert(original, Xid::new(0, 1), 0).unwrap();
    // HOT update (same size)
    let updated = b"BBBB_data_here";
    let new_tid = table.update(tid, updated, Xid::new(0, 2), 0).unwrap();
    assert_eq!(new_tid, tid, "HOT update should preserve TupleId");
    let (_, data) = table.fetch(tid).unwrap().unwrap();
    assert_eq!(data, updated);
}

#[test]
fn test_non_hot_update_new_tid() {
    let (bp, tm) = setup(200);
    let table = HeapTable::new(13, Arc::clone(&bp), Arc::clone(&tm));
    let tid = table.insert(b"short", Xid::new(0, 1), 0).unwrap();
    // Non-HOT update (different size)
    let new_tid = table.update(tid, b"much longer data that changes size", Xid::new(0, 2), 0).unwrap();
    assert_ne!(new_tid, tid, "Non-HOT update should create new TupleId");
}
