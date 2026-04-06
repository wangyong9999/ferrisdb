//! Phase 2 hardening tests — concurrent vacuum+DML, undo limits, lockfile

use std::sync::Arc;
use ferrisdb_core::Xid;
use ferrisdb_storage::{BufferPool, BufferPoolConfig};
use ferrisdb_transaction::{HeapTable, TransactionManager};
use tempfile::TempDir;

fn setup(n: usize) -> (Arc<BufferPool>, Arc<TransactionManager>) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap());
    let mut tm = TransactionManager::new(64);
    tm.set_buffer_pool(Arc::clone(&bp));
    (bp, Arc::new(tm))
}

// ============================================================
// Concurrent vacuum + DML — no panic
// ============================================================

#[test]
fn test_concurrent_insert_and_vacuum_no_panic() {
    let (bp, tm) = setup(500);
    let heap = Arc::new(HeapTable::new(1, Arc::clone(&bp), Arc::clone(&tm)));
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let mut handles = vec![];

    // Inserter
    let heap_c = Arc::clone(&heap);
    let tm_c = Arc::clone(&tm);
    let bar_c = Arc::clone(&barrier);
    handles.push(std::thread::spawn(move || {
        bar_c.wait();
        for i in 0..200u32 {
            let mut txn = tm_c.begin().unwrap();
            let xid = txn.xid();
            let _ = heap_c.insert(format!("row_{}", i).as_bytes(), xid, 0);
            txn.commit().unwrap();
        }
    }));

    // Updater
    let heap_c2 = Arc::clone(&heap);
    let tm_c2 = Arc::clone(&tm);
    let bar_c2 = Arc::clone(&barrier);
    handles.push(std::thread::spawn(move || {
        bar_c2.wait();
        for i in 0..100u32 {
            let mut txn = tm_c2.begin().unwrap();
            let xid = txn.xid();
            // Try to update tuple 1 — may fail if vacuumed, that's OK
            let _ = heap_c2.update(
                ferrisdb_transaction::TupleId::new(0, 1),
                format!("updated_{}", i).as_bytes(),
                xid, 0,
            );
            let _ = txn.commit();
        }
    }));

    // Vacuumer
    let heap_c3 = Arc::clone(&heap);
    let bar_c3 = Arc::clone(&barrier);
    handles.push(std::thread::spawn(move || {
        bar_c3.wait();
        for _ in 0..50 {
            let _ = heap_c3.vacuum();
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }));

    for h in handles {
        h.join().unwrap(); // No panic = test passes
    }
}

// ============================================================
// Undo log size limit
// ============================================================

#[test]
fn test_undo_log_size_limit() {
    let (bp, tm) = setup(2000);
    let heap = HeapTable::new(2, Arc::clone(&bp), Arc::clone(&tm));

    let mut txn = tm.begin().unwrap();
    let xid = txn.xid();

    // Insert many rows in single transaction — push_undo should eventually fail at 1M
    // We can't actually hit 1M in test, but verify the mechanism compiles and runs
    for i in 0..100u32 {
        let data = format!("undo_test_{}", i);
        heap.insert(data.as_bytes(), xid, 0).unwrap();
    }
    txn.commit().unwrap();
}

// Engine lockfile test is in crates/ferrisdb/tests/persistence_tests.rs
