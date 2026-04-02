//! Buffer Pool comprehensive tests

use std::sync::Arc;
use ferrisdb_core::{BufferTag, PdbId};
use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager};
use ferrisdb_storage::page::PAGE_SIZE;
use tempfile::TempDir;

fn make_bp(n: usize) -> Arc<BufferPool> {
    Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap())
}

fn make_bp_with_smgr(n: usize) -> (Arc<BufferPool>, TempDir) {
    let td = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let mut bp = BufferPool::new(BufferPoolConfig::new(n)).unwrap();
    bp.set_smgr(smgr);
    (Arc::new(bp), td)
}

fn tag(rel: u16, block: u32) -> BufferTag {
    BufferTag::new(PdbId::new(0), rel, block)
}

// ==================== Pin/Unpin ====================

#[test]
fn test_pin_new_page() {
    let bp = make_bp(100);
    let pinned = bp.pin(&tag(1, 0)).unwrap();
    assert!(pinned.buf_id() >= 0);
}

#[test]
fn test_pin_same_page_twice() {
    let bp = make_bp(100);
    let p1 = bp.pin(&tag(1, 0)).unwrap();
    let id1 = p1.buf_id();
    drop(p1);
    let p2 = bp.pin(&tag(1, 0)).unwrap();
    assert_eq!(p2.buf_id(), id1, "Same tag should return same buffer");
}

#[test]
fn test_pin_different_pages() {
    let bp = make_bp(100);
    let p1 = bp.pin(&tag(1, 0)).unwrap();
    let p2 = bp.pin(&tag(1, 1)).unwrap();
    assert_ne!(p1.buf_id(), p2.buf_id());
}

#[test]
fn test_unpin_on_drop() {
    let bp = make_bp(10);
    {
        let _p = bp.pin(&tag(1, 0)).unwrap();
        // Buffer is pinned
    }
    // Buffer should be unpinned after drop
    // We can reuse it by pinning another page
    let _p2 = bp.pin(&tag(2, 0)).unwrap();
}

// ==================== Page Data ====================

#[test]
fn test_page_data_readable() {
    let bp = make_bp(100);
    let pinned = bp.pin(&tag(1, 0)).unwrap();
    let data = pinned.page_slice();
    assert_eq!(data.len(), PAGE_SIZE);
}

#[test]
fn test_page_write_and_read() {
    let bp = make_bp(100);
    let pinned = bp.pin(&tag(1, 0)).unwrap();
    let _lock = pinned.lock_exclusive();
    let ptr = pinned.page_data();
    unsafe { *ptr = 42; }
    drop(_lock);

    let _lock2 = pinned.lock_shared();
    assert_eq!(pinned.page_slice()[0], 42);
}

// ==================== Dirty Page ====================

#[test]
fn test_mark_dirty() {
    let bp = make_bp(100);
    let pinned = bp.pin(&tag(1, 0)).unwrap();
    pinned.mark_dirty();
    assert!(bp.dirty_page_count() > 0);
}

// ==================== Eviction ====================

#[test]
fn test_eviction_when_pool_full() {
    let bp = make_bp(5); // Very small pool
    // Pin and unpin 10 different pages
    for i in 0..10u32 {
        let p = bp.pin(&tag(1, i)).unwrap();
        drop(p); // Unpin so it can be evicted
    }
    // Should not crash — eviction should work
}

#[test]
fn test_pool_full_all_pinned() {
    let bp = make_bp(3);
    let _p0 = bp.pin(&tag(1, 0)).unwrap();
    let _p1 = bp.pin(&tag(1, 1)).unwrap();
    let _p2 = bp.pin(&tag(1, 2)).unwrap();
    // All 3 buffers pinned, 4th pin should fail
    let result = bp.pin(&tag(1, 3));
    assert!(result.is_err(), "Should fail when all buffers pinned");
}

// ==================== Flush ====================

#[test]
fn test_flush_all() {
    let (bp, _td) = make_bp_with_smgr(100);
    let pinned = bp.pin(&tag(1, 0)).unwrap();
    let _lock = pinned.lock_exclusive();
    // Write some data
    let page = unsafe {
        ferrisdb_storage::HeapPage::from_bytes(
            std::slice::from_raw_parts_mut(pinned.page_data(), PAGE_SIZE)
        )
    };
    page.init();
    drop(_lock);
    pinned.mark_dirty();
    drop(pinned);

    bp.flush_all().unwrap();
    assert_eq!(bp.dirty_page_count(), 0);
}

#[test]
fn test_flush_some() {
    let (bp, _td) = make_bp_with_smgr(100);
    for i in 0..10u32 {
        let p = bp.pin(&tag(1, i)).unwrap();
        p.mark_dirty();
    }
    let flushed = bp.flush_some(5);
    assert!(flushed <= 5);
}

// ==================== Stats ====================

#[test]
fn test_hit_rate() {
    let bp = make_bp(100);
    bp.reset_stats();
    let _ = bp.pin(&tag(1, 0)).unwrap(); // Miss
    let _ = bp.pin(&tag(1, 0)).unwrap(); // Hit
    let _ = bp.pin(&tag(1, 0)).unwrap(); // Hit
    assert!(bp.hit_rate() > 0.5);
}

#[test]
fn test_stats_reset() {
    let bp = make_bp(100);
    let _ = bp.pin(&tag(1, 0)).unwrap();
    bp.reset_stats();
    assert_eq!(bp.stat_hits(), 0);
    assert_eq!(bp.stat_misses(), 0);
}

// ==================== Concurrent ====================

#[test]
fn test_concurrent_pin() {
    let bp = make_bp(200);
    let bp = Arc::new(bp);
    let mut handles = vec![];

    for t in 0..4 {
        let bp = bp.clone();
        handles.push(std::thread::spawn(move || {
            for i in 0..50u32 {
                let tag = BufferTag::new(PdbId::new(0), (t + 1) as u16, i);
                let p = bp.pin(&tag).unwrap();
                let _ = p.page_slice();
                drop(p);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

#[test]
fn test_concurrent_read_write() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(100)).unwrap());
    let t = tag(1, 0);
    // Pre-create the page
    { let _ = bp.pin(&t).unwrap(); }

    let mut handles = vec![];
    // Writers
    for _ in 0..2 {
        let bp = bp.clone();
        let t = t;
        handles.push(std::thread::spawn(move || {
            for _ in 0..100 {
                let p = bp.pin(&t).unwrap();
                let _lock = p.lock_exclusive();
                let ptr = p.page_data();
                unsafe { *ptr.add(100) = 1; }
                drop(_lock);
                p.mark_dirty();
            }
        }));
    }
    // Readers
    for _ in 0..2 {
        let bp = bp.clone();
        let t = t;
        handles.push(std::thread::spawn(move || {
            for _ in 0..100 {
                let p = bp.pin(&t).unwrap();
                let _lock = p.lock_shared();
                let _ = p.page_slice()[100];
                drop(_lock);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

// ==================== Persistence ====================

#[test]
fn test_data_survives_flush_restart() {
    let td = TempDir::new().unwrap();

    // Write
    {
        let smgr = Arc::new(StorageManager::new(td.path()));
        smgr.init().unwrap();
        let mut bp = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
        bp.set_smgr(smgr);
        let bp = Arc::new(bp);

        let p = bp.pin(&tag(50, 0)).unwrap();
        let _lock = p.lock_exclusive();
        let page = unsafe {
            ferrisdb_storage::HeapPage::from_bytes(
                std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)
            )
        };
        page.init();
        page.insert_tuple(b"persistent test data 1234567890abcdef").unwrap();
        drop(_lock);
        p.mark_dirty();
        drop(p);
        bp.flush_all().unwrap();
    }

    // Read
    {
        let smgr = Arc::new(StorageManager::new(td.path()));
        smgr.init().unwrap();
        let mut bp = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
        bp.set_smgr(smgr);
        let bp = Arc::new(bp);

        let p = bp.pin(&tag(50, 0)).unwrap();
        let _lock = p.lock_shared();
        let page = unsafe {
            ferrisdb_storage::HeapPage::from_bytes(
                std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)
            )
        };
        let data = page.get_tuple(1);
        assert!(data.is_some(), "Data should survive flush+restart");
    }
}
