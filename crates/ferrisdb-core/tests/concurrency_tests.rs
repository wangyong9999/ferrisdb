//! Concurrency and lock stress tests

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use ferrisdb_core::lock::*;

// ==================== ContentLock Stress ====================

#[test]
fn test_content_lock_exclusive_mutex_property() {
    let lock = Arc::new(ContentLock::new());
    let counter = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for _ in 0..8 {
        let lock = Arc::clone(&lock);
        let counter = Arc::clone(&counter);
        handles.push(std::thread::spawn(move || {
            for _ in 0..500 {
                lock.acquire_exclusive();
                // Critical section: increment should be atomic
                let prev = counter.load(Ordering::Relaxed);
                counter.store(prev + 1, Ordering::Relaxed);
                lock.release_exclusive();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(counter.load(Ordering::Relaxed), 4000);
}

#[test]
fn test_content_lock_shared_allows_concurrent_reads() {
    let lock = Arc::new(ContentLock::new());
    let read_count = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for _ in 0..8 {
        let lock = Arc::clone(&lock);
        let read_count = Arc::clone(&read_count);
        handles.push(std::thread::spawn(move || {
            for _ in 0..500 {
                lock.acquire_shared();
                read_count.fetch_add(1, Ordering::Relaxed);
                lock.release_shared();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(read_count.load(Ordering::Relaxed), 4000);
    assert_eq!(get_share_count(lock.state()), 0);
}

#[test]
fn test_content_lock_rw_exclusion() {
    let lock = Arc::new(ContentLock::new());
    let data = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // Writers
    for _ in 0..4 {
        let lock = Arc::clone(&lock);
        let data = Arc::clone(&data);
        handles.push(std::thread::spawn(move || {
            for _ in 0..300 {
                lock.acquire_exclusive();
                data.fetch_add(1, Ordering::Relaxed);
                lock.release_exclusive();
            }
        }));
    }
    // Readers
    for _ in 0..4 {
        let lock = Arc::clone(&lock);
        let data = Arc::clone(&data);
        handles.push(std::thread::spawn(move || {
            for _ in 0..300 {
                lock.acquire_shared();
                let _ = data.load(Ordering::Relaxed);
                lock.release_shared();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(data.load(Ordering::Relaxed), 1200);
}

#[test]
fn test_content_lock_try_acquire() {
    let lock = ContentLock::new();

    // Try shared on free lock
    assert!(lock.try_acquire_shared());
    lock.release_shared();

    // Try exclusive on free lock
    assert!(lock.try_acquire_exclusive());
    // Try shared while exclusive held
    assert!(!lock.try_acquire_shared());
    // Try exclusive while exclusive held
    assert!(!lock.try_acquire_exclusive());
    lock.release_exclusive();
}

#[test]
fn test_content_lock_16_threads_exclusive() {
    let lock = Arc::new(ContentLock::new());
    let counter = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for _ in 0..16 {
        let lock = Arc::clone(&lock);
        let counter = Arc::clone(&counter);
        handles.push(std::thread::spawn(move || {
            for _ in 0..100 {
                lock.acquire_exclusive();
                counter.fetch_add(1, Ordering::Relaxed);
                lock.release_exclusive();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(counter.load(Ordering::Relaxed), 1600);
}

// ==================== LWLock ====================

// ==================== LWLock ====================

#[test]
fn test_lwlock_exclusive() {
    let lock = LWLock::new();
    lock.acquire(LockMode::Exclusive);
    lock.release(LockMode::Exclusive);
}

#[test]
fn test_lwlock_shared_multiple() {
    let lock = LWLock::new();
    lock.acquire(LockMode::Shared);
    lock.acquire(LockMode::Shared);
    lock.release(LockMode::Shared);
    lock.release(LockMode::Shared);
}

#[test]
fn test_lwlock_try() {
    let lock = LWLock::new();
    assert!(lock.try_acquire(LockMode::Exclusive));
    assert!(!lock.try_acquire(LockMode::Shared));
    lock.release(LockMode::Exclusive);
}

#[test]
fn test_lwlock_concurrent_exclusive() {
    let lock = Arc::new(LWLock::new());
    let counter = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    for _ in 0..4 {
        let lock = Arc::clone(&lock);
        let counter = Arc::clone(&counter);
        handles.push(std::thread::spawn(move || {
            for _ in 0..200 {
                lock.acquire(LockMode::Exclusive);
                counter.fetch_add(1, Ordering::Relaxed);
                lock.release(LockMode::Exclusive);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(counter.load(Ordering::Relaxed), 800);
}

// ==================== SpinLock ====================

#[test]
fn test_spinlock_basic() {
    let lock = SpinLock::new(0u64);
    {
        let guard = lock.lock();
        assert_eq!(*guard, 0);
    } // guard dropped = unlock
}

#[test]
fn test_spinlock_try() {
    let lock = SpinLock::new(0u64);
    let guard = lock.try_lock();
    assert!(guard.is_some());
    let guard2 = lock.try_lock();
    assert!(guard2.is_none());
    drop(guard);
}

#[test]
fn test_spinlock_concurrent() {
    let lock = Arc::new(SpinLock::new(0u64));
    let mut handles = vec![];

    for _ in 0..4 {
        let lock = Arc::clone(&lock);
        handles.push(std::thread::spawn(move || {
            for _ in 0..500 {
                let mut guard = lock.lock();
                *guard += 1;
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(*lock.lock(), 2000);
}

// ==================== Atomic Types ====================

#[test]
fn test_atomic_cas() {
    use ferrisdb_core::atomic::AtomicU64;
    let v = AtomicU64::new(0);
    assert_eq!(v.compare_exchange(0, 42, Ordering::AcqRel, Ordering::Relaxed), Ok(0));
    assert_eq!(v.load(Ordering::Acquire), 42);
    assert!(v.compare_exchange(0, 99, Ordering::AcqRel, Ordering::Relaxed).is_err());
}

#[test]
fn test_atomic_fetch_add() {
    use ferrisdb_core::atomic::AtomicU64;
    let v = AtomicU64::new(10);
    assert_eq!(v.fetch_add(5, Ordering::AcqRel), 10);
    assert_eq!(v.load(Ordering::Acquire), 15);
}

// ==================== State Bits ====================

#[test]
fn test_state_bits_share_count() {
    let state = SHARED | 5;
    assert!(has_shared(state));
    assert_eq!(get_share_count(state), 5);
    assert!(!has_exclusive(state));
}

#[test]
fn test_state_bits_exclusive() {
    let state = EXCLUSIVE;
    assert!(has_exclusive(state));
    assert!(!has_shared(state));
    assert!(is_locked(state));
}

#[test]
fn test_state_bits_can_acquire() {
    assert!(can_acquire(0));
    assert!(!can_acquire(EXCLUSIVE));
    assert!(!can_acquire(SHARED | 1));
    assert!(!can_acquire(DISALLOW_PREEMPT));
}

// ==================== Xid/Csn/Lsn Types ====================

#[test]
fn test_xid_validity() {
    assert!(!ferrisdb_core::Xid::INVALID.is_valid());
    assert!(ferrisdb_core::Xid::new(0, 1).is_valid());
}

#[test]
fn test_csn_monotonic() {
    let alloc = ferrisdb_core::CsnAllocator::new();
    let c1 = alloc.allocate();
    let c2 = alloc.allocate();
    assert!(c2.raw() > c1.raw());
}

#[test]
fn test_lsn_parts() {
    let lsn = ferrisdb_core::Lsn::from_parts(3, 1024);
    let (file, offset) = lsn.parts();
    assert_eq!(file, 3);
    assert_eq!(offset, 1024);
}

#[test]
fn test_snapshot_active() {
    let snap = ferrisdb_core::Snapshot::new(
        ferrisdb_core::CsnAllocator::new().allocate(),
        vec![ferrisdb_core::Xid::new(0, 5), ferrisdb_core::Xid::new(0, 10)],
    );
    assert!(snap.is_active(ferrisdb_core::Xid::new(0, 5)));
    assert!(!snap.is_active(ferrisdb_core::Xid::new(0, 7)));
}

// ==================== Buffer Tag / PageId ====================

#[test]
fn test_buffer_tag_equality() {
    use ferrisdb_core::{BufferTag, PdbId};
    let t1 = BufferTag::new(PdbId::new(0), 100, 5);
    let t2 = BufferTag::new(PdbId::new(0), 100, 5);
    let t3 = BufferTag::new(PdbId::new(0), 100, 6);
    assert_eq!(t1, t2);
    assert_ne!(t1, t3);
}

#[test]
fn test_page_id_default() {
    let p = ferrisdb_core::PageId::INVALID;
    assert_eq!(p.block, 0);
}
