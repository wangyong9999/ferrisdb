//! Round 13: LWLock deep paths + config GUC + LockGuard + state_bits

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use ferrisdb_core::lock::{LWLock, LockGuard, ContentLock, LockMode};

// ============================================================
// LWLock: shared + exclusive interleave
// ============================================================

#[test]
fn test_lwlock_shared_then_exclusive() {
    let lock = LWLock::new();
    lock.acquire(LockMode::Shared);
    lock.release(LockMode::Shared);
    lock.acquire(LockMode::Exclusive);
    lock.release(LockMode::Exclusive);
}

#[test]
fn test_lwlock_multiple_shared() {
    let lock = LWLock::new();
    lock.acquire(LockMode::Shared);
    lock.acquire(LockMode::Shared);
    lock.acquire(LockMode::Shared);
    lock.release(LockMode::Shared);
    lock.release(LockMode::Shared);
    lock.release(LockMode::Shared);
}

#[test]
fn test_lwlock_try_shared_while_exclusive() {
    let lock = LWLock::new();
    lock.acquire(LockMode::Exclusive);
    assert!(!lock.try_acquire(LockMode::Shared));
    lock.release(LockMode::Exclusive);
    assert!(lock.try_acquire(LockMode::Shared));
    lock.release(LockMode::Shared);
}

#[test]
fn test_lwlock_try_exclusive_while_shared() {
    let lock = LWLock::new();
    lock.acquire(LockMode::Shared);
    assert!(!lock.try_acquire(LockMode::Exclusive));
    lock.release(LockMode::Shared);
    assert!(lock.try_acquire(LockMode::Exclusive));
    lock.release(LockMode::Exclusive);
}

#[test]
fn test_lwlock_is_locked() {
    let lock = LWLock::new();
    assert!(!lock.is_locked());
    lock.acquire(LockMode::Shared);
    assert!(lock.is_locked());
    lock.release(LockMode::Shared);
    assert!(!lock.is_locked());
}

#[test]
fn test_lwlock_has_waiters() {
    let lock = LWLock::new();
    assert!(!lock.has_waiters());
}

#[test]
fn test_lwlock_state() {
    let lock = LWLock::new();
    let s = lock.state();
    assert_eq!(s, 0);
}

// ============================================================
// LWLock: LockGuard RAII
// ============================================================

#[test]
fn test_lwlock_guard_shared() {
    let lock = LWLock::new();
    {
        let _guard = LockGuard::new(&lock, LockMode::Shared);
        assert!(lock.is_locked());
    }
    assert!(!lock.is_locked());
}

#[test]
fn test_lwlock_guard_exclusive() {
    let lock = LWLock::new();
    {
        let _guard = LockGuard::new(&lock, LockMode::Exclusive);
        assert!(lock.is_locked());
    }
    assert!(!lock.is_locked());
}

#[test]
fn test_lwlock_guard_try() {
    let lock = LWLock::new();
    let guard = LockGuard::try_new(&lock, LockMode::Exclusive);
    assert!(guard.is_some());
    let guard2 = LockGuard::try_new(&lock, LockMode::Shared);
    assert!(guard2.is_none());
    drop(guard);
}

// ============================================================
// LWLock: concurrent R/W with verification
// ============================================================

#[test]
fn test_lwlock_concurrent_rw_correctness() {
    let lock = Arc::new(LWLock::new());
    let data = Arc::new(AtomicU64::new(0));
    let mut handles = vec![];

    // 2 writers
    for _ in 0..2 {
        let lock = Arc::clone(&lock);
        let data = Arc::clone(&data);
        handles.push(std::thread::spawn(move || {
            for _ in 0..50 {
                lock.acquire(LockMode::Exclusive);
                data.fetch_add(1, Ordering::Relaxed);
                lock.release(LockMode::Exclusive);
            }
        }));
    }
    // 2 readers
    for _ in 0..2 {
        let lock = Arc::clone(&lock);
        let data = Arc::clone(&data);
        handles.push(std::thread::spawn(move || {
            for _ in 0..50 {
                lock.acquire(LockMode::Shared);
                let _ = data.load(Ordering::Relaxed);
                lock.release(LockMode::Shared);
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
    assert_eq!(data.load(Ordering::Relaxed), 100);
}

// ============================================================
// ContentLock: additional patterns
// ============================================================

#[test]
fn test_content_lock_release_mode() {
    let lock = ContentLock::new();
    lock.acquire(LockMode::Shared);
    lock.release(LockMode::Shared);
    lock.acquire(LockMode::Exclusive);
    lock.release(LockMode::Exclusive);
}

#[test]
fn test_content_lock_is_locked() {
    let lock = ContentLock::new();
    assert!(!lock.is_locked());
    lock.acquire_exclusive();
    assert!(lock.is_locked());
    lock.release_exclusive();
    assert!(!lock.is_locked());
}

#[test]
fn test_content_lock_state() {
    let lock = ContentLock::new();
    assert_eq!(lock.state(), 0);
}

// ============================================================
// Config GUC: all fields + modify
// ============================================================

#[test]
fn test_config_modify_and_read() {
    use ferrisdb_core::config::config;

    let cfg = config();
    // Store + load roundtrip for each field
    cfg.shared_buffers.store(512, Ordering::Relaxed);
    assert_eq!(cfg.shared_buffers.load(Ordering::Relaxed), 512);

    cfg.wal_buffers.store(64, Ordering::Relaxed);
    assert_eq!(cfg.wal_buffers.load(Ordering::Relaxed), 64);

    cfg.max_connections.store(100, Ordering::Relaxed);
    assert_eq!(cfg.max_connections.load(Ordering::Relaxed), 100);

    cfg.bgwriter_delay_ms.store(200, Ordering::Relaxed);
    cfg.bgwriter_max_pages.store(50, Ordering::Relaxed);
    cfg.wal_flusher_delay_ms.store(5, Ordering::Relaxed);
    cfg.deadlock_timeout_ms.store(1000, Ordering::Relaxed);
    cfg.log_level.store(2, Ordering::Relaxed);
    cfg.synchronous_commit.store(true, Ordering::Relaxed);
    cfg.checkpoint_interval_ms.store(30000, Ordering::Relaxed);
    cfg.transaction_timeout_ms.store(5000, Ordering::Relaxed);
}

// ============================================================
// State bits: additional coverage
// ============================================================

#[test]
fn test_state_bits_combinations() {
    use ferrisdb_core::lock::*;

    // Shared + count
    let state = SHARED | 5;
    assert!(has_shared(state));
    assert_eq!(get_share_count(state), 5);
    assert!(!has_exclusive(state));
    assert!(is_locked(state));
    assert!(!can_acquire(state));
    assert!(can_acquire_shared(state));

    // Exclusive only
    let state = EXCLUSIVE;
    assert!(!has_shared(state));
    assert!(has_exclusive(state));
    assert!(is_locked(state));
    assert!(!can_acquire(state));

    // DISALLOW_PREEMPT blocks shared acquire
    let state = DISALLOW_PREEMPT;
    assert!(is_disallow_preempt(state));
    assert!(!can_acquire(state));
    assert!(!can_acquire_shared(state));

    // HAS_WAITERS + RELEASE_OK
    let state = HAS_WAITERS | RELEASE_OK;
    assert!(has_waiters(state));
    assert!(is_release_ok(state));

    // Clean state
    assert!(can_acquire(0));
    assert!(!is_locked(0));
    assert!(!is_wait_list_locked(0));
}

// ============================================================
// LWLock: with_group
// ============================================================

#[test]
fn test_lwlock_with_group() {
    let lock = LWLock::with_group(42);
    lock.acquire(LockMode::Exclusive);
    lock.release(LockMode::Exclusive);
}
