//! 紧凑内容锁（16 字节）
//!
//! 专为嵌入 BufferDesc 设计的轻量级读写锁。
//! 复用 LWLock 的 64 位状态字布局，但去掉等待队列和 padding。
//!
//! 等待策略：短自旋 + parking_lot_core 基于地址的 park/unpark。
//! 排他锁释放时唤醒等待者。park 带 1ms 超时作为安全网。
//!
//! 与 C++ `BufferDesc::contentLwLock` 功能等价，但不要求 128 字节对齐。

use crate::atomic::{AtomicI32, AtomicU64, Ordering};
use crate::lock::state_bits::*;
use crate::waiter::LockMode;

/// 短自旋次数（先尝试自旋，避免 park 开销）
const SPIN_LIMIT: u32 = 40;

/// 紧凑内容锁（16 字节）
#[derive(Debug)]
#[repr(C)]
pub struct ContentLock {
    /// 64 位状态字（与 LWLock 使用相同的位布局）
    state: AtomicU64,
    /// 自适应自旋参数（保留）
    _pad: AtomicI32,
}

const _: () = assert!(std::mem::size_of::<ContentLock>() == 16);

impl ContentLock {
    /// 创建新的内容锁
    #[inline]
    pub const fn new() -> Self {
        Self {
            state: AtomicU64::new(0),
            _pad: AtomicI32::new(0),
        }
    }

    /// 获取当前状态字
    #[inline]
    pub fn state(&self) -> u64 {
        self.state.load(Ordering::Acquire)
    }

    /// park key = state 字段地址
    #[inline]
    fn park_key(&self) -> usize {
        &self.state as *const AtomicU64 as usize
    }

    // ========== 共享锁 ==========

    /// 获取共享锁
    ///
    /// 使用纯 CAS 循环（无 fetch_add + rollback 模式）。
    ///
    /// 之前的 fetch_add(1) + 发现冲突后 fetch_sub(1) 回滚模式存在竞态：
    /// 在 fetch_add 和 fetch_sub 之间，其他线程的 CAS 可能看到一个
    /// 临时的错误 share count，导致状态字损坏（count 下溢）。
    ///
    /// CAS 循环虽然在无竞争时略慢（多一次 load），但完全避免了
    /// 对状态字的非原子修改，确保并发安全。
    pub fn acquire_shared(&self) {
        let mut spins: u32 = 0;

        loop {
            let current = self.state.load(Ordering::Acquire);

            // 检查是否可获取共享锁
            if !has_exclusive(current) && !is_disallow_preempt(current) {
                // 构造新状态: count + 1, 确保 SHARED flag
                let new_count = get_share_count(current) + 1;
                let desired = (current & !SHARE_COUNT_MASK) | SHARED | (new_count as u64);
                match self.state.compare_exchange_weak(
                    current, desired, Ordering::AcqRel, Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(_) => continue, // 被并发修改，重试
                }
            }

            // 有排他锁或 DISALLOW_PREEMPT — 等待
            spins += 1;
            if spins < SPIN_LIMIT {
                std::hint::spin_loop();
            } else {
                let key = self.park_key();
                unsafe {
                    parking_lot_core::park(
                        key,
                        || {
                            let state = self.state.load(Ordering::Acquire);
                            has_exclusive(state) || is_disallow_preempt(state)
                        },
                        || {},
                        |_, _| {},
                        parking_lot_core::DEFAULT_PARK_TOKEN,
                        Some(std::time::Instant::now() + std::time::Duration::from_millis(2)),
                    );
                }
                spins = 0;
            }
        }
    }

    /// 尝试获取共享锁（非阻塞）
    pub fn try_acquire_shared(&self) -> bool {
        let old_state = self.state.load(Ordering::Acquire);
        if has_exclusive(old_state) || is_disallow_preempt(old_state) {
            return false;
        }
        let new_state = if has_shared(old_state) {
            old_state + 1
        } else {
            (old_state & !SHARE_COUNT_MASK) | SHARED | 1
        };
        self.state.compare_exchange(old_state, new_state, Ordering::AcqRel, Ordering::Relaxed).is_ok()
    }

    // ========== 排他锁 ==========

    /// 获取排他锁
    ///
    /// 使用 DISALLOW_PREEMPT 防止写者饥饿：等待时设置该标志，
    /// 阻止新共享锁获取（已持有的共享锁不受影响，会自然释放）。
    /// 参照 C++ dstore LWLockAcquire<LW_EXCLUSIVE> + DISALLOW_PREEMPT 机制。
    pub fn acquire_exclusive(&self) {
        // 快速路径：无竞争直接获取
        let old_state = self.state.load(Ordering::Acquire);
        if can_acquire(old_state) {
            let new_state = old_state | EXCLUSIVE;
            if self.state.compare_exchange(
                old_state, new_state, Ordering::AcqRel, Ordering::Relaxed,
            ).is_ok() {
                return;
            }
        }

        // 慢速路径：设置 DISALLOW_PREEMPT 防止新 reader 进入
        self.acquire_exclusive_slow();
    }

    fn acquire_exclusive_slow(&self) {
        // 设置 DISALLOW_PREEMPT — 新的 acquire_shared CAS 会看到此标志
        // 并在 CAS 条件中失败（不会 fetch_add 污染 count）
        let _ = self.state.fetch_or(DISALLOW_PREEMPT, Ordering::AcqRel);

        let mut spins: u32 = 0;
        loop {
            let old_state = self.state.load(Ordering::Acquire);

            // 可获取条件：无 EXCLUSIVE 且 share count == 0
            // （SHARED flag 可能残留但 count==0 说明没有持有者）
            if !has_exclusive(old_state) && get_share_count(old_state) == 0 {
                // 清除 DISALLOW_PREEMPT + SHARED flag，设置 EXCLUSIVE
                let new_state = (old_state & !DISALLOW_PREEMPT & !SHARED) | EXCLUSIVE;
                if self.state.compare_exchange_weak(
                    old_state, new_state, Ordering::AcqRel, Ordering::Relaxed,
                ).is_ok() {
                    return;
                }
                continue;
            }

            spins += 1;
            if spins < SPIN_LIMIT {
                std::hint::spin_loop();
            } else {
                self.park_wait();
                spins = 0;
            }
        }
    }

    /// 尝试获取排他锁（非阻塞）
    pub fn try_acquire_exclusive(&self) -> bool {
        let old_state = self.state.load(Ordering::Acquire);
        if !can_acquire(old_state) {
            return false;
        }
        let new_state = old_state | EXCLUSIVE;
        self.state.compare_exchange(old_state, new_state, Ordering::AcqRel, Ordering::Relaxed).is_ok()
    }

    // ========== 释放 ==========

    /// 释放共享锁
    ///
    /// CAS 循环确保 count 不会下溢：只有 count > 0 时才减 1。
    pub fn release_shared(&self) {
        loop {
            let old_state = self.state.load(Ordering::Acquire);
            let count = get_share_count(old_state);
            debug_assert!(count > 0, "release_shared with count=0");
            if count == 0 { return; } // 安全兜底

            let new_count = count - 1;
            let mut new_state = (old_state & !SHARE_COUNT_MASK) | (new_count as u64);
            // 最后一个 reader 释放时清除 SHARED flag
            if new_count == 0 {
                new_state &= !SHARED;
            }

            if self.state.compare_exchange_weak(
                old_state, new_state, Ordering::AcqRel, Ordering::Relaxed,
            ).is_ok() {
                if new_count == 0 {
                    self.unpark_one();
                }
                return;
            }
        }
    }

    /// 释放排他锁
    pub fn release_exclusive(&self) {
        let mut old_state = self.state.load(Ordering::Acquire);
        loop {
            debug_assert!(has_exclusive(old_state));
            let new_state = old_state & !EXCLUSIVE;
            match self.state.compare_exchange_weak(
                old_state, new_state, Ordering::AcqRel, Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.unpark_one();
                    return;
                }
                Err(current) => old_state = current,
            }
        }
    }

    // ========== 通用接口 ==========

    #[inline]
    /// 按模式获取锁
    pub fn acquire(&self, mode: LockMode) {
        match mode {
            LockMode::Shared => self.acquire_shared(),
            LockMode::Exclusive => self.acquire_exclusive(),
        }
    }

    #[inline]
    /// 按模式释放锁
    pub fn release(&self, mode: LockMode) {
        match mode {
            LockMode::Shared => self.release_shared(),
            LockMode::Exclusive => self.release_exclusive(),
        }
    }

    /// 检查是否被持有
    #[inline]
    pub fn is_locked(&self) -> bool {
        is_locked(self.state())
    }

    // ========== Park/Unpark ==========

    #[inline]
    fn park_wait(&self) {
        let key = self.park_key();
        unsafe {
            parking_lot_core::park(
                key,
                || {
                    let state = self.state.load(Ordering::Acquire);
                    is_locked(state) || is_disallow_preempt(state)
                },
                || {},
                |_, _| {},
                parking_lot_core::DEFAULT_PARK_TOKEN,
                // 1ms 超时安全网
                Some(std::time::Instant::now() + std::time::Duration::from_millis(1)),
            );
        }
    }

    /// 唤醒一个等待者（足够 — 被唤醒者获取锁后释放时会唤醒下一个）
    #[inline]
    fn unpark_one(&self) {
        let key = self.park_key();
        unsafe {
            parking_lot_core::unpark_one(key, |_| parking_lot_core::DEFAULT_UNPARK_TOKEN);
        }
    }
}

impl Default for ContentLock {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Send for ContentLock {}
unsafe impl Sync for ContentLock {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_content_lock_size() {
        assert_eq!(std::mem::size_of::<ContentLock>(), 16);
    }

    #[test]
    fn test_content_lock_shared_basic() {
        let lock = ContentLock::new();
        lock.acquire_shared();
        assert!(lock.is_locked());
        assert!(!has_exclusive(lock.state()));
        assert!(has_shared(lock.state()));
        assert_eq!(get_share_count(lock.state()), 1);

        lock.acquire_shared();
        assert_eq!(get_share_count(lock.state()), 2);

        lock.release_shared();
        assert_eq!(get_share_count(lock.state()), 1);

        lock.release_shared();
        assert!(!lock.is_locked());
    }

    #[test]
    fn test_content_lock_exclusive_basic() {
        let lock = ContentLock::new();
        lock.acquire_exclusive();
        assert!(lock.is_locked());
        assert!(has_exclusive(lock.state()));
        lock.release_exclusive();
        assert!(!lock.is_locked());
    }

    #[test]
    fn test_content_lock_concurrent_shared() {
        let lock = Arc::new(ContentLock::new());
        let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let mut handles = vec![];

        for _ in 0..10 {
            let lock = Arc::clone(&lock);
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    lock.acquire_shared();
                    let _ = counter.load(Ordering::Relaxed);
                    lock.release_shared();
                }
            }));
        }

        for h in handles { h.join().unwrap(); }
        assert_eq!(get_share_count(lock.state()), 0);
    }

    #[test]
    fn test_content_lock_concurrent_exclusive() {
        let lock = Arc::new(ContentLock::new());
        let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let mut handles = vec![];

        for _ in 0..10 {
            let lock = Arc::clone(&lock);
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    lock.acquire_exclusive();
                    counter.fetch_add(1, Ordering::Relaxed);
                    lock.release_exclusive();
                }
            }));
        }

        for h in handles { h.join().unwrap(); }
        assert_eq!(counter.load(Ordering::Relaxed), 1000);
    }

    #[test]
    fn test_content_lock_high_contention_no_livelock() {
        let lock = Arc::new(ContentLock::new());
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = vec![];

        for _ in 0..16 {
            let lock = Arc::clone(&lock);
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..200 {
                    lock.acquire_exclusive();
                    counter.fetch_add(1, Ordering::Relaxed);
                    lock.release_exclusive();
                }
            }));
        }

        for h in handles { h.join().unwrap(); }
        assert_eq!(counter.load(Ordering::Relaxed), 3200);
    }

    #[test]
    fn test_content_lock_mixed_rw_high_contention() {
        let lock = Arc::new(ContentLock::new());
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = vec![];

        for _ in 0..8 {
            let lock = Arc::clone(&lock);
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..200 {
                    lock.acquire_exclusive();
                    counter.fetch_add(1, Ordering::Relaxed);
                    lock.release_exclusive();
                }
            }));
        }

        for _ in 0..8 {
            let lock = Arc::clone(&lock);
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..200 {
                    lock.acquire_shared();
                    let _ = counter.load(Ordering::Relaxed);
                    lock.release_shared();
                }
            }));
        }

        for h in handles { h.join().unwrap(); }
        assert_eq!(counter.load(Ordering::Relaxed), 1600);
    }
}
