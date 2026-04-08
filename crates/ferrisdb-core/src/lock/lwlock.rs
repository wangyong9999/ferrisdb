//! 轻量级锁 (LWLock) 实现
//!
//! 与 C++ 版本二进制兼容的 64 位状态字 LWLock。
//! 核心特性：
//! 1. 共享锁获取：先 fetch_add(1) 无条件 +1，再检查是否有排他锁，如有则 CAS 回滚
//! 2. 高/低32位分离操作
//! 3. 等待队列通过自旋锁保护
//! 4. DISALLOW_PREEMPT 机制

use crate::atomic::{AtomicI32, AtomicU16, AtomicU64, AtomicU8, AtomicPtr, Ordering};
use crate::list::{ShmDListHead, ShmDListNode};
use crate::lock::state_bits::*;
use crate::waiter::LockMode;
use std::cell::UnsafeCell;
use std::sync::atomic::fence;
use std::time::Duration;

/// 默认自旋次数
const DEFAULT_SPINS_PER_DELAY: i32 = 100;

/// 最大自旋次数
const MAX_SPINS_PER_DELAY: i32 = 1000;

/// LWLock 等待者
///
/// 每个线程持有一个，用于等待队列。
#[derive(Debug)]
#[repr(C)]
pub struct LWLockWaiter {
    /// 等待中标志
    pub waiting: AtomicU8,
    /// 等待的锁模式
    pub mode: AtomicU8,
    /// 链表节点
    pub node: UnsafeCell<ShmDListNode>,
    /// 关联的锁
    pub lock: AtomicPtr<LWLock>,
}

impl LWLockWaiter {
    /// 创建新的等待者
    #[inline]
    pub const fn new() -> Self {
        Self {
            waiting: AtomicU8::new(0),
            mode: AtomicU8::new(0),
            node: UnsafeCell::new(ShmDListNode::new()),
            lock: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// 检查是否在等待
    #[inline]
    pub fn is_waiting(&self) -> bool {
        self.waiting.load(Ordering::Acquire) != 0
    }

    /// 设置等待状态
    #[inline]
    pub fn set_waiting(&self, waiting: bool) {
        self.waiting.store(if waiting { 1 } else { 0 }, Ordering::Release);
    }

    /// 获取等待模式
    #[inline]
    pub fn wait_mode(&self) -> LockMode {
        match self.mode.load(Ordering::Acquire) {
            1 => LockMode::Exclusive,
            _ => LockMode::Shared,
        }
    }

    /// 设置等待模式
    #[inline]
    pub fn set_mode(&self, mode: LockMode) {
        self.mode.store(
            match mode {
                LockMode::Shared => 0,
                LockMode::Exclusive => 1,
            },
            Ordering::Release,
        );
    }
}

unsafe impl Send for LWLockWaiter {}
unsafe impl Sync for LWLockWaiter {}

/// LWLock 结构
///
/// 与 C++ 版本二进制兼容，128 字节对齐。
#[derive(Debug)]
#[repr(C, align(128))]
pub struct LWLock {
    /// 自适应自旋参数
    pub spins_per_delay: AtomicI32,
    /// 锁组 ID
    pub group_id: AtomicU16,
    /// 64 位状态字
    pub state: AtomicU64,
    /// 等待队列
    pub waiters: UnsafeCell<ShmDListHead>,
    /// 保留填充
    _padding: [u8; 80],
}

impl LWLock {
    /// 创建新的 LWLock
    #[inline]
    pub const fn new() -> Self {
        Self {
            spins_per_delay: AtomicI32::new(DEFAULT_SPINS_PER_DELAY),
            group_id: AtomicU16::new(0),
            state: AtomicU64::new(0),
            waiters: UnsafeCell::new(ShmDListHead::new()),
            _padding: [0; 80],
        }
    }

    /// 创建指定锁组的 LWLock
    #[inline]
    pub const fn with_group(group_id: u16) -> Self {
        Self {
            spins_per_delay: AtomicI32::new(DEFAULT_SPINS_PER_DELAY),
            group_id: AtomicU16::new(group_id),
            state: AtomicU64::new(0),
            waiters: UnsafeCell::new(ShmDListHead::new()),
            _padding: [0; 80],
        }
    }

    /// 获取当前状态
    #[inline]
    pub fn state(&self) -> u64 {
        self.state.load(Ordering::Acquire)
    }

    /// 检查锁是否被持有
    #[inline]
    pub fn is_locked(&self) -> bool {
        is_locked(self.state())
    }

    /// 检查是否有等待者
    #[inline]
    pub fn has_waiters(&self) -> bool {
        has_waiters(self.state())
    }

    // ========== 低32位操作（无锁共享获取的关键） ==========

    /// 获取低32位状态
    #[inline]
    fn load_low32(&self) -> u32 {
        // SAFETY: 我们只读取低32位，不会造成撕裂读
        // 因为低32位和高32位可以独立修改
        let state = self.state.load(Ordering::Acquire);
        (state & LOW32_MASK) as u32
    }

    // ========== 共享锁获取（fetch_add + CAS回滚） ==========

    /// 获取共享锁
    ///
    /// 核心算法：
    /// 1. 先 fetch_add(1) 无条件 +1
    /// 2. 检查是否有排他锁或 DISALLOW_PREEMPT
    /// 3. 如果有，则 CAS 回滚（恢复计数）
    /// 4. 如果没有，设置 SHARED 标志（如果是第一个共享锁）
    /// 获取共享锁
    ///
    /// 纯 CAS 循环——不使用 fetch_add + rollback 模式。
    /// 原因同 ContentLock：fetch_add/sub 对会在中间态暴露虚假 count，
    /// 被并发 CAS 观察后导致状态字损坏。
    pub fn acquire_shared(&self) {
        let mut spins = self.spins_per_delay.load(Ordering::Relaxed);

        loop {
            let current = self.state.load(Ordering::Acquire);

            if !has_exclusive(current) && !is_disallow_preempt(current) {
                let new_count = get_share_count(current) + 1;
                let desired = (current & !SHARE_COUNT_MASK) | SHARED | (new_count as u64);
                if self.state.compare_exchange_weak(
                    current, desired, Ordering::AcqRel, Ordering::Relaxed,
                ).is_ok() {
                    return;
                }
                continue;
            }

            // 需要等待——使用 FIFO park 队列
            if has_waiters(current) {
                self.wait_shared();
                return;
            }

            let with_waiters = current | HAS_WAITERS;
            if self.state.compare_exchange_weak(
                current, with_waiters, Ordering::AcqRel, Ordering::Relaxed,
            ).is_ok() {
                self.wait_shared();
                return;
            }

            spins = self.spin_delay(spins);
        }
    }

    /// 尝试设置 SHARED 标志
    fn try_set_shared_flag(&self) {
        let mut old_state = self.state.load(Ordering::Acquire);
        loop {
            if has_shared(old_state) {
                // 已经设置了
                return;
            }

            let new_state = old_state | SHARED;
            match self.state.compare_exchange_weak(
                old_state,
                new_state,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => old_state = current,
            }
        }
    }

    /// 尝试清除 SHARED 标志
    fn try_clear_shared_flag(&self, old_state: u64) {
        let mut state = old_state;
        loop {
            if !has_shared(state) || get_share_count(state) > 0 {
                // 没有 SHARED 或还有计数，不需要清除
                return;
            }

            let new_state = state & !SHARED;
            match self.state.compare_exchange_weak(
                state,
                new_state,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return,
                Err(current) => state = current,
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

        self.state
            .compare_exchange(old_state, new_state, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    // ========== 排他锁获取 ==========

    /// 获取排他锁
    pub fn acquire_exclusive(&self) {
        let mut spins = self.spins_per_delay.load(Ordering::Relaxed);

        loop {
            let old_state = self.state.load(Ordering::Acquire);

            // 检查是否可以获取排他锁（无任何锁，无 DISALLOW_PREEMPT）
            if can_acquire(old_state) {
                // CAS 设置 EXCLUSIVE 标志
                let new_state = old_state | EXCLUSIVE;
                if self
                    .state
                    .compare_exchange_weak(old_state, new_state, Ordering::AcqRel, Ordering::Relaxed)
                    .is_ok()
                {
                    fence(Ordering::Acquire);
                    return;
                }
                continue;
            }

            // 需要等待
            if has_waiters(old_state) {
                self.wait_exclusive();
                return;
            }

            // 尝试设置 HAS_WAITERS
            let with_waiters = old_state | HAS_WAITERS;
            if self
                .state
                .compare_exchange_weak(old_state, with_waiters, Ordering::AcqRel, Ordering::Relaxed)
                .is_ok()
            {
                self.wait_exclusive();
                return;
            }

            // 自旋
            spins = self.spin_delay(spins);
        }
    }

    /// 尝试获取排他锁（非阻塞）
    pub fn try_acquire_exclusive(&self) -> bool {
        let old_state = self.state.load(Ordering::Acquire);

        if !can_acquire(old_state) {
            return false;
        }

        let new_state = old_state | EXCLUSIVE;
        self.state
            .compare_exchange(old_state, new_state, Ordering::AcqRel, Ordering::Relaxed)
            .is_ok()
    }

    // ========== 锁释放 ==========

    /// 释放共享锁
    /// 释放共享锁（纯 CAS，与 acquire_shared 对称）
    pub fn release_shared(&self) {
        loop {
            let old_state = self.state.load(Ordering::Acquire);
            let count = get_share_count(old_state);
            debug_assert!(count > 0, "release_shared with count=0");
            if count == 0 { return; }

            let new_count = count - 1;
            let mut new_state = (old_state & !SHARE_COUNT_MASK) | (new_count as u64);
            if new_count == 0 {
                new_state &= !SHARED;
            }

            if self.state.compare_exchange_weak(
                old_state, new_state, Ordering::AcqRel, Ordering::Relaxed,
            ).is_ok() {
                // 最后一个 reader 释放且有等待者 → 唤醒
                if new_count == 0 && has_waiters(old_state) {
                    self.wakeup_waiters();
                }
                return;
            }
        }
    }

    /// 释放排他锁
    pub fn release_exclusive(&self) {
        let old_state = self.state.load(Ordering::Acquire);

        loop {
            debug_assert!(has_exclusive(old_state), "release_exclusive called without exclusive lock");

            // 清除 EXCLUSIVE 标志
            let new_state = old_state & !EXCLUSIVE;

            match self.state.compare_exchange_weak(
                old_state,
                new_state,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    // 检查是否需要唤醒等待者
                    if has_waiters(old_state) {
                        self.wakeup_waiters();
                    }
                    return;
                }
                Err(current) => {
                    // 重试
                    continue;
                }
            }
        }
    }

    // ========== FIFO 等待队列（parking_lot_core）==========
    //
    // 参照 C++ dstore LWLockQueueSelf + LWLockWakeup 协议：
    // - 等待者入队到 FIFO 队列（parking_lot_core 内部维护）
    // - ParkToken 区分 shared(0) vs exclusive(1)
    // - 释放时选择性唤醒：所有前导 shared OR 一个 exclusive
    //
    // 对比之前的 thread::park_timeout 轮询方案：
    // - 之前：等待者独立 park，靠超时重试检测锁状态（非公平）
    // - 现在：释放者主动 unpark 等待者（FIFO 公平）

    /// parking_lot_core 的 park key = state 字段地址
    #[inline]
    fn park_key(&self) -> usize {
        &self.state as *const AtomicU64 as usize
    }

    /// ParkToken 编码
    const PARK_TOKEN_SHARED: parking_lot_core::ParkToken = parking_lot_core::ParkToken(0);
    const PARK_TOKEN_EXCLUSIVE: parking_lot_core::ParkToken = parking_lot_core::ParkToken(1);

    /// 等待共享锁
    ///
    /// 短 spin → parking_lot_core::park (FIFO 队列)
    fn wait_shared(&self) {
        let mut spins: u32 = 0;
        loop {
            let state = self.state.load(Ordering::Acquire);
            if !has_exclusive(state) && !is_disallow_preempt(state) {
                if self.try_acquire_shared() {
                    return;
                }
            }

            spins += 1;
            if spins < DEFAULT_SPINS_PER_DELAY as u32 {
                std::hint::spin_loop();
            } else {
                // 入队 park — 等待 release 时被唤醒
                unsafe {
                    parking_lot_core::park(
                        self.park_key(),
                        || {
                            // Validation: 仍然需要等待吗？
                            let s = self.state.load(Ordering::Acquire);
                            has_exclusive(s) || is_disallow_preempt(s)
                        },
                        || {},   // before_sleep
                        |_, _| {}, // timed_out
                        Self::PARK_TOKEN_SHARED,
                        Some(std::time::Instant::now() + Duration::from_millis(5)),
                    );
                }
                spins = 0;
            }
        }
    }

    /// 等待排他锁
    ///
    /// 设置 DISALLOW_PREEMPT 防止写者饥饿，然后 park 等待。
    fn wait_exclusive(&self) {
        // 设置 DISALLOW_PREEMPT — 阻止新 reader 进入
        let _ = self.state.fetch_or(DISALLOW_PREEMPT, Ordering::AcqRel);

        let mut spins: u32 = 0;
        loop {
            let state = self.state.load(Ordering::Acquire);
            // 检查是否可获取（忽略自己设置的 DISALLOW_PREEMPT）
            let effective = state & !DISALLOW_PREEMPT;
            if can_acquire(effective) {
                // CAS: 清除 DISALLOW_PREEMPT + 设置 EXCLUSIVE
                let new = (state & !DISALLOW_PREEMPT) | EXCLUSIVE;
                if self.state.compare_exchange_weak(
                    state, new, Ordering::AcqRel, Ordering::Relaxed,
                ).is_ok() {
                    return;
                }
                continue;
            }

            spins += 1;
            if spins < DEFAULT_SPINS_PER_DELAY as u32 {
                std::hint::spin_loop();
            } else {
                // 入队 park — 等待 release 时被唤醒
                unsafe {
                    parking_lot_core::park(
                        self.park_key(),
                        || {
                            // Validation: 仍然需要等待吗？
                            let s = self.state.load(Ordering::Acquire);
                            let eff = s & !DISALLOW_PREEMPT;
                            !can_acquire(eff)
                        },
                        || {},
                        |_, _| {},
                        Self::PARK_TOKEN_EXCLUSIVE,
                        Some(std::time::Instant::now() + Duration::from_millis(5)),
                    );
                }
                spins = 0;
            }
        }
    }

    /// 唤醒等待者
    ///
    /// FIFO 选择性唤醒（参照 C++ dstore LWLockWakeup）：
    /// - 唤醒队列前端所有 shared waiters
    /// - 遇到 exclusive waiter 唤醒它并停止
    /// - 使用 parking_lot_core::unpark_filter 实现
    fn wakeup_waiters(&self) {
        let key = self.park_key();
        let mut woke_exclusive = false;

        unsafe {
            parking_lot_core::unpark_filter(
                key,
                |token| {
                    if woke_exclusive {
                        // 已唤醒一个 exclusive，不再唤醒更多
                        return parking_lot_core::FilterOp::Stop;
                    }
                    if token == Self::PARK_TOKEN_EXCLUSIVE {
                        woke_exclusive = true;
                        parking_lot_core::FilterOp::Unpark
                    } else {
                        // Shared waiter: 唤醒
                        parking_lot_core::FilterOp::Unpark
                    }
                },
                |_| parking_lot_core::DEFAULT_UNPARK_TOKEN,
            );
        }

        // 清除 HAS_WAITERS 标志（如果队列已空）
        if !woke_exclusive {
            let mut old_state = self.state.load(Ordering::Acquire);
            loop {
                if !has_waiters(old_state) { return; }
                let new_state = old_state & !HAS_WAITERS;
                match self.state.compare_exchange_weak(
                    old_state, new_state, Ordering::AcqRel, Ordering::Relaxed,
                ) {
                    Ok(_) => return,
                    Err(current) => old_state = current,
                }
            }
        }
    }

    // ========== 工具方法 ==========

    /// 自旋延迟（带自适应退避）
    #[inline]
    fn spin_delay(&self, mut spins: i32) -> i32 {
        spins -= 1;
        if spins <= 0 {
            std::thread::yield_now();
            let current = self.spins_per_delay.load(Ordering::Relaxed);
            let new_spins = (current * 2).min(MAX_SPINS_PER_DELAY);
            self.spins_per_delay.store(new_spins, Ordering::Relaxed);
            new_spins
        } else {
            std::hint::spin_loop();
            spins
        }
    }

    /// 获取锁（根据模式）
    #[inline]
    pub fn acquire(&self, mode: LockMode) {
        match mode {
            LockMode::Shared => self.acquire_shared(),
            LockMode::Exclusive => self.acquire_exclusive(),
        }
    }

    /// 尝试获取锁（根据模式）
    #[inline]
    pub fn try_acquire(&self, mode: LockMode) -> bool {
        match mode {
            LockMode::Shared => self.try_acquire_shared(),
            LockMode::Exclusive => self.try_acquire_exclusive(),
        }
    }

    /// 释放锁（根据模式）
    #[inline]
    pub fn release(&self, mode: LockMode) {
        match mode {
            LockMode::Shared => self.release_shared(),
            LockMode::Exclusive => self.release_exclusive(),
        }
    }
}

impl Default for LWLock {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Send for LWLock {}
unsafe impl Sync for LWLock {}

// 编译时验证大小
const _: () = assert!(std::mem::size_of::<LWLock>() == 128);
const _: () = assert!(std::mem::align_of::<LWLock>() == 128);

/// LWLock 守卫（RAII）
#[derive(Debug)]
pub struct LockGuard<'a> {
    lock: &'a LWLock,
    mode: LockMode,
}

impl<'a> LockGuard<'a> {
    /// 获取锁并返回守卫
    #[inline]
    pub fn new(lock: &'a LWLock, mode: LockMode) -> Self {
        lock.acquire(mode);
        Self { lock, mode }
    }

    /// 尝试获取锁并返回守卫
    #[inline]
    pub fn try_new(lock: &'a LWLock, mode: LockMode) -> Option<Self> {
        if lock.try_acquire(mode) {
            Some(Self { lock, mode })
        } else {
            None
        }
    }

    /// 释放锁（提前）
    pub fn release(guard: Self) {
        drop(guard);
    }
}

impl Drop for LockGuard<'_> {
    #[inline]
    fn drop(&mut self) {
        self.lock.release(self.mode);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::atomic::AtomicU32;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_lwlock_size() {
        assert_eq!(std::mem::size_of::<LWLock>(), 128);
        assert_eq!(std::mem::align_of::<LWLock>(), 128);
    }

    #[test]
    fn test_lwlock_shared_basic() {
        let lock = LWLock::new();

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
    fn test_lwlock_exclusive_basic() {
        let lock = LWLock::new();

        lock.acquire_exclusive();
        assert!(lock.is_locked());
        assert!(has_exclusive(lock.state()));

        lock.release_exclusive();
        assert!(!lock.is_locked());
    }

    #[test]
    fn test_lwlock_try_acquire() {
        let lock = LWLock::new();

        assert!(lock.try_acquire_exclusive());
        assert!(!lock.try_acquire_shared());
        assert!(!lock.try_acquire_exclusive());

        lock.release_exclusive();

        assert!(lock.try_acquire_shared());
        assert!(lock.try_acquire_shared());
        assert!(!lock.try_acquire_exclusive());

        lock.release_shared();
        lock.release_shared();
    }

    #[test]
    fn test_lwlock_guard() {
        let lock = LWLock::new();

        {
            let _guard = LockGuard::new(&lock, LockMode::Exclusive);
            assert!(lock.is_locked());
        }

        assert!(!lock.is_locked());
    }

    #[test]
    fn test_lwlock_concurrent_shared() {
        let lock = Arc::new(LWLock::new());
        let counter = Arc::new(AtomicU32::new(0));
        let mut handles = vec![];

        for _ in 0..10 {
            let lock = Arc::clone(&lock);
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let _guard = LockGuard::new(&lock, LockMode::Shared);
                    // 临界区内读取
                    let _ = counter.load(Ordering::Relaxed);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        // 最终状态：所有共享锁已释放
        let state = lock.state();
        assert_eq!(get_share_count(state), 0);
    }

    #[test]
    fn test_lwlock_concurrent_exclusive() {
        let lock = Arc::new(LWLock::new());
        let counter = Arc::new(AtomicU32::new(0));
        let mut handles = vec![];

        for _ in 0..2 {
            let lock = Arc::clone(&lock);
            let counter = Arc::clone(&counter);
            handles.push(thread::spawn(move || {
                for _ in 0..20 {
                    let _guard = LockGuard::new(&lock, LockMode::Exclusive);
                    counter.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(counter.load(Ordering::Relaxed), 40);
    }

    #[test]
    fn test_lwlock_shared_exclusive_mutex() {
        let lock = Arc::new(LWLock::new());
        let data = Arc::new(AtomicU32::new(0));
        let mut handles = vec![];

        for _ in 0..2 {
            let lock = Arc::clone(&lock);
            let data = Arc::clone(&data);
            handles.push(thread::spawn(move || {
                for _ in 0..20 {
                    let _guard = LockGuard::new(&lock, LockMode::Shared);
                    let _ = data.load(Ordering::Relaxed);
                }
            }));
        }

        for _ in 0..2 {
            let lock = Arc::clone(&lock);
            let data = Arc::clone(&data);
            handles.push(thread::spawn(move || {
                for _ in 0..20 {
                    let _guard = LockGuard::new(&lock, LockMode::Exclusive);
                    data.fetch_add(1, Ordering::Relaxed);
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(data.load(Ordering::Relaxed), 40);
    }

    #[test]
    fn test_fetch_add_rollback() {
        // 测试 fetch_add + CAS 回滚机制
        let lock = LWLock::new();

        // 先获取排他锁
        lock.acquire_exclusive();

        // 在另一个线程尝试获取共享锁（会回滚）
        let lock_clone = Arc::new(LWLock::new());
        lock_clone.acquire_exclusive();

        let lock_ref = Arc::clone(&lock_clone);
        let handle = thread::spawn(move || {
            // 这会先 fetch_add，发现排他锁，然后回滚
            let state_before = lock_ref.state();
            lock_ref.acquire_shared();
            let state_after = lock_ref.state();
            (state_before, state_after)
        });

        // 释放排他锁
        lock_clone.release_exclusive();

        let (before, after) = handle.join().unwrap();

        // 验证共享锁获取成功
        assert!(has_shared(after));
    }
}

// ========== loom 并发测试 ==========
#[cfg(all(test, feature = "loom"))]
mod loom_tests {
    use super::*;
    use loom::sync::atomic::{AtomicU32, AtomicU64, Ordering};
    use loom::thread;
    use std::sync::Arc;

    #[test]
    fn test_shared_lock_concurrent() {
        loom::model(|| {
            let lock = Arc::new(LWLock::new());

            let handles: Vec<_> = (0..3)
                .map(|_| {
                    let lock = lock.clone();
                    thread::spawn(move || {
                        lock.acquire_shared();
                        // 临界区
                        lock.release_shared();
                    })
                })
                .collect();

            for h in handles {
                h.join().unwrap();
            }

            // 验证最终状态
            let state = lock.state();
            assert_eq!(get_share_count(state), 0);
            assert!(!has_shared(state));
        });
    }

    #[test]
    fn test_shared_exclusive_mutex() {
        loom::model(|| {
            let lock = Arc::new(LWLock::new());
            let counter = Arc::new(AtomicU32::new(0));

            let lock1 = lock.clone();
            let counter1 = counter.clone();
            let t1 = thread::spawn(move || {
                lock1.acquire_exclusive();
                counter1.fetch_add(1, Ordering::Relaxed);
                lock1.release_exclusive();
            });

            let lock2 = lock.clone();
            let counter2 = counter.clone();
            let t2 = thread::spawn(move || {
                lock2.acquire_shared();
                counter2.fetch_add(1, Ordering::Relaxed);
                lock2.release_shared();
            });

            t1.join().unwrap();
            t2.join().unwrap();

            assert_eq!(counter.load(Ordering::Relaxed), 2);
        });
    }
}
