//! LWLock 等待者节点
//!
//! 与 C++ LWLockWaiter 二进制兼容。

use crate::atomic::{AtomicBool, AtomicU8, AtomicU32, Ordering};
use crate::list::{ShmDListNode, INVALID_NODE_INDEX};

/// 锁模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum LockMode {
    /// 共享锁
    Shared = 0,
    /// 排他锁
    Exclusive = 1,
}

impl Default for LockMode {
    fn default() -> Self {
        Self::Shared
    }
}

/// LWLock 等待者节点
///
/// 与 C++ LWLockWaiter 结构二进制兼容。
/// 用于等待队列中的每个线程。
#[derive(Debug)]
#[repr(C)]
pub struct LWLockWaiter {
    /// 是否正在等待
    pub waiting: AtomicBool,
    /// 请求的锁模式
    pub mode: AtomicU8,
    /// 等待队列链接节点（使用索引而非指针）
    pub link: ShmDListNode,
    /// 等待的锁索引（INVALID_NODE_INDEX 表示未等待任何锁）
    pub waiting_on: AtomicU32,
    /// 线程 ID（用于调试和诊断）
    pub thread_id: AtomicU32,
}

impl LWLockWaiter {
    /// 创建新的等待者节点
    #[inline]
    pub const fn new() -> Self {
        Self {
            waiting: AtomicBool::new(false),
            mode: AtomicU8::new(LockMode::Shared as u8),
            link: ShmDListNode::new(),
            waiting_on: AtomicU32::new(INVALID_NODE_INDEX),
            thread_id: AtomicU32::new(0),
        }
    }

    /// 初始化等待者（清除所有状态）
    #[inline]
    pub fn init(&self) {
        self.waiting.store(false, Ordering::Release);
        self.mode.store(LockMode::Shared as u8, Ordering::Release);
        self.waiting_on.store(INVALID_NODE_INDEX, Ordering::Release);
        self.link.init();
    }

    /// 检查是否正在等待
    #[inline]
    pub fn is_waiting(&self) -> bool {
        self.waiting.load(Ordering::Acquire)
    }

    /// 设置等待状态
    #[inline]
    pub fn set_waiting(&self, waiting: bool) {
        self.waiting.store(waiting, Ordering::Release);
    }

    /// 获取锁模式
    #[inline]
    pub fn get_mode(&self) -> LockMode {
        match self.mode.load(Ordering::Acquire) {
            0 => LockMode::Shared,
            1 => LockMode::Exclusive,
            _ => LockMode::Shared,
        }
    }

    /// 设置锁模式
    #[inline]
    pub fn set_mode(&self, mode: LockMode) {
        self.mode.store(mode as u8, Ordering::Release);
    }

    /// 获取等待的锁索引
    #[inline]
    pub fn get_waiting_on(&self) -> u32 {
        self.waiting_on.load(Ordering::Acquire)
    }

    /// 设置等待的锁索引
    #[inline]
    pub fn set_waiting_on(&self, lock_idx: u32) {
        self.waiting_on.store(lock_idx, Ordering::Release);
    }

    /// 清除等待的锁索引
    #[inline]
    pub fn clear_waiting_on(&self) {
        self.waiting_on.store(INVALID_NODE_INDEX, Ordering::Release);
    }
}

impl Default for LWLockWaiter {
    fn default() -> Self {
        Self::new()
    }
}

// 安全实现：等待者节点可以在线程间共享
unsafe impl Send for LWLockWaiter {}
unsafe impl Sync for LWLockWaiter {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_mode() {
        assert_eq!(LockMode::Shared as u8, 0);
        assert_eq!(LockMode::Exclusive as u8, 1);
    }

    #[test]
    fn test_lwlock_waiter_new() {
        let waiter = LWLockWaiter::new();
        assert!(!waiter.is_waiting());
        assert_eq!(waiter.get_mode(), LockMode::Shared);
        assert_eq!(waiter.get_waiting_on(), INVALID_NODE_INDEX);
    }

    #[test]
    fn test_lwlock_waiter_state() {
        let waiter = LWLockWaiter::new();

        waiter.set_mode(LockMode::Exclusive);
        assert_eq!(waiter.get_mode(), LockMode::Exclusive);

        waiter.set_waiting(true);
        assert!(waiter.is_waiting());

        waiter.set_waiting_on(42);
        assert_eq!(waiter.get_waiting_on(), 42);

        waiter.clear_waiting_on();
        assert_eq!(waiter.get_waiting_on(), INVALID_NODE_INDEX);
    }

    #[test]
    fn test_lwlock_waiter_init() {
        let waiter = LWLockWaiter::new();
        waiter.set_waiting(true);
        waiter.set_mode(LockMode::Exclusive);
        waiter.set_waiting_on(100);

        waiter.init();
        assert!(!waiter.is_waiting());
        assert_eq!(waiter.get_mode(), LockMode::Shared);
        assert_eq!(waiter.get_waiting_on(), INVALID_NODE_INDEX);
    }
}
