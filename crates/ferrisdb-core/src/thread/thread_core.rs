//! ThreadCore 实现
//!
//! 提供线程核心抽象，用于 LWLock 等待队列。

use crate::waiter::LWLockWaiter;
use std::cell::RefCell;
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;

/// 线程 ID
pub type ThreadId = u32;

/// 线程核心抽象
///
/// 提供线程的基本操作，包括休眠、唤醒和等待者节点管理。
/// 用于 LWLock 等待队列中的线程唤醒。
pub trait ThreadCore: Send + Sync {
    /// 获取线程 ID
    fn thread_id(&self) -> ThreadId;

    /// 获取 LWLock 等待者节点
    fn lock_waiter(&self) -> &LWLockWaiter;

    /// 休眠等待唤醒
    ///
    /// # Arguments
    /// * `timeout` - 可选的超时时间
    /// * `predicate` - 继续等待的条件（避免虚假唤醒）
    ///
    /// # Returns
    /// 是否正常被唤醒（false 表示超时）
    fn sleep(&self, timeout: Option<Duration>) -> bool;

    /// 唤醒线程
    fn wakeup(&self);

    /// 检查是否在等待 LWLock
    fn is_wait_lwlock(&self) -> bool {
        self.lock_waiter().is_waiting()
    }
}

/// 标准库实现的 ThreadCore
///
/// 使用 `Condvar` 实现线程休眠和唤醒。
#[derive(Debug)]
pub struct StdThreadCore {
    /// 线程 ID
    thread_id: ThreadId,
    /// LWLock 等待者节点
    waiter: LWLockWaiter,
    /// 休眠状态
    sleep_state: Arc<(Mutex<bool>, Condvar)>,
}

impl StdThreadCore {
    /// 创建新的 ThreadCore
    pub fn new(thread_id: ThreadId) -> Self {
        Self {
            thread_id,
            waiter: LWLockWaiter::new(),
            sleep_state: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }

    /// 创建带有自定义等待者的 ThreadCore
    pub fn with_waiter(thread_id: ThreadId, waiter: LWLockWaiter) -> Self {
        Self {
            thread_id,
            waiter,
            sleep_state: Arc::new((Mutex::new(false), Condvar::new())),
        }
    }
}

impl ThreadCore for StdThreadCore {
    #[inline]
    fn thread_id(&self) -> ThreadId {
        self.thread_id
    }

    #[inline]
    fn lock_waiter(&self) -> &LWLockWaiter {
        &self.waiter
    }

    fn sleep(&self, timeout: Option<Duration>) -> bool {
        let (lock, cvar) = &*self.sleep_state;
        let mut notified = lock.lock().unwrap();

        // 设置等待状态
        self.waiter.set_waiting(true);

        // 等待唤醒
        match timeout {
            Some(dur) => {
                let result = cvar.wait_timeout_while(notified, dur, |n| !*n).unwrap();
                notified = result.0;
                self.waiter.set_waiting(false);
                // Return true if woken up (notified), false if timed out
                *notified
            }
            None => {
                while !*notified {
                    notified = cvar.wait(notified).unwrap();
                }
                self.waiter.set_waiting(false);
                true
            }
        }
    }

    fn wakeup(&self) {
        let (lock, cvar) = &*self.sleep_state;
        let mut notified = lock.lock().unwrap();
        *notified = true;
        cvar.notify_one();
    }

    #[inline]
    fn is_wait_lwlock(&self) -> bool {
        self.waiter.is_waiting()
    }
}

// 线程本地存储的 ThreadCore 引用
thread_local! {
    static CURRENT_THREAD_CORE: RefCell<Option<*const dyn ThreadCore>> = RefCell::new(None);
}

/// 获取当前线程的 ThreadCore
///
/// # Safety
/// 返回的引用可能在多线程环境下不安全使用。
/// 仅在单线程上下文中使用。
pub fn current_thread_core() -> Option<&'static dyn ThreadCore> {
    CURRENT_THREAD_CORE.with(|tc| tc.borrow().map(|ptr| unsafe { &*ptr }))
}

/// 设置当前线程的 ThreadCore
///
/// # Safety
/// ptr 必须在线程生命周期内有效。
pub unsafe fn set_current_thread_core(ptr: *const dyn ThreadCore) {
    CURRENT_THREAD_CORE.with(|tc| {
        *tc.borrow_mut() = Some(ptr);
    });
}

/// 清除当前线程的 ThreadCore
pub fn clear_current_thread_core() {
    CURRENT_THREAD_CORE.with(|tc| {
        *tc.borrow_mut() = None;
    });
}

/// 线程 ID 分配器
#[derive(Debug, Default)]
pub struct ThreadIdAllocator {
    next_id: AtomicU32,
}

impl ThreadIdAllocator {
    /// 创建新的分配器
    pub const fn new() -> Self {
        Self {
            next_id: AtomicU32::new(1), // 0 保留给无效 ID
        }
    }

    /// 分配新的线程 ID
    pub fn allocate(&self) -> ThreadId {
        use std::sync::atomic::Ordering;
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }
}

use std::sync::atomic::AtomicU32;

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_std_thread_core_basic() {
        let tc = StdThreadCore::new(1);
        assert_eq!(tc.thread_id(), 1);
        assert!(!tc.is_wait_lwlock());
    }

    #[test]
    fn test_std_thread_core_sleep_wakeup() {
        let tc = Arc::new(StdThreadCore::new(1));
        let tc2 = Arc::clone(&tc);

        let handle = thread::spawn(move || {
            // 模拟等待
            let result = tc.sleep(Some(Duration::from_millis(100)));
            assert!(!result); // 超时
        });

        handle.join().unwrap();
    }

    #[test]
    fn test_std_thread_core_wakeup_from_another() {
        let tc = Arc::new(StdThreadCore::new(1));
        let tc2 = Arc::clone(&tc);

        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            tc2.wakeup();
        });

        let result = tc.sleep(Some(Duration::from_secs(1)));
        assert!(result); // 被唤醒

        handle.join().unwrap();
    }

    #[test]
    fn test_thread_id_allocator() {
        let alloc = ThreadIdAllocator::new();
        assert_eq!(alloc.allocate(), 1);
        assert_eq!(alloc.allocate(), 2);
        assert_eq!(alloc.allocate(), 3);
    }

    #[test]
    fn test_current_thread_core_lifecycle() {
        // 初始无 ThreadCore
        assert!(current_thread_core().is_none());

        let tc = Arc::new(StdThreadCore::new(99));
        let ptr: *const dyn ThreadCore = Arc::as_ptr(&tc) as *const dyn ThreadCore;
        unsafe { set_current_thread_core(ptr); }

        let current = current_thread_core();
        assert!(current.is_some());
        assert_eq!(current.unwrap().thread_id(), 99);

        clear_current_thread_core();
        assert!(current_thread_core().is_none());
    }
}
