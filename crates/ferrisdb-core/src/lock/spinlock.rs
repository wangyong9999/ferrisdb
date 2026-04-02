//! 自旋锁实现
//!
//! 简单的自旋锁，用于保护短临界区。

use crate::atomic::{AtomicU32, Ordering};
use crate::atomic::helpers::{spin_try_lock as try_lock_internal, spin_unlock as unlock_internal};
use std::cell::UnsafeCell;
use std::ops::{Deref, DerefMut};

/// 自旋锁
#[derive(Debug)]
pub struct SpinLock<T> {
    /// 锁状态：0 = 未锁定，1 = 已锁定
    lock: AtomicU32,
    /// 被保护的数据
    data: UnsafeCell<T>,
}

impl<T> SpinLock<T> {
    /// 创建新的自旋锁
    #[inline]
    pub const fn new(data: T) -> Self {
        Self {
            lock: AtomicU32::new(0),
            data: UnsafeCell::new(data),
        }
    }

    /// 获取锁
    ///
    /// 自旋直到成功获取锁
    #[inline]
    pub fn acquire(&self) {
        loop {
            if try_lock_internal(&self.lock) {
                return;
            }
        }
    }

    /// 尝试获取锁
    ///
    /// 返回是否成功
    #[inline]
    pub fn try_acquire(&self) -> bool {
        self.lock
            .compare_exchange(0, 1, Ordering::Acquire, Ordering::Relaxed)
            .is_ok()
    }

    /// 释放锁
    #[inline]
    pub fn release(&self) {
        unlock_internal(&self.lock);
    }

    /// 获取锁并返回守卫
    #[inline]
    pub fn lock(&self) -> SpinLockGuard<'_, T> {
        self.acquire();
        SpinLockGuard { lock: self }
    }

    /// 尝试获取锁并返回守卫
    #[inline]
    pub fn try_lock(&self) -> Option<SpinLockGuard<'_, T>> {
        if self.try_acquire() {
            Some(SpinLockGuard { lock: self })
        } else {
            None
        }
    }

    /// 获取内部数据的可变引用
    ///
    /// # Safety
    /// 调用者必须确保没有其他线程访问此锁
    #[inline]
    pub unsafe fn get_mut(&mut self) -> &mut T {
        self.data.get_mut()
    }
}

unsafe impl<T: Send> Send for SpinLock<T> {}
unsafe impl<T: Send> Sync for SpinLock<T> {}

/// 自旋锁守卫
#[derive(Debug)]
pub struct SpinLockGuard<'a, T> {
    lock: &'a SpinLock<T>,
}

impl<T> Deref for SpinLockGuard<'_, T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        // SAFETY: 持有锁时，可以安全访问数据
        unsafe { &*self.lock.data.get() }
    }
}

impl<T> DerefMut for SpinLockGuard<'_, T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        // SAFETY: 持有锁时，可以安全访问数据
        unsafe { &mut *self.lock.data.get() }
    }
}

impl<T> Drop for SpinLockGuard<'_, T> {
    #[inline]
    fn drop(&mut self) {
        self.lock.release();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_spinlock_basic() {
        let lock = SpinLock::new(0);
        {
            let mut guard = lock.lock();
            *guard = 42;
        }
        assert_eq!(*lock.lock(), 42);
    }

    #[test]
    fn test_spinlock_try_lock() {
        let lock = SpinLock::new(0);

        let guard1 = lock.try_lock();
        assert!(guard1.is_some());

        let guard2 = lock.try_lock();
        assert!(guard2.is_none());

        drop(guard1);

        let guard3 = lock.try_lock();
        assert!(guard3.is_some());
    }

    #[test]
    fn test_spinlock_concurrent() {
        let lock = Arc::new(SpinLock::new(0i32));
        let mut handles = vec![];

        for _ in 0..10 {
            let lock = Arc::clone(&lock);
            handles.push(thread::spawn(move || {
                for _ in 0..100 {
                    let mut guard = lock.lock();
                    *guard += 1;
                }
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(*lock.lock(), 1000);
    }
}
