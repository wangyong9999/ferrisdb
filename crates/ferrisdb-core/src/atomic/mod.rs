//! 原子操作扩展

#[cfg(feature = "loom")]
pub use loom::sync::atomic::{
    AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicPtr, AtomicU16,
    AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};

#[cfg(not(feature = "loom"))]
pub use std::sync::atomic::{
    AtomicBool, AtomicI16, AtomicI32, AtomicI64, AtomicI8, AtomicIsize, AtomicPtr, AtomicU16,
    AtomicU32, AtomicU64, AtomicU8, AtomicUsize, Ordering,
};

/// 原子操作辅助函数
pub mod helpers {
    use super::*;

    /// 自适应自旋等待
    #[inline]
    pub fn spin_loop() {
        #[cfg(feature = "loom")]
        loom::thread::yield_now();

        #[cfg(not(feature = "loom"))]
        std::hint::spin_loop();
    }

    /// 带退避的自旋锁获取
    ///
    /// 返回是否成功获取
    #[inline]
    pub fn spin_try_lock(lock: &AtomicU32) -> bool {
        let mut spins = 0u32;
        loop {
            let old = lock.compare_exchange_weak(0, 1, Ordering::Acquire, Ordering::Relaxed);
            if old.is_ok() {
                return true;
            }
            spins += 1;
            if spins > 100 {
                std::thread::yield_now();
                spins = 0;
            } else {
                spin_loop();
            }
            // 简单实现：不无限循环，交给调用者处理
            if spins > 1000 {
                return false;
            }
        }
    }

    /// 带退避的自旋锁释放
    #[inline]
    pub fn spin_unlock(lock: &AtomicU32) {
        lock.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_atomic_u64_basic() {
        let atomic = AtomicU64::new(0);
        assert_eq!(atomic.load(Ordering::SeqCst), 0);

        atomic.store(42, Ordering::SeqCst);
        assert_eq!(atomic.load(Ordering::SeqCst), 42);

        let old = atomic.swap(100, Ordering::SeqCst);
        assert_eq!(old, 42);
        assert_eq!(atomic.load(Ordering::SeqCst), 100);
    }

    #[test]
    fn test_cas() {
        let atomic = AtomicU64::new(10);

        // CAS 成功
        let result = atomic.compare_exchange(10, 20, Ordering::SeqCst, Ordering::SeqCst);
        assert!(result.is_ok());
        assert_eq!(atomic.load(Ordering::SeqCst), 20);

        // CAS 失败
        let result = atomic.compare_exchange(10, 30, Ordering::SeqCst, Ordering::SeqCst);
        assert!(result.is_err());
        assert_eq!(atomic.load(Ordering::SeqCst), 20);
    }
}
