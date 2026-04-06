//! Lock Manager
//!
//! 锁管理器实现。使用 parking_lot Condvar + 超时退避避免 livelock。

use super::lock::{Lock, LockMode, LockTag};
use ferrisdb_core::Xid;
use parking_lot::{Condvar, Mutex, RwLock};
use std::collections::HashMap;

/// 锁管理器
///
/// 管理所有重量级锁。
/// 使用 parking_lot Condvar 等待机制，释放锁时唤醒等待者。
pub struct LockManager {
    /// 锁表
    locks: RwLock<HashMap<LockTag, Lock>>,
    /// 死锁检测器
    deadlock_detector: super::deadlock::DeadlockDetector,
    /// 等待通知机制（parking_lot，与 locks 的 RwLock 同系避免混用问题）
    wait_state: Mutex<()>,
    wait_condvar: Condvar,
}

impl LockManager {
    /// 创建新的锁管理器
    pub fn new() -> Self {
        Self {
            locks: RwLock::new(HashMap::new()),
            deadlock_detector: super::deadlock::DeadlockDetector::new(),
            wait_state: Mutex::new(()),
            wait_condvar: Condvar::new(),
        }
    }

    /// 获取锁
    ///
    /// 如果获取失败，会等待直到获取或检测到死锁。
    pub fn acquire(&self, tag: LockTag, mode: LockMode, xid: Xid) -> ferrisdb_core::Result<()> {
        // 快速路径：锁已存在，尝试获取
        {
            let locks = self.locks.read();
            if let Some(lock) = locks.get(&tag) {
                if lock.try_acquire(mode) {
                    lock.set_holder(xid);
                    return Ok(());
                }
                let holder = lock.holder();
                drop(locks);
                return self.wait_for_lock(&tag, mode, xid, holder);
            }
        }

        // 慢速路径：创建新锁
        {
            let mut locks = self.locks.write();
            if let Some(lock) = locks.get(&tag) {
                if lock.try_acquire(mode) {
                    lock.set_holder(xid);
                    return Ok(());
                }
                let holder = lock.holder();
                drop(locks);
                return self.wait_for_lock(&tag, mode, xid, holder);
            }

            let lock = Lock::new(tag.clone());
            lock.try_acquire(mode);
            lock.set_holder(xid);
            locks.insert(tag, lock);
        }

        Ok(())
    }

    /// 尝试获取锁（非阻塞）
    pub fn try_acquire(&self, tag: &LockTag, mode: LockMode) -> bool {
        let locks = self.locks.read();
        if let Some(lock) = locks.get(tag) {
            lock.try_acquire(mode)
        } else {
            false
        }
    }

    /// 释放锁
    pub fn release(&self, tag: &LockTag) -> ferrisdb_core::Result<()> {
        let locks = self.locks.read();
        if let Some(lock) = locks.get(tag) {
            lock.release();
            lock.clear_holder();
            drop(locks);
            // 唤醒一个等待者（避免惊群）
            self.wait_condvar.notify_one();
            Ok(())
        } else {
            Err(ferrisdb_core::FerrisDBError::Lock(ferrisdb_core::error::LockError::NotHeld))
        }
    }

    /// 等待锁
    fn wait_for_lock(
        &self,
        tag: &LockTag,
        mode: LockMode,
        xid: Xid,
        blocking_xid: Option<Xid>,
    ) -> ferrisdb_core::Result<()> {
        if let Some(blocker) = blocking_xid {
            self.deadlock_detector.add_wait(xid, blocker);
        }

        let mut attempts = 0u32;
        loop {
            // 死锁检测
            if self.deadlock_detector.check_deadlock(xid) {
                self.deadlock_detector.remove_wait(xid);
                return Err(ferrisdb_core::FerrisDBError::Lock(
                    ferrisdb_core::error::LockError::Deadlock,
                ));
            }

            // 尝试获取
            {
                let locks = self.locks.read();
                if let Some(lock) = locks.get(tag) {
                    if lock.try_acquire(mode) {
                        lock.set_holder(xid);
                        self.deadlock_detector.remove_wait(xid);
                        return Ok(());
                    }
                    if let Some(new_blocker) = lock.holder() {
                        self.deadlock_detector.remove_wait(xid);
                        self.deadlock_detector.add_wait(xid, new_blocker);
                    }
                } else {
                    self.deadlock_detector.remove_wait(xid);
                    return Ok(());
                }
            }

            // 等待：Condvar + 超时（指数退避，1ms → 2ms → 4ms → 最大 10ms）
            let wait_ms = std::cmp::min(1u64 << attempts.min(3), 10);
            let mut guard = self.wait_state.lock();
            self.wait_condvar.wait_for(&mut guard, std::time::Duration::from_millis(wait_ms));
            attempts += 1;

            // 安全阀：超过 5000 次重试 = 超时
            if attempts > 5000 {
                self.deadlock_detector.remove_wait(xid);
                return Err(ferrisdb_core::FerrisDBError::Lock(
                    ferrisdb_core::error::LockError::Timeout,
                ));
            }
        }
    }

    /// 获取锁数量
    pub fn lock_count(&self) -> usize {
        self.locks.read().len()
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    #[test]
    fn test_lock_manager_new() {
        let mgr = LockManager::new();
        assert_eq!(mgr.lock_count(), 0);
    }

    #[test]
    fn test_lock_manager_acquire_release() {
        let mgr = LockManager::new();
        let xid = Xid::new(0, 1);
        let tag = LockTag::Relation(100);

        mgr.acquire(tag.clone(), LockMode::Share, xid).unwrap();
        assert_eq!(mgr.lock_count(), 1);

        mgr.release(&tag).unwrap();
        assert_eq!(mgr.lock_count(), 1); // Lock still exists but released
    }

    #[test]
    fn test_lock_manager_exclusive_blocks() {
        let mgr = Arc::new(LockManager::new());
        let tag = LockTag::Relation(200);

        // Thread 1: hold exclusive lock
        let xid1 = Xid::new(0, 1);
        mgr.acquire(tag.clone(), LockMode::Exclusive, xid1).unwrap();

        // Thread 2: try to acquire should fail
        let xid2 = Xid::new(0, 2);
        assert!(!mgr.try_acquire(&tag, LockMode::Exclusive));

        // Release and re-acquire
        mgr.release(&tag).unwrap();
        mgr.acquire(tag.clone(), LockMode::Exclusive, xid2).unwrap();
        mgr.release(&tag).unwrap();
    }

    #[test]
    fn test_lock_manager_concurrent_no_livelock() {
        let mgr = Arc::new(LockManager::new());
        let tag = LockTag::Relation(300);
        let counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let mut handles = vec![];

        for i in 0..8 {
            let mgr = Arc::clone(&mgr);
            let tag = tag.clone();
            let counter = Arc::clone(&counter);
            handles.push(std::thread::spawn(move || {
                let xid = Xid::new(0, (i + 1) as u32);
                for _ in 0..50 {
                    mgr.acquire(tag.clone(), LockMode::Exclusive, xid).unwrap();
                    counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    mgr.release(&tag).unwrap();
                }
            }));
        }

        for h in handles { h.join().unwrap(); }
        assert_eq!(counter.load(std::sync::atomic::Ordering::Relaxed), 400);
    }
}
