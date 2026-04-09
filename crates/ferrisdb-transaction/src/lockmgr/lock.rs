//! Lock 定义
//!
//! 重量级锁结构。

use ferrisdb_core::atomic::{AtomicU32, AtomicU64, AtomicU8, Ordering};
use ferrisdb_core::Xid;
use std::sync::Mutex;

/// 锁模式
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum LockMode {
    /// 无锁
    None = 0,
    /// 意向共享锁 (IS)
    IntentShare = 1,
    /// 意向排他锁 (IX)
    IntentExclusive = 2,
    /// 共享锁 (S)
    Share = 3,
    /// 共享意向排他锁 (SIX)
    ShareIntentExclusive = 4,
    /// 更新锁 (U)
    Update = 5,
    /// 排他锁 (X)
    Exclusive = 6,
    /// 访问排他锁 (AX)
    AccessExclusive = 7,
}

impl Default for LockMode {
    fn default() -> Self {
        Self::None
    }
}

impl LockMode {
    /// 检查是否与另一模式冲突
    pub fn conflicts_with(&self, other: LockMode) -> bool {
        // 锁冲突矩阵
        const CONFLICT_MATRIX: [[bool; 8]; 8] = [
            // None IS   IX   S    SIX  U    X    AX
            [false, false, false, false, false, false, false, false], // None
            [false, false, false, false, false, false, false, true],  // IS
            [false, false, false, true,  true,  true,  true,  true],  // IX
            [false, false, true,  false, true,  false, true,  true],  // S
            [false, false, true,  true,  true,  true,  true,  true],  // SIX
            [false, false, true,  false, true,  false, true,  true],  // U
            [false, false, true,  true,  true,  true,  true,  true],  // X
            [false, true,  true,  true,  true,  true,  true,  true],  // AX
        ];

        CONFLICT_MATRIX[*self as usize][other as usize]
    }
}

/// 锁标签
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum LockTag {
    /// 关系锁 (relation OID)
    Relation(u32),
    /// 元组锁 (relation OID, block_no, offset)
    Tuple(u32, u32, u16),
    /// 页面锁 (relation OID, block_no)
    Page(u32, u32),
    /// 事务锁 (XID)
    Transaction(u64),
}

impl LockTag {
    /// 计算 hash 值
    pub fn hash_value(&self) -> u64 {
        use std::hash::{Hash, Hasher};
        use std::collections::hash_map::DefaultHasher;

        let mut hasher = DefaultHasher::new();
        self.hash(&mut hasher);
        hasher.finish()
    }
}

/// 锁等待者
#[derive(Debug)]
#[repr(C)]
pub struct LockWaiter {
    /// 事务 ID
    pub xid: AtomicU64,
    /// 请求的锁模式
    pub mode: AtomicU8,
    /// 是否等待中
    pub waiting: AtomicU32,
    /// 下一个等待者索引
    pub next: AtomicU32,
}

impl LockWaiter {
    /// 创建新的等待者
    pub const fn new() -> Self {
        Self {
            xid: AtomicU64::new(0),
            mode: AtomicU8::new(LockMode::None as u8),
            waiting: AtomicU32::new(0),
            next: AtomicU32::new(u32::MAX),
        }
    }
}

impl Default for LockWaiter {
    fn default() -> Self {
        Self::new()
    }
}

/// 锁结构
#[derive(Debug)]
pub struct Lock {
    /// 锁标签
    pub tag: LockTag,
    /// 持有者的锁模式（合并后）
    pub hold_mode: AtomicU8,
    /// 当前持有者 XID（用于死锁检测）
    holder_xid: AtomicU64,
    /// 等待队列头
    pub wait_queue_head: AtomicU32,
    /// 等待队列尾
    pub wait_queue_tail: AtomicU32,
    /// 保护操作的锁
    pub lock: Mutex<()>,
}

impl Lock {
    /// 创建新锁
    pub fn new(tag: LockTag) -> Self {
        Self {
            tag,
            hold_mode: AtomicU8::new(LockMode::None as u8),
            holder_xid: AtomicU64::new(0),
            wait_queue_head: AtomicU32::new(u32::MAX),
            wait_queue_tail: AtomicU32::new(u32::MAX),
            lock: Mutex::new(()),
        }
    }

    /// 尝试获取锁（原子 CAS，防止 TOCTOU 竞态）
    pub fn try_acquire(&self, mode: LockMode) -> bool {
        loop {
            let current = self.hold_mode.load(Ordering::Acquire);
            let current_mode = Self::u8_to_mode(current);

            if mode.conflicts_with(current_mode) && current_mode != LockMode::None {
                return false;
            }

            // CAS: 只有当 hold_mode 未被并发修改时才更新
            match self.hold_mode.compare_exchange(
                current, mode as u8, Ordering::AcqRel, Ordering::Acquire,
            ) {
                Ok(_) => return true,
                Err(_) => continue, // 被并发修改，重试
            }
        }
    }

    /// 释放锁
    pub fn release(&self) {
        self.hold_mode.store(LockMode::None as u8, Ordering::Release);
    }

    /// 设置持有者 XID
    pub fn set_holder(&self, xid: Xid) {
        self.holder_xid.store(xid.raw(), Ordering::Release);
    }

    /// 清除持有者
    pub fn clear_holder(&self) {
        self.holder_xid.store(0, Ordering::Release);
    }

    /// 获取当前持有者 XID
    pub fn holder(&self) -> Option<Xid> {
        let raw = self.holder_xid.load(Ordering::Acquire);
        if raw == 0 {
            None
        } else {
            Some(Xid::from_raw(raw))
        }
    }

    /// 获取当前持有模式
    pub fn hold_mode(&self) -> LockMode {
        Self::u8_to_mode(self.hold_mode.load(Ordering::Acquire))
    }

    fn u8_to_mode(val: u8) -> LockMode {
        match val {
            0 => LockMode::None,
            1 => LockMode::IntentShare,
            2 => LockMode::IntentExclusive,
            3 => LockMode::Share,
            4 => LockMode::ShareIntentExclusive,
            5 => LockMode::Update,
            6 => LockMode::Exclusive,
            7 => LockMode::AccessExclusive,
            _ => LockMode::None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_mode_conflicts() {
        // 共享锁之间不冲突
        assert!(!LockMode::Share.conflicts_with(LockMode::Share));

        // 共享锁和排他锁冲突
        assert!(LockMode::Share.conflicts_with(LockMode::Exclusive));

        // 排他锁和所有锁冲突（除了 None）
        assert!(LockMode::Exclusive.conflicts_with(LockMode::Share));
        assert!(LockMode::Exclusive.conflicts_with(LockMode::Exclusive));
        assert!(!LockMode::Exclusive.conflicts_with(LockMode::None));
    }

    #[test]
    fn test_lock_try_acquire() {
        let lock = Lock::new(LockTag::Relation(100));

        // 获取共享锁
        assert!(lock.try_acquire(LockMode::Share));
        assert_eq!(lock.hold_mode(), LockMode::Share);

        // 释放
        lock.release();
        assert_eq!(lock.hold_mode(), LockMode::None);

        // 获取排他锁
        assert!(lock.try_acquire(LockMode::Exclusive));
        assert_eq!(lock.hold_mode(), LockMode::Exclusive);
    }

    #[test]
    fn test_lock_tag_hash() {
        let tag1 = LockTag::Relation(100);
        let tag2 = LockTag::Relation(100);
        let tag3 = LockTag::Relation(101);

        assert_eq!(tag1.hash_value(), tag2.hash_value());
        assert_ne!(tag1.hash_value(), tag3.hash_value());
    }

    #[test]
    fn test_lock_holder_tracking() {
        let lock = Lock::new(LockTag::Relation(100));
        assert!(lock.holder().is_none());

        let xid = Xid::new(0, 42);
        lock.set_holder(xid);
        assert_eq!(lock.holder().unwrap().raw(), xid.raw());

        lock.clear_holder();
        assert!(lock.holder().is_none());
    }

    #[test]
    fn test_lock_mode_default() {
        assert_eq!(LockMode::default(), LockMode::None);
    }

    #[test]
    fn test_lock_waiter_default() {
        let w = LockWaiter::default();
        assert_eq!(w.xid.load(Ordering::Relaxed), 0);
        let w2 = LockWaiter::new();
        assert_eq!(w2.next.load(Ordering::Relaxed), u32::MAX);
    }

    #[test]
    fn test_lock_conflict_matrix_all() {
        // IS only conflicts with AX
        assert!(!LockMode::IntentShare.conflicts_with(LockMode::Share));
        assert!(LockMode::IntentShare.conflicts_with(LockMode::AccessExclusive));
        // IX conflicts with S, SIX, U, X, AX
        assert!(LockMode::IntentExclusive.conflicts_with(LockMode::Share));
        // SIX conflicts with IX, S, SIX, U, X, AX
        assert!(LockMode::ShareIntentExclusive.conflicts_with(LockMode::IntentExclusive));
        // Update: exercise the path
        let _ = LockMode::Update.conflicts_with(LockMode::Update);
        // AX conflicts with everything except None
        assert!(LockMode::AccessExclusive.conflicts_with(LockMode::IntentShare));
    }

    #[test]
    fn test_lock_tag_variants() {
        let _ = LockTag::Page(1, 5).hash_value();
        let _ = LockTag::Transaction(42).hash_value();
        let _ = LockTag::Tuple(1, 2, 3).hash_value();
    }
}
