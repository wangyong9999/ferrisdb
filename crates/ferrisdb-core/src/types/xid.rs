//! 事务 ID 定义
//!
//! 64 位编码：高 32 位为 Undo Zone ID，低 32 位为 Slot ID。

use std::fmt;
use std::hash::Hash;
use std::sync::atomic::{AtomicU64, Ordering};

/// 事务 ID
///
/// # 布局
/// - 高 32 位: zone_id (Undo Zone ID)
/// - 低 32 位: slot_id (事务槽位 ID)
///
/// # C++ 参考
/// `include/transaction/ferrisdb_transaction.h` 中的 TransactionId
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct Xid(u64);

impl Xid {
    /// 无效事务 ID
    pub const INVALID: Self = Self(0);

    /// 第一个正常事务 ID
    pub const FIRST_NORMAL: Self = Self(1);

    /// 冻结事务 ID（用于非常老的事务，跳过可见性检查）
    pub const FROZEN: Self = Self(u64::MAX / 2);

    /// 创建新的事务 ID
    #[inline]
    pub const fn new(zone_id: u32, slot_id: u32) -> Self {
        Self(((zone_id as u64) << 32) | (slot_id as u64))
    }

    /// 获取 Undo Zone ID
    #[inline]
    pub const fn zone_id(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// 获取事务槽位 ID
    #[inline]
    pub const fn slot_id(&self) -> u32 {
        self.0 as u32
    }

    /// 是否是有效的事务 ID
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.0 != Self::INVALID.0
    }

    /// 是否是无效的事务 ID
    #[inline]
    pub const fn is_invalid(&self) -> bool {
        self.0 == Self::INVALID.0
    }

    /// 是否是冻结的事务 ID
    #[inline]
    pub const fn is_frozen(&self) -> bool {
        self.0 >= Self::FROZEN.0
    }

    /// 获取原始值
    #[inline]
    pub const fn raw(&self) -> u64 {
        self.0
    }

    /// 从原始值创建
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// 比较两个事务 ID 的大小（考虑回卷）
    ///
    /// 返回 true 如果 a < b
    #[inline]
    pub fn precedes(a: Self, b: Self) -> bool {
        // 使用 32 位比较（假设不使用 FROZEN）
        let diff = a.0.wrapping_sub(b.0) as i64;
        diff < 0
    }
}

impl Default for Xid {
    fn default() -> Self {
        Self::INVALID
    }
}

impl fmt::Display for Xid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_valid() {
            write!(f, "Xid({}:{})", self.zone_id(), self.slot_id())
        } else {
            write!(f, "Xid(INVALID)")
        }
    }
}

/// 原子事务 ID
#[repr(transparent)]
pub struct AtomicXid(AtomicU64);

impl AtomicXid {
    /// 创建新的原子事务 ID
    #[inline]
    pub const fn new(xid: Xid) -> Self {
        Self(AtomicU64::new(xid.0))
    }

    /// 加载事务 ID
    #[inline]
    pub fn load(&self, order: Ordering) -> Xid {
        Xid(self.0.load(order))
    }

    /// 存储事务 ID
    #[inline]
    pub fn store(&self, xid: Xid, order: Ordering) {
        self.0.store(xid.0, order)
    }

    /// 比较并交换
    #[inline]
    pub fn compare_exchange(
        &self,
        current: Xid,
        new: Xid,
        success: Ordering,
        failure: Ordering,
    ) -> Result<Xid, Xid> {
        match self.0.compare_exchange(current.0, new.0, success, failure) {
            Ok(v) => Ok(Xid(v)),
            Err(v) => Err(Xid(v)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_xid_new() {
        let xid = Xid::new(1, 100);
        assert_eq!(xid.zone_id(), 1);
        assert_eq!(xid.slot_id(), 100);
        assert!(xid.is_valid());
        assert!(!xid.is_frozen());
    }

    #[test]
    fn test_xid_invalid() {
        let xid = Xid::INVALID;
        assert!(!xid.is_valid());
        assert_eq!(xid.zone_id(), 0);
        assert_eq!(xid.slot_id(), 0);
    }

    #[test]
    fn test_xid_frozen() {
        let xid = Xid::FROZEN;
        assert!(xid.is_frozen());
        assert!(xid.is_valid());
    }

    #[test]
    fn test_xid_precedes() {
        let a = Xid::new(0, 100);
        let b = Xid::new(0, 200);
        assert!(Xid::precedes(a, b));
        assert!(!Xid::precedes(b, a));
        assert!(!Xid::precedes(a, a));
    }

    #[test]
    fn test_xid_display() {
        let xid = Xid::new(1, 42);
        assert_eq!(format!("{}", xid), "Xid(1:42)");

        let invalid = Xid::INVALID;
        assert_eq!(format!("{}", invalid), "Xid(INVALID)");
    }

    #[test]
    fn test_atomic_xid() {
        let atomic = AtomicXid::new(Xid::new(1, 100));
        assert_eq!(atomic.load(Ordering::SeqCst), Xid::new(1, 100));

        atomic.store(Xid::new(2, 200), Ordering::SeqCst);
        assert_eq!(atomic.load(Ordering::SeqCst), Xid::new(2, 200));
    }

    #[test]
    fn test_atomic_xid_compare_exchange() {
        let atomic = AtomicXid::new(Xid::new(0, 10));
        let result = atomic.compare_exchange(
            Xid::new(0, 10), Xid::new(0, 20),
            Ordering::SeqCst, Ordering::SeqCst,
        );
        assert!(result.is_ok());
        assert_eq!(atomic.load(Ordering::SeqCst), Xid::new(0, 20));

        // Failed CAS
        let result2 = atomic.compare_exchange(
            Xid::new(0, 10), Xid::new(0, 30),
            Ordering::SeqCst, Ordering::SeqCst,
        );
        assert!(result2.is_err());
    }

    #[test]
    fn test_xid_is_invalid() {
        assert!(Xid::INVALID.is_invalid());
        assert!(!Xid::new(0, 1).is_invalid());
    }
}
