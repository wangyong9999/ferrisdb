//! 提交序列号定义
//!
//! CSN (Commit Sequence Number) 用于 MVCC 可见性判断。

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// 提交序列号
///
/// CSN 是单调递增的，用于判断事务的提交顺序和可见性。
///
/// # 特殊值
/// - 0: 无效/未提交
/// - 1: 第一个有效 CSN
/// - u64::MAX / 2: 冻结 CSN
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(C)]
pub struct Csn(u64);

impl Csn {
    /// 无效 CSN（未提交）
    pub const INVALID: Self = Self(0);

    /// 第一个有效 CSN
    pub const FIRST_VALID: Self = Self(1);

    /// 冻结 CSN（用于非常老的事务）
    pub const FROZEN: Self = Self(u64::MAX / 2);

    /// 最大 CSN
    pub const MAX: Self = Self(u64::MAX - 1);

    /// 从原始值创建
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self(raw)
    }

    /// 获取原始值
    #[inline]
    pub const fn raw(&self) -> u64 {
        self.0
    }

    /// 是否是有效的 CSN（已提交）
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.0 >= Self::FIRST_VALID.0 && self.0 < Self::FROZEN.0
    }

    /// 是否是冻结的 CSN
    #[inline]
    pub const fn is_frozen(&self) -> bool {
        self.0 >= Self::FROZEN.0
    }

    /// 是否是无效的 CSN（未提交）
    #[inline]
    pub const fn is_invalid(&self) -> bool {
        self.0 == Self::INVALID.0
    }

    /// 增加并返回新值
    #[inline]
    pub fn next(&self) -> Self {
        Self(self.0 + 1)
    }

    /// 比较 CSN 是否在另一个之前（考虑回卷，通常不会回卷）
    #[inline]
    pub fn precedes(&self, other: Self) -> bool {
        self.0 < other.0
    }
}

impl Default for Csn {
    fn default() -> Self {
        Self::INVALID
    }
}

impl fmt::Display for Csn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_invalid() {
            write!(f, "Csn(INVALID)")
        } else if self.is_frozen() {
            write!(f, "Csn(FROZEN)")
        } else {
            write!(f, "Csn({})", self.0)
        }
    }
}

/// CSN 分配器
///
/// 线程安全的 CSN 分配器，保证单调递增。
pub struct CsnAllocator {
    next_csn: AtomicU64,
}

impl CsnAllocator {
    /// 创建新的 CSN 分配器
    #[inline]
    pub const fn new() -> Self {
        Self {
            next_csn: AtomicU64::new(Csn::FIRST_VALID.raw()),
        }
    }

    /// 分配下一个 CSN
    ///
    /// 返回分配的 CSN。
    #[inline]
    pub fn allocate(&self) -> Csn {
        let raw = self.next_csn.fetch_add(1, Ordering::AcqRel);
        Csn::from_raw(raw)
    }

    /// 获取当前 CSN（不分配）
    #[inline]
    pub fn current(&self) -> Csn {
        Csn::from_raw(self.next_csn.load(Ordering::Acquire))
    }

    /// 设置当前 CSN（用于恢复）
    #[inline]
    pub fn set_current(&self, csn: Csn) {
        self.next_csn.store(csn.raw(), Ordering::Release);
    }
}

impl Default for CsnAllocator {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_csn_values() {
        assert!(!Csn::INVALID.is_valid());
        assert!(Csn::FIRST_VALID.is_valid());
        assert!(Csn::FROZEN.is_frozen());
    }

    #[test]
    fn test_csn_precedes() {
        let a = Csn::from_raw(100);
        let b = Csn::from_raw(200);
        assert!(a.precedes(b));
        assert!(!b.precedes(a));
    }

    #[test]
    fn test_csn_allocator_basic() {
        let allocator = CsnAllocator::new();

        let csn1 = allocator.allocate();
        let csn2 = allocator.allocate();
        let csn3 = allocator.allocate();

        assert!(csn1.precedes(csn2));
        assert!(csn2.precedes(csn3));
    }

    #[test]
    fn test_csn_allocator_concurrent() {
        let allocator = Arc::new(CsnAllocator::new());
        let mut handles = vec![];

        for _ in 0..4 {
            let alloc = Arc::clone(&allocator);
            handles.push(thread::spawn(move || {
                let mut csns = vec![];
                for _ in 0..100 {
                    csns.push(alloc.allocate());
                }
                csns
            }));
        }

        let all_csns: Vec<Csn> = handles
            .into_iter()
            .flat_map(|h| h.join().unwrap())
            .collect();

        // 验证所有 CSN 唯一
        let mut raw_csns: Vec<u64> = all_csns.iter().map(|c| c.raw()).collect();
        raw_csns.sort();
        raw_csns.dedup();
        assert_eq!(raw_csns.len(), 400);
    }
}
