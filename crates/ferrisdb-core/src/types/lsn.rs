//! 日志序列号定义
//!
//! LSN (Log Sequence Number) 用于 WAL 管理。

use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};

/// 日志序列号
///
/// LSN 用于标识 WAL 记录的位置。
///
/// # 布局
/// - 高 32 位: 文件号
/// - 低 32 位: 文件内偏移量
///
/// # 特殊值
/// - 0: 无效 LSN
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(C)]
pub struct Lsn(u64);

impl Lsn {
    /// 无效 LSN
    pub const INVALID: Self = Self(0);

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

    /// 从文件号和偏移量创建
    #[inline]
    pub const fn from_parts(file_no: u32, offset: u32) -> Self {
        Self(((file_no as u64) << 32) | (offset as u64))
    }

    /// 获取文件号（高 32 位）
    #[inline]
    pub const fn file_no(&self) -> u32 {
        (self.0 >> 32) as u32
    }

    /// 获取文件内偏移量（低 32 位）
    #[inline]
    pub const fn offset(&self) -> u32 {
        self.0 as u32
    }

    /// 获取文件号和偏移量
    #[inline]
    pub const fn parts(&self) -> (u32, u32) {
        (self.file_no(), self.offset())
    }

    /// 是否有效
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.0 != Self::INVALID.0
    }

    /// 增加
    #[inline]
    pub const fn add(&self, delta: u64) -> Self {
        Self(self.0 + delta)
    }

    /// 对齐到指定边界
    #[inline]
    pub const fn align_up(&self, alignment: u64) -> Self {
        let mask = alignment - 1;
        Self((self.0 + mask) & !mask)
    }

    /// 检查此 LSN 是否在另一个 LSN 之前
    #[inline]
    pub const fn precedes(&self, other: Lsn) -> bool {
        self.0 < other.0
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_valid() {
            write!(f, "Lsn({}:{})", self.file_no(), self.offset())
        } else {
            write!(f, "Lsn(INVALID)")
        }
    }
}

/// PLSN (Page LSN) - 页面级 LSN
///
/// 每个页面有自己的 LSN，用于 WAL 协议。
pub type Plsn = Lsn;

/// GLSN (Global LSN) - 全局 LSN
///
/// 全局唯一的 LSN，由 WAL 分配。
pub type Glsn = Lsn;

/// LSN 分配器
pub struct LsnAllocator {
    /// 全局 LSN
    global_lsn: AtomicU64,
    /// 段文件大小
    segment_size: u32,
}

impl LsnAllocator {
    /// 创建新的 LSN 分配器
    pub fn new(segment_size: u32) -> Self {
        Self {
            global_lsn: AtomicU64::new(Lsn::INVALID.raw()),
            segment_size,
        }
    }

    /// 分配指定大小的 LSN 范围
    pub fn allocate(&self, size: u32) -> Lsn {
        let aligned_size = (size + 7) / 8 * 8; // 8 字节对齐
        let raw = self.global_lsn.fetch_add(aligned_size as u64, Ordering::AcqRel);
        Lsn::from_raw(raw)
    }

    /// 获取当前 LSN
    pub fn current(&self) -> Lsn {
        Lsn::from_raw(self.global_lsn.load(Ordering::Acquire))
    }

    /// 设置当前 LSN（用于恢复）
    pub fn set_current(&self, lsn: Lsn) {
        self.global_lsn.store(lsn.raw(), Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lsn_parts() {
        let lsn = Lsn::from_parts(1, 1024);
        assert_eq!(lsn.file_no(), 1);
        assert_eq!(lsn.offset(), 1024);
        assert!(lsn.is_valid());
    }

    #[test]
    fn test_lsn_invalid() {
        let lsn = Lsn::INVALID;
        assert!(!lsn.is_valid());
    }

    #[test]
    fn test_lsn_add() {
        let lsn = Lsn::from_raw(100);
        let lsn2 = lsn.add(50);
        assert_eq!(lsn2.raw(), 150);
    }

    #[test]
    fn test_lsn_align() {
        let lsn = Lsn::from_raw(100);
        let aligned = lsn.align_up(8);
        assert_eq!(aligned.raw(), 104);
    }

    #[test]
    fn test_lsn_allocator() {
        let allocator = LsnAllocator::new(1024 * 1024); // 1MB segment

        let lsn1 = allocator.allocate(100);
        let lsn2 = allocator.allocate(50);

        assert!(lsn1.precedes(lsn2));
    }

    #[test]
    fn test_lsn_add_file_no_offset() {
        let lsn = Lsn::from_parts(2, 500);
        assert_eq!(lsn.file_no(), 2);
        assert_eq!(lsn.offset(), 500);
        let added = lsn.add(100);
        assert_eq!(added.raw(), lsn.raw() + 100);
        let _ = Lsn::INVALID;
        let _ = format!("{:?}", lsn);
    }
}
