//! 内存布局工具

use std::ops::{Deref, DerefMut};

/// 向上对齐到指定边界
///
/// # Panics
/// 如果 align 不是 2 的幂次方，会 panic
#[inline]
pub const fn align_up(size: usize, align: usize) -> usize {
    assert!(align.is_power_of_two(), "align must be power of two");
    let mask = align - 1;
    (size + mask) & !mask
}

/// 检查 size 是否对齐到指定边界
#[inline]
pub const fn is_aligned(size: usize, align: usize) -> bool {
    assert!(align.is_power_of_two(), "align must be power of two");
    (size & (align - 1)) == 0
}

/// 缓存行对齐的包装器
///
/// 用于确保数据结构不会发生伪共享（false sharing）。
#[repr(align(128))]
#[derive(Debug)]
pub struct CacheAligned<T>(pub T);

impl<T> CacheAligned<T> {
    /// 创建新的缓存行对齐值
    #[inline]
    pub const fn new(value: T) -> Self {
        Self(value)
    }

    /// 获取内部值的引用
    #[inline]
    pub const fn get(&self) -> &T {
        &self.0
    }

    /// 获取内部值的可变引用
    #[inline]
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.0
    }
}

impl<T> Deref for CacheAligned<T> {
    type Target = T;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<T> DerefMut for CacheAligned<T> {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl<T: Clone> Clone for CacheAligned<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl<T: Copy> Copy for CacheAligned<T> {}

impl<T: Default> Default for CacheAligned<T> {
    #[inline]
    fn default() -> Self {
        Self(T::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn test_align_up() {
        assert_eq!(align_up(0, 8), 0);
        assert_eq!(align_up(1, 8), 8);
        assert_eq!(align_up(7, 8), 8);
        assert_eq!(align_up(8, 8), 8);
        assert_eq!(align_up(9, 8), 16);
        assert_eq!(align_up(128, 128), 128);
        assert_eq!(align_up(129, 128), 256);
    }

    #[test]
    fn test_is_aligned() {
        assert!(is_aligned(0, 8));
        assert!(is_aligned(8, 8));
        assert!(is_aligned(16, 8));
        assert!(is_aligned(128, 128));
        assert!(!is_aligned(1, 8));
        assert!(!is_aligned(7, 8));
        assert!(!is_aligned(127, 128));
    }

    #[test]
    fn test_cache_aligned_size() {
        // CacheAligned 应该至少有 128 字节对齐
        assert!(mem::align_of::<CacheAligned<u8>>() >= 128);
    }
}
