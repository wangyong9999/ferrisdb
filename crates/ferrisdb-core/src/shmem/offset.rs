//! 偏移量指针
//!
//! 使用偏移量代替原始指针，支持共享内存场景。

use std::marker::PhantomData;

/// 无效偏移量
pub const INVALID_OFFSET: u64 = u64::MAX;

/// 偏移量类型（相对于共享内存基地址）
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[repr(transparent)]
pub struct Offset(pub u64);

impl Offset {
    /// 创建空偏移量
    #[inline]
    pub const fn null() -> Self {
        Self(0)
    }

    /// 检查是否为空
    #[inline]
    pub const fn is_null(self) -> bool {
        self.0 == 0
    }

    /// 检查是否有效（非 INVALID_OFFSET）
    #[inline]
    pub const fn is_valid(self) -> bool {
        self.0 != INVALID_OFFSET
    }

    /// 偏移增加
    #[inline]
    pub const fn add(self, offset: u64) -> Self {
        Self(self.0 + offset)
    }

    /// 偏移减少
    #[inline]
    pub const fn sub(self, offset: u64) -> Self {
        Self(self.0 - offset)
    }
}

impl Default for Offset {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

impl From<u64> for Offset {
    #[inline]
    fn from(value: u64) -> Self {
        Self(value)
    }
}

impl From<Offset> for u64 {
    #[inline]
    fn from(offset: Offset) -> Self {
        offset.0
    }
}

/// 偏移量指针
///
/// 存储相对于基地址的偏移量，而不是原始指针。
/// 这样可以在共享内存场景中安全使用。
#[derive(Debug)]
#[repr(transparent)]
pub struct OffsetPtr<T> {
    offset: u64,
    _marker: PhantomData<*mut T>,
}

impl<T> OffsetPtr<T> {
    /// 创建空指针
    #[inline]
    pub const fn null() -> Self {
        Self {
            offset: INVALID_OFFSET,
            _marker: PhantomData,
        }
    }

    /// 从偏移量创建
    #[inline]
    pub const fn from_offset(offset: Offset) -> Self {
        Self {
            offset: offset.0,
            _marker: PhantomData,
        }
    }

    /// 获取偏移量
    #[inline]
    pub const fn offset(self) -> Offset {
        Offset(self.offset)
    }

    /// 检查是否为空
    #[inline]
    pub const fn is_null(self) -> bool {
        self.offset == INVALID_OFFSET
    }

    /// 转换为原始指针
    ///
    /// # Safety
    /// base 必须是有效的基地址，且 offset 必须指向有效的 T 对象
    #[inline]
    pub unsafe fn as_ptr(self, base: *const u8) -> *const T {
        if self.is_null() {
            return std::ptr::null();
        }
        // SAFETY: Caller ensures base is valid and offset points to valid T
        unsafe { base.add(self.offset as usize) as *const T }
    }

    /// 转换为可变原始指针
    ///
    /// # Safety
    /// base 必须是有效的基地址，且 offset 必须指向有效的 T 对象
    #[inline]
    pub unsafe fn as_mut_ptr(self, base: *mut u8) -> *mut T {
        if self.is_null() {
            return std::ptr::null_mut();
        }
        // SAFETY: Caller ensures base is valid and offset points to valid T
        unsafe { base.add(self.offset as usize) as *mut T }
    }
}

impl<T> Clone for OffsetPtr<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            offset: self.offset,
            _marker: PhantomData,
        }
    }
}

impl<T> Copy for OffsetPtr<T> {}

impl<T> Default for OffsetPtr<T> {
    #[inline]
    fn default() -> Self {
        Self::null()
    }
}

impl<T> PartialEq for OffsetPtr<T> {
    #[inline]
    fn eq(&self, other: &Self) -> bool {
        self.offset == other.offset
    }
}

impl<T> Eq for OffsetPtr<T> {}

unsafe impl<T: Send> Send for OffsetPtr<T> {}
unsafe impl<T: Sync> Sync for OffsetPtr<T> {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_offset() {
        let offset = Offset::null();
        assert!(offset.is_null());
        assert!(offset.is_valid());

        let offset = Offset::from(100);
        assert!(!offset.is_null());
        assert!(offset.is_valid());

        let offset = Offset::from(INVALID_OFFSET);
        assert!(!offset.is_valid());
    }

    #[test]
    fn test_offset_arithmetic() {
        let offset = Offset::from(100);
        assert_eq!(offset.add(50), Offset::from(150));
        assert_eq!(offset.sub(50), Offset::from(50));
    }

    #[test]
    fn test_offset_ptr() {
        let ptr: OffsetPtr<u32> = OffsetPtr::null();
        assert!(ptr.is_null());

        let ptr = OffsetPtr::<u32>::from_offset(Offset::from(100));
        assert!(!ptr.is_null());
        assert_eq!(ptr.offset(), Offset::from(100));
    }
}
