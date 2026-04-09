//! 文件标签定义

use std::fmt;
use std::hash::{Hash, Hasher};

/// 文件标签
///
/// 标识表空间内的一个文件，大小为 6 字节。
///
/// # C++ 参考
/// `include/storage/smgr.h` 中的 FileTag
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, packed)]
pub struct FileTag {
    /// 文件 ID (表空间内的文件号)
    pub file_id: u16,
    /// 块号 (文件内的块号)
    pub block_id: u32,
}

// 手动实现 Hash 因为是 packed struct
impl Hash for FileTag {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Copy fields to avoid unaligned reference
        let file_id = self.file_id;
        let block_id = self.block_id;
        file_id.hash(state);
        block_id.hash(state);
    }
}

impl PartialEq for FileTag {
    fn eq(&self, other: &Self) -> bool {
        // Copy fields to avoid unaligned reference
        let self_file_id = self.file_id;
        let self_block_id = self.block_id;
        let other_file_id = other.file_id;
        let other_block_id = other.block_id;
        self_file_id == other_file_id && self_block_id == other_block_id
    }
}

impl Eq for FileTag {}

impl FileTag {
    /// 创建新的文件标签
    #[inline]
    pub const fn new(file_id: u16, block_id: u32) -> Self {
        Self { file_id, block_id }
    }

    /// 无效文件标签
    pub const INVALID: Self = Self::new(0, 0);

    /// 是否有效
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.file_id != 0 || self.block_id != 0
    }
}

impl fmt::Display for FileTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Copy fields to avoid unaligned reference
        let file_id = self.file_id;
        let block_id = self.block_id;
        write!(f, "FileTag({}:{})", file_id, block_id)
    }
}

// 编译时验证大小
const _: () = assert!(std::mem::size_of::<FileTag>() == 6);
const _: () = assert!(std::mem::align_of::<FileTag>() == 1);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_tag_size() {
        assert_eq!(std::mem::size_of::<FileTag>(), 6);
        assert_eq!(std::mem::align_of::<FileTag>(), 1);
    }

    #[test]
    fn test_file_tag_new() {
        let tag = FileTag::new(1, 100);
        // Copy fields to local before assertion (packed struct)
        let file_id = tag.file_id;
        let block_id = tag.block_id;
        assert_eq!(file_id, 1);
        assert_eq!(block_id, 100);
        assert!(tag.is_valid());
    }

    #[test]
    fn test_file_tag_eq() {
        let a = FileTag::new(1, 100);
        let b = FileTag::new(1, 100);
        let c = FileTag::new(2, 100);

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_file_tag_default_display() {
        let ft: FileTag = Default::default();
        let _ = format!("{:?}", ft);
        let ft2 = FileTag::new(0, 0);
        assert_eq!(ft, ft2);
    }
}
