//! 缓冲标签定义

use std::fmt;
use std::hash::{Hash, Hasher};

use super::{FileTag, PdbId};

/// 缓冲标签
///
/// 唯一标识一个缓冲区页面，大小为 12 字节 packed。
///
/// # C++ 参考
/// `include/buffer/dstore_buf.h` 中的 BufferTag
///
/// # 布局
/// ```text
/// Offset 0-5:   FileTag (file_id: u16, block_id: u32)
/// Offset 6-7:   padding (u16)
/// Offset 8-11:  pdb_id (u32)
/// ```
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, packed)]
pub struct BufferTag {
    /// 文件标识 (6 bytes)
    pub file_tag: FileTag,
    /// 填充 (2 bytes)
    pub padding: u16,
    /// PDB ID (4 bytes)
    pub pdb_id: PdbId,
}

// 手动实现 Hash 因为是 packed struct
impl Hash for BufferTag {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Copy fields to avoid unaligned reference
        let file_id = self.file_tag.file_id;
        let block_id = self.file_tag.block_id;
        let pdb_id = self.pdb_id; // Copy entire struct first
        let pdb_raw = pdb_id.raw();
        file_id.hash(state);
        block_id.hash(state);
        pdb_raw.hash(state);
    }
}

impl PartialEq for BufferTag {
    fn eq(&self, other: &Self) -> bool {
        // 安全访问 packed struct - copy fields first
        let self_file_id = self.file_tag.file_id;
        let self_block_id = self.file_tag.block_id;
        let self_pdb = self.pdb_id;

        let other_file_id = other.file_tag.file_id;
        let other_block_id = other.file_tag.block_id;
        let other_pdb = other.pdb_id;

        self_file_id == other_file_id
            && self_block_id == other_block_id
            && self_pdb == other_pdb
    }
}

impl Eq for BufferTag {}

impl BufferTag {
    /// 创建新的缓冲标签
    #[inline]
    pub const fn new(pdb_id: PdbId, file_id: u16, block_id: u32) -> Self {
        Self {
            file_tag: FileTag::new(file_id, block_id),
            padding: 0,
            pdb_id,
        }
    }

    /// 无效缓冲标签
    pub const INVALID: Self = Self::new(PdbId::INVALID, 0, 0);

    /// 是否有效
    #[inline]
    pub fn is_valid(&self) -> bool {
        // Copy structs to avoid unaligned reference
        let pdb = self.pdb_id;
        let file = self.file_tag;
        pdb.is_valid() || file.is_valid()
    }

    /// 获取文件 ID
    #[inline]
    pub const fn file_id(&self) -> u16 {
        self.file_tag.file_id
    }

    /// 获取块号
    #[inline]
    pub const fn block_id(&self) -> u32 {
        self.file_tag.block_id
    }

    /// 计算 hash 值（用于 hash table）
    #[inline]
    pub fn hash_value(&self) -> u64 {
        // Copy fields to avoid unaligned reference
        let file_id = self.file_tag.file_id;
        let block_id = self.file_tag.block_id;
        let pdb_id = self.pdb_id;
        let pdb_raw = pdb_id.raw();

        // FxHash 风格的 hash
        let mut hash = 0u64;

        // 文件 ID
        hash = (hash.rotate_left(5) ^ file_id as u64).wrapping_mul(0x517cc1b727220a95);

        // 块 ID
        hash = (hash.rotate_left(5) ^ block_id as u64).wrapping_mul(0x517cc1b727220a95);

        // PDB ID
        hash = (hash.rotate_left(5) ^ pdb_raw as u64).wrapping_mul(0x517cc1b727220a95);

        hash
    }

    /// 序列化为字节数组
    #[inline]
    pub fn to_bytes(&self) -> [u8; 12] {
        // Copy fields to avoid unaligned reference
        let file_id = self.file_tag.file_id;
        let block_id = self.file_tag.block_id;
        let pdb_id = self.pdb_id;
        let pdb_raw = pdb_id.raw();

        let mut bytes = [0u8; 12];
        bytes[0..2].copy_from_slice(&file_id.to_le_bytes());
        bytes[2..6].copy_from_slice(&block_id.to_le_bytes());
        bytes[6..8].copy_from_slice(&self.padding.to_le_bytes());
        bytes[8..12].copy_from_slice(&pdb_raw.to_le_bytes());
        bytes
    }

    /// 从字节数组反序列化
    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= 12, "BufferTag requires at least 12 bytes");

        let file_id = u16::from_le_bytes([bytes[0], bytes[1]]);
        let block_id = u32::from_le_bytes([bytes[2], bytes[3], bytes[4], bytes[5]]);
        let padding = u16::from_le_bytes([bytes[6], bytes[7]]);
        let pdb_raw = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);

        Self {
            file_tag: FileTag { file_id, block_id },
            padding,
            pdb_id: PdbId::from_raw(pdb_raw),
        }
    }
}

impl fmt::Display for BufferTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Copy structs to avoid unaligned reference
        let pdb_id = self.pdb_id;
        let file_id = self.file_tag.file_id;
        let block_id = self.file_tag.block_id;
        write!(f, "BufferTag({}:{}:{})", pdb_id.raw(), file_id, block_id)
    }
}

// 编译时验证
const _: () = assert!(std::mem::size_of::<BufferTag>() == 12);
const _: () = assert!(std::mem::align_of::<BufferTag>() == 1);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_tag_size() {
        assert_eq!(std::mem::size_of::<BufferTag>(), 12);
        assert_eq!(std::mem::align_of::<BufferTag>(), 1);
    }

    #[test]
    fn test_buffer_tag_new() {
        let tag = BufferTag::new(PdbId::new(1), 100, 200);
        // Copy to local before assertion
        let pdb = tag.pdb_id;
        assert_eq!(pdb.raw(), 1);
        assert_eq!(tag.file_id(), 100);
        assert_eq!(tag.block_id(), 200);
        assert!(tag.is_valid());
    }

    #[test]
    fn test_buffer_tag_eq() {
        let a = BufferTag::new(PdbId::new(1), 100, 200);
        let b = BufferTag::new(PdbId::new(1), 100, 200);
        let c = BufferTag::new(PdbId::new(2), 100, 200);

        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_buffer_tag_hash() {
        let tag = BufferTag::new(PdbId::new(1), 100, 200);
        let hash = tag.hash_value();
        assert_ne!(hash, 0);

        // 相同的 tag 应该有相同的 hash
        let tag2 = BufferTag::new(PdbId::new(1), 100, 200);
        assert_eq!(tag.hash_value(), tag2.hash_value());
    }

    #[test]
    fn test_buffer_tag_bytes() {
        let tag = BufferTag::new(PdbId::new(1), 100, 200);
        let bytes = tag.to_bytes();
        assert_eq!(bytes.len(), 12);

        let decoded = BufferTag::from_bytes(&bytes);
        assert_eq!(tag, decoded);
    }
}
