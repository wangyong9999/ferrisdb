//! Page 头部定义
//!
//! 定义页面的通用头部结构。

use ferrisdb_core::Lsn;
use std::fmt;

/// 页面大小（8KB）
pub const PAGE_SIZE: usize = 8192;

/// 页面掩码
pub const PAGE_MASK: usize = PAGE_SIZE - 1;

/// 页号类型
pub type PageNo = u32;

/// 页面 ID
pub type PageId = ferrisdb_core::PageId;

/// 页面类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageType {
    /// 未使用
    Unused = 0,
    /// 堆页面
    Heap = 1,
    /// B-Tree 索引页面
    BtreeIndex = 2,
    /// Undo 页面
    Undo = 3,
    /// FSM 页面
    Fsm = 4,
    /// 回收队列页面
    RecycleQueue = 5,
}

impl Default for PageType {
    fn default() -> Self {
        Self::Unused
    }
}

/// 页面头部（24 字节）
///
/// 所有页面类型的通用头部。
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct PageHeader {
    /// 页面 LSN（用于 WAL 协议）
    pub lsn: Lsn,
    /// 空闲空间起始偏移
    pub pd_lower: u16,
    /// 空闲空间结束偏移
    pub pd_upper: u16,
    /// 特殊区域起始偏移
    pub pd_special: u16,
    /// 页面类型
    pub pd_type: u8,
    /// 页面版本
    pub pd_version: u8,
    /// 标志位
    pub pd_flags: u16,
    /// 校验和（可选）
    pub pd_checksum: u16,
    /// 页面 magic（用于 torn page 检测）
    pub pd_magic: u16,
}

/// 页面 magic number — 用于区分有效页面和 torn/未初始化页面
pub const PAGE_MAGIC: u16 = 0xD570;

impl PageHeader {
    /// 创建新的页面头部
    #[inline]
    pub const fn new(page_type: PageType) -> Self {
        Self {
            lsn: Lsn::INVALID,
            pd_lower: std::mem::size_of::<Self>() as u16,
            pd_upper: PAGE_SIZE as u16,
            pd_special: PAGE_SIZE as u16,
            pd_type: page_type as u8,
            pd_version: 1,
            pd_flags: 0,
            pd_checksum: 0,
            pd_magic: PAGE_MAGIC,
        }
    }

    /// 获取页面类型
    #[inline]
    pub fn page_type(&self) -> PageType {
        match self.pd_type {
            0 => PageType::Unused,
            1 => PageType::Heap,
            2 => PageType::BtreeIndex,
            3 => PageType::Undo,
            4 => PageType::Fsm,
            5 => PageType::RecycleQueue,
            _ => PageType::Unused,
        }
    }

    /// 获取空闲空间大小
    #[inline]
    pub fn free_space(&self) -> u16 {
        self.pd_upper.saturating_sub(self.pd_lower)
    }

    /// 检查是否有足够的空闲空间
    #[inline]
    pub fn has_free_space(&self, needed: u16) -> bool {
        self.free_space() >= needed
    }

    /// 检查页面是否有效（magic number 正确）
    ///
    /// 全零页面视为未初始化（有效），非零页面必须有正确的 magic。
    pub fn is_valid_page(page_data: &[u8]) -> bool {
        if page_data.len() < PAGE_SIZE {
            return false;
        }
        // magic 在 PageHeader 末尾 2 字节（offset = size_of::<PageHeader>() - 2）
        let magic_offset = std::mem::size_of::<Self>() - 2;
        let magic = u16::from_le_bytes([page_data[magic_offset], page_data[magic_offset + 1]]);
        magic == PAGE_MAGIC || magic == 0 // 0 = 未初始化/全零
    }

    /// 计算页面 checksum（CRC32 截断为 u16）
    ///
    /// 对整个页面数据计算 CRC32，然后折叠为 16 位。
    /// 计算时 pd_checksum 字段位置用 0 填充。
    pub fn compute_checksum(page_data: &[u8]) -> u16 {
        if page_data.len() < PAGE_SIZE {
            return 0;
        }
        let mut hasher = crc32fast::Hasher::new();
        // checksum 字段在 PageHeader 中的偏移：8(lsn) + 2+2+2+1+1+2 = 18, 大小 2
        let checksum_offset = 18;
        hasher.update(&page_data[..checksum_offset]);
        hasher.update(&[0u8; 2]); // checksum 位置用 0
        hasher.update(&page_data[checksum_offset + 2..]);
        let crc = hasher.finalize();
        // 折叠 32 位为 16 位（XOR 高低 16 位）
        ((crc >> 16) ^ (crc & 0xFFFF)) as u16
    }

    /// 设置页面 checksum
    pub fn set_checksum(page_data: &mut [u8]) {
        let checksum = Self::compute_checksum(page_data);
        let checksum_offset = 18;
        if page_data.len() > checksum_offset + 2 {
            page_data[checksum_offset..checksum_offset + 2].copy_from_slice(&checksum.to_le_bytes());
        }
    }

    /// 验证页面 checksum
    ///
    /// 返回 true 如果 checksum 正确或页面全零（未初始化）。
    pub fn verify_checksum(page_data: &[u8]) -> bool {
        if page_data.len() < PAGE_SIZE {
            return false;
        }
        // 全零页面视为有效（未初始化）
        let checksum_offset = 18;
        let stored = u16::from_le_bytes([page_data[checksum_offset], page_data[checksum_offset + 1]]);
        if stored == 0 {
            // 检查是否全零页面
            let is_zero = page_data.iter().all(|&b| b == 0);
            if is_zero {
                return true;
            }
        }
        let computed = Self::compute_checksum(page_data);
        stored == computed
    }
}

impl Default for PageHeader {
    fn default() -> Self {
        Self::new(PageType::Unused)
    }
}

// 编译时验证
const _: () = assert!(std::mem::size_of::<PageHeader>() == 24);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_page_header_size() {
        assert_eq!(std::mem::size_of::<PageHeader>(), 24);
    }

    #[test]
    fn test_page_header_new() {
        let header = PageHeader::new(PageType::Heap);
        assert_eq!(header.page_type(), PageType::Heap);
        assert!(header.free_space() > 0);
    }

    #[test]
    fn test_page_constants() {
        assert_eq!(PAGE_SIZE, 8192);
        assert_eq!(PAGE_MASK, 8191);
    }
}
