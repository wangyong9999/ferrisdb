//! 堆页面定义
//!
//! 存储表数据的堆页面布局。

use super::{PageHeader, PageType, PAGE_SIZE};
use ferrisdb_core::Xid;

/// 堆页面头部（在 PageHeader 之后）
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct HeapPageHeader {
    /// 通用页面头部
    pub base: PageHeader,
    /// 空闲空间映射
    pub pd_prune_xid: Xid,
    /// 行指针数量
    pub pd_linp_count: u16,
    /// 填充
    pub pd_padding: [u8; 6],
}

impl HeapPageHeader {
    /// 创建新的堆页面头部
    #[inline]
    pub const fn new() -> Self {
        Self {
            base: PageHeader::new(PageType::Heap),
            pd_prune_xid: Xid::INVALID,
            pd_linp_count: 0,
            pd_padding: [0; 6],
        }
    }
}

impl Default for HeapPageHeader {
    fn default() -> Self {
        Self::new()
    }
}

/// 行指针（6 字节）
///
/// 指向页面内的元组数据。
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, packed)]
pub struct ItemIdData {
    /// 元组偏移量
    pub lp_off: u16,
    /// 元组标志（LP_NORMAL, LP_REDIRECT, LP_DEAD）
    pub lp_flags: u8,
    /// 元组长度
    pub lp_len: u16,
}

/// 行指针标志
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ItemIdFlags {
    /// 未使用
    Unused = 0,
    /// 正常
    Normal = 1,
    /// 重定向
    Redirect = 2,
    /// 已删除
    Dead = 3,
}

impl ItemIdData {
    /// 创建新的行指针
    #[inline]
    pub const fn new(offset: u16, len: u16) -> Self {
        Self {
            lp_off: offset,
            lp_flags: ItemIdFlags::Normal as u8,
            lp_len: len,
        }
    }

    /// 检查是否正常
    #[inline]
    pub fn is_normal(&self) -> bool {
        self.lp_flags == ItemIdFlags::Normal as u8
    }

    /// 检查是否已删除
    #[inline]
    pub fn is_dead(&self) -> bool {
        self.lp_flags == ItemIdFlags::Dead as u8
    }
}

/// 元组指针（6 字节）
///
/// 标识一个元组的位置。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(C, packed)]
pub struct ItemPointerData {
    /// 块号
    pub ip_blkid: u32,
    /// 偏移号
    pub ip_posid: u16,
}

impl ItemPointerData {
    /// 创建新的元组指针
    #[inline]
    pub const fn new(block: u32, offset: u16) -> Self {
        Self {
            ip_blkid: block,
            ip_posid: offset,
        }
    }

    /// 无效元组指针
    pub const INVALID: Self = Self::new(0, 0);

    /// 检查是否有效
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.ip_blkid != 0 || self.ip_posid != 0
    }
}

impl Default for ItemPointerData {
    fn default() -> Self {
        Self::INVALID
    }
}

/// 堆元组头部（23 字节 + 填充）
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct HeapTupleHeader {
    /// 事务 ID (插入)
    pub t_xmin: Xid,
    /// 事务 ID (删除/更新)
    pub t_xmax: Xid,
    /// 命令 ID
    pub t_cid: u32,
    /// 元组指针
    pub t_ctid: ItemPointerData,
    /// 信息标志
    pub t_infomask: u16,
    /// 信息标志2
    pub t_infomask2: u16,
    /// 偏移到 null bitmap
    pub t_hoff: u8,
    /// 填充
    pub t_padding: [u8; 3],
}

/// 堆页面操作 trait
pub trait HeapPageOps {
    /// 初始化页面
    fn init(&mut self);
    /// 插入元组，返回偏移号
    fn insert_tuple(&mut self, data: &[u8]) -> Option<u16>;
    /// 获取元组数据
    fn get_tuple(&self, offset: u16) -> Option<&[u8]>;
    /// 获取元组数据（可变）
    fn get_tuple_mut(&mut self, offset: u16) -> Option<&mut [u8]>;
    /// 标记元组为已删除
    fn mark_dead(&mut self, offset: u16);
    /// 获取行指针数量
    fn linp_count(&self) -> u16;
    /// 获取空闲空间大小
    fn free_space(&self) -> u16;
}

/// 堆页面结构
///
/// 页面布局：
/// ```text
/// +------------------+
/// | PageHeader (24B) |
/// +------------------+
/// | HeapPageHeader   |
/// +------------------+
/// | ItemIdData[]     | <- pd_lower
/// +------------------+
/// |    free space    |
/// +------------------+
/// | TupleData[]      | <- pd_upper
/// +------------------+
/// | Special Space    | <- pd_special
/// +------------------+
/// ```
#[repr(C, align(8192))]
pub struct HeapPage {
    /// 页面数据
    pub data: [u8; PAGE_SIZE],
}

impl HeapPage {
    /// 创建新的堆页面
    #[inline]
    pub const fn new() -> Self {
        Self {
            data: [0u8; PAGE_SIZE],
        }
    }

    /// 从字节数组创建
    #[inline]
    pub fn from_bytes(data: &mut [u8]) -> &mut Self {
        debug_assert_eq!(data.len(), PAGE_SIZE);
        // SAFETY: HeapPage is #[repr(C, align(8192))] and size is PAGE_SIZE
        unsafe { &mut *(data.as_mut_ptr() as *mut HeapPage) }
    }

    /// 获取页面头部
    #[inline]
    pub fn header(&self) -> &HeapPageHeader {
        // SAFETY: HeapPage is aligned to PAGE_SIZE and HeapPageHeader is at offset 0
        unsafe { &*(self.data.as_ptr() as *const HeapPageHeader) }
    }

    /// 获取页面头部（可变）
    #[inline]
    pub fn header_mut(&mut self) -> &mut HeapPageHeader {
        // SAFETY: HeapPage is aligned to PAGE_SIZE and HeapPageHeader is at offset 0
        unsafe { &mut *(self.data.as_mut_ptr() as *mut HeapPageHeader) }
    }

    /// 获取行指针数组
    #[inline]
    pub fn item_ids(&self) -> &[ItemIdData] {
        let header = self.header();
        let count = header.pd_linp_count as usize;
        if count == 0 {
            return &[];
        }
        // SAFETY: ItemIdData array starts after HeapPageHeader
        unsafe {
            let start = self.data.as_ptr().add(std::mem::size_of::<HeapPageHeader>());
            std::slice::from_raw_parts(start as *const ItemIdData, count)
        }
    }

    /// 获取行指针（可变）
    #[inline]
    fn item_id_mut(&mut self, index: u16) -> Option<&mut ItemIdData> {
        let header = self.header();
        if index >= header.pd_linp_count {
            return None;
        }
        // SAFETY: ItemIdData array starts after HeapPageHeader
        unsafe {
            let start = self.data.as_mut_ptr().add(std::mem::size_of::<HeapPageHeader>());
            Some(&mut *(start as *mut ItemIdData).add(index as usize))
        }
    }

    /// 获取行指针
    #[inline]
    pub fn item_id(&self, index: u16) -> Option<&ItemIdData> {
        let header = self.header();
        if index >= header.pd_linp_count {
            return None;
        }
        // SAFETY: ItemIdData array starts after HeapPageHeader
        unsafe {
            let start = self.data.as_ptr().add(std::mem::size_of::<HeapPageHeader>());
            Some(&*(start as *const ItemIdData).add(index as usize))
        }
    }

    /// 获取空闲空间大小
    #[inline]
    pub fn free_space(&self) -> u16 {
        let header = self.header();
        header.base.pd_upper.saturating_sub(header.base.pd_lower)
    }

    /// 检查是否有足够的空闲空间
    #[inline]
    pub fn has_free_space(&self, needed: u16) -> bool {
        self.free_space() >= needed
    }

    /// 初始化页面
    #[inline]
    pub fn init(&mut self) {
        // 清零数据
        self.data.fill(0);

        // 初始化头部
        let header = self.header_mut();
        *header = HeapPageHeader::new();

        // 设置 pd_lower 和 pd_upper
        let header_size = std::mem::size_of::<HeapPageHeader>() as u16;
        header.base.pd_lower = header_size;
        header.base.pd_upper = PAGE_SIZE as u16;
    }

    /// 插入元组（直接写入头部和数据，避免中间分配）
    ///
    /// 返回元组偏移号（行指针索引，从 1 开始）
    pub fn insert_tuple_from_parts(&mut self, header: &[u8], user_data: &[u8]) -> Option<u16> {
        let tuple_size = (header.len() + user_data.len()) as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let total_needed = tuple_size + item_id_size;

        // 检查空闲空间
        if !self.has_free_space(total_needed) {
            return None;
        }

        let (old_lower, old_upper, old_linp_count) = {
            let hdr = self.header();
            (hdr.base.pd_lower, hdr.base.pd_upper, hdr.pd_linp_count)
        };

        let new_upper = old_upper - tuple_size;
        let new_lower = old_lower + item_id_size;

        if new_lower > new_upper {
            return None;
        }

        // 写入元组数据（header + user_data）
        let tuple_offset = new_upper as usize;
        self.data[tuple_offset..tuple_offset + header.len()].copy_from_slice(header);
        self.data[tuple_offset + header.len()..tuple_offset + header.len() + user_data.len()].copy_from_slice(user_data);

        // 添加行指针
        let linp_index = old_linp_count;
        let linp_offset = std::mem::size_of::<HeapPageHeader>() + linp_index as usize * std::mem::size_of::<ItemIdData>();

        let item_id = ItemIdData::new(new_upper, tuple_size);
        unsafe {
            let ptr = self.data.as_mut_ptr().add(linp_offset) as *mut ItemIdData;
            std::ptr::write_unaligned(ptr, item_id);
        }

        let new_linp_count = old_linp_count + 1;
        {
            let hdr = self.header_mut();
            hdr.base.pd_lower = new_lower;
            hdr.base.pd_upper = new_upper;
            hdr.pd_linp_count = new_linp_count;
        }

        Some(new_linp_count)
    }

    /// 插入元组
    ///
    /// 返回元组偏移号（行指针索引，从 1 开始）
    pub fn insert_tuple(&mut self, data: &[u8]) -> Option<u16> {
        let tuple_size = data.len() as u16;
        let item_id_size = std::mem::size_of::<ItemIdData>() as u16;
        let total_needed = tuple_size + item_id_size;

        // 检查空闲空间
        if !self.has_free_space(total_needed) {
            return None;
        }

        // 先读取当前头部值
        let (old_lower, old_upper, old_linp_count) = {
            let header = self.header();
            (header.base.pd_lower, header.base.pd_upper, header.pd_linp_count)
        };

        // 计算新的 pd_upper（元组从后往前放）
        let new_upper = old_upper - tuple_size;

        // 计算新的 pd_lower（行指针从前往后放）
        let new_lower = old_lower + item_id_size;

        // 检查是否交叉
        if new_lower > new_upper {
            return None;
        }

        // 写入元组数据
        let tuple_offset = new_upper as usize;
        self.data[tuple_offset..tuple_offset + data.len()].copy_from_slice(data);

        // 添加行指针
        let linp_index = old_linp_count;
        let linp_offset = std::mem::size_of::<HeapPageHeader>() + linp_index as usize * std::mem::size_of::<ItemIdData>();

        let item_id = ItemIdData::new(new_upper, tuple_size);
        // SAFETY: we checked bounds above
        unsafe {
            let ptr = self.data.as_mut_ptr().add(linp_offset) as *mut ItemIdData;
            std::ptr::write_unaligned(ptr, item_id);
        }

        // 更新头部
        let new_linp_count = old_linp_count + 1;
        {
            let header = self.header_mut();
            header.base.pd_lower = new_lower;
            header.base.pd_upper = new_upper;
            header.pd_linp_count = new_linp_count;
        }

        // 返回偏移号（从 1 开始，0 表示无效）
        Some(new_linp_count)
    }

    /// 获取元组数据
    pub fn get_tuple(&self, offset: u16) -> Option<&[u8]> {
        if offset == 0 {
            return None;
        }

        let item_id = self.item_id(offset - 1)?;
        if !item_id.is_normal() {
            return None;
        }

        let start = item_id.lp_off as usize;
        let len = item_id.lp_len as usize;

        if start + len > PAGE_SIZE {
            return None;
        }

        Some(&self.data[start..start + len])
    }

    /// 获取元组数据（可变）
    pub fn get_tuple_mut(&mut self, offset: u16) -> Option<&mut [u8]> {
        if offset == 0 {
            return None;
        }

        let item_id = self.item_id(offset - 1)?;
        if !item_id.is_normal() {
            return None;
        }

        let start = item_id.lp_off as usize;
        let len = item_id.lp_len as usize;

        if start + len > PAGE_SIZE {
            return None;
        }

        Some(&mut self.data[start..start + len])
    }

    /// 标记元组为已删除
    pub fn mark_dead(&mut self, offset: u16) {
        if offset == 0 {
            return;
        }

        if let Some(item_id) = self.item_id_mut(offset - 1) {
            item_id.lp_flags = ItemIdFlags::Dead as u8;
        }
    }

    /// 回收所有 Dead 元组占用的数据空间。
    ///
    /// 将存活元组的数据向页面末尾紧缩，更新各自的 lp_off。
    /// **不改变元组的索引位置**（lp_posid 不变），所以外部 TupleId 仍然有效。
    /// 返回回收的字节数。
    pub fn compact_dead(&mut self) -> usize {
        let linp_count = self.header().pd_linp_count as usize;
        if linp_count == 0 {
            return 0;
        }

        // 收集存活元组信息：(index, old_off, len)
        let mut surviving: Vec<(usize, u16, u16)> = Vec::with_capacity(linp_count);
        let mut dead_data_bytes: usize = 0;

        for i in 0..linp_count {
            if let Some(item) = self.item_id(i as u16) {
                if item.lp_flags == ItemIdFlags::Dead as u8 || item.lp_flags == ItemIdFlags::Unused as u8 {
                    dead_data_bytes += item.lp_len as usize;
                    continue;
                }
                surviving.push((i, item.lp_off, item.lp_len));
            }
        }

        if dead_data_bytes == 0 {
            return 0;
        }

        // 将存活元组按 old_off 降序排列（从页面末尾开始紧缩）
        surviving.sort_by(|a, b| b.1.cmp(&a.1));

        // 从页面末尾开始，紧密排列存活元组
        let mut write_pos = PAGE_SIZE as u16;
        for &(idx, old_off, len) in &surviving {
            let new_off = write_pos - len;
            if new_off != old_off {
                // 移动数据
                self.data.copy_within(
                    old_off as usize..old_off as usize + len as usize,
                    new_off as usize,
                );
            }
            // 更新行指针的偏移
            if let Some(item) = self.item_id_mut(idx as u16) {
                item.lp_off = new_off;
            }
            write_pos = new_off;
        }

        // 更新 pd_upper
        self.header_mut().base.pd_upper = write_pos;

        dead_data_bytes
    }

    /// 获取行指针数量
    #[inline]
    pub fn linp_count(&self) -> u16 {
        self.header().pd_linp_count
    }

    /// 获取字节数组引用
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.data
    }

    /// 获取字节数组引用（可变）
    #[inline]
    pub fn as_bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// 清理页面中已被标记为删除/更新的元组（xmax != 0）。
    ///
    /// 回收死元组占用的空间，重建行指针数组。
    /// 返回回收的字节数。
    pub fn prune_dead_tuples(&mut self) -> usize {
        let linp_count = self.header().pd_linp_count as usize;
        if linp_count == 0 {
            return 0;
        }

        let header_size = std::mem::size_of::<HeapPageHeader>();
        let item_id_size = std::mem::size_of::<ItemIdData>();

        // 收集存活元组信息：(在原页面的偏移, 长度)
        let mut surviving: Vec<(u16, u16)> = Vec::with_capacity(linp_count);

        for i in 0..linp_count {
            let item = self.item_id(i as u16);
            if let Some(id) = item {
                if !id.is_normal() {
                    continue;
                }
                let off = id.lp_off as usize;
                let len = id.lp_len as usize;

                // 检查 xmax（TupleHeader 的 bytes [8..16]）
                // 如果 xmax != 0（有任何非零字节），说明已被更新/删除
                if off + 16 <= PAGE_SIZE && len >= 16 {
                    let xmax_bytes = &self.data[off + 8..off + 16];
                    let xmax_nonzero = xmax_bytes.iter().any(|&b| b != 0);
                    if xmax_nonzero {
                        continue; // 跳过已删除的元组
                    }
                }

                surviving.push((id.lp_off, id.lp_len));
            }
        }

        if surviving.len() == linp_count {
            return 0; // 没有需要清理的
        }

        let reclaimed = (linp_count - surviving.len()) * item_id_size;

        // 从页面末尾开始重新写入存活的元组数据
        // 先收集所有存活元组的数据到临时 buffer
        let mut tuple_bufs: Vec<Vec<u8>> = Vec::with_capacity(surviving.len());
        for &(off, len) in &surviving {
            let off = off as usize;
            let len = len as usize;
            if off + len <= PAGE_SIZE {
                tuple_bufs.push(self.data[off..off + len].to_vec());
            }
        }

        // 重新初始化页面（保留 header 基本信息）
        let old_prune_xid = self.header().pd_prune_xid;
        self.init();
        self.header_mut().pd_prune_xid = old_prune_xid;

        // 重新插入所有存活元组
        for buf in &tuple_bufs {
            self.insert_tuple(buf);
        }

        reclaimed
    }
}

impl Default for HeapPage {
    fn default() -> Self {
        let mut page = Self::new();
        // 初始化头部
        let header = page.header_mut();
        *header = HeapPageHeader::new();
        page
    }
}

// 编译时验证
const _: () = assert!(std::mem::size_of::<HeapPage>() == PAGE_SIZE);
// ItemIdData: packed struct, u16 + u8 + u16 = 5 bytes
const _: () = assert!(std::mem::size_of::<ItemIdData>() == 5);
// ItemPointerData: packed struct, u32 + u16 = 6 bytes
const _: () = assert!(std::mem::size_of::<ItemPointerData>() == 6);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_heap_page_size() {
        assert_eq!(std::mem::size_of::<HeapPage>(), PAGE_SIZE);
    }

    #[test]
    fn test_item_id_data_size() {
        assert_eq!(std::mem::size_of::<ItemIdData>(), 5);
    }

    #[test]
    fn test_item_pointer_data_size() {
        assert_eq!(std::mem::size_of::<ItemPointerData>(), 6);
    }

    #[test]
    fn test_heap_page_new() {
        let page = HeapPage::default();
        let header = page.header();
        assert_eq!(header.base.page_type(), PageType::Heap);
        assert!(header.base.free_space() > 0);
    }
}
