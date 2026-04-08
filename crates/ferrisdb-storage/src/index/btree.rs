//! B-Tree 索引实现（B+Tree）
//!
//! 提供键值有序存储和快速查找的 B+Tree 索引。
//! 支持正确的页面分裂、叶子链表扫描、前缀扫描。

#![allow(missing_docs)]

use ferrisdb_core::{BufferTag, FerrisDBError, PdbId};
use ferrisdb_core::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;

use crate::buffer::BufferPool;
use crate::page::{PageHeader, PageId, PAGE_SIZE};
use crate::wal::{WalWriter, WalRecordHeader, WalRecordType, WalRecordForPage};

/// B-Tree 页面类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BTreePageType {
    /// 叶子节点
    Leaf = 1,
    /// 内部节点
    Internal = 2,
    /// 元页面
    Meta = 3,
}

/// B-Tree 页面头部（在 PageHeader 之后）
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BTreePageHeader {
    /// 通用页面头部
    pub base: PageHeader,
    /// 页面层级（0 = leaf）
    pub level: u32,
    /// 页面类型
    pub page_type: u8,
    /// 键数量
    pub nkeys: u32,
    /// 键值数据区的上界（从 PAGE_SIZE 向下生长）
    pub max_offset: u16,
    /// 索引区的下界（从 header 尾部向下生长）
    pub min_offset: u16,
    /// 右兄弟页号（叶子链表）
    pub right_link: u32,
    /// 最左子节点（仅内部节点使用，叶子节点为 INVALID）
    pub first_child: u32,
    /// 填充
    pub _padding: [u8; 2],
}

impl BTreePageHeader {
    /// 创建新的 B-Tree 页面头部
    #[inline]
    pub const fn new(page_type: BTreePageType, level: u32) -> Self {
        let header_size = std::mem::size_of::<BTreePageHeader>();
        Self {
            base: PageHeader::new(crate::page::PageType::BtreeIndex),
            level,
            page_type: page_type as u8,
            nkeys: 0,
            max_offset: PAGE_SIZE as u16,
            min_offset: header_size as u16,
            right_link: u32::MAX,
            first_child: u32::MAX,
            _padding: [0; 2],
        }
    }

    /// 索引区起始位置
    #[inline]
    fn index_start(&self) -> usize {
        std::mem::size_of::<BTreePageHeader>()
    }
}

const INVALID_PAGE: u32 = u32::MAX;

/// B-Tree 键（变长）
#[derive(Debug, Clone)]
pub struct BTreeKey {
    pub data: Vec<u8>,
}

impl BTreeKey {
    #[inline]
    pub fn new(data: Vec<u8>) -> Self {
        Self { data }
    }

    #[inline]
    pub fn from_bytes(bytes: &[u8]) -> Self {
        Self { data: bytes.to_vec() }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    #[inline]
    pub fn serialized_size(&self) -> usize {
        2 + self.data.len()
    }

    #[inline]
    pub fn compare(&self, other: &BTreeKey) -> std::cmp::Ordering {
        self.data.cmp(&other.data)
    }

    /// 检查是否以 prefix 开头
    #[inline]
    pub fn starts_with(&self, prefix: &[u8]) -> bool {
        self.data.starts_with(prefix)
    }
}

impl PartialEq for BTreeKey {
    fn eq(&self, other: &Self) -> bool { self.data == other.data }
}
impl Eq for BTreeKey {}
impl PartialOrd for BTreeKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.data.cmp(&other.data)) }
}
impl Ord for BTreeKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.data.cmp(&other.data) }
}

/// B-Tree 值
#[derive(Debug, Clone, Copy)]
pub enum BTreeValue {
    /// 内部节点：子页面号
    Child(u32),
    /// 叶子节点：元组指针
    Tuple { block: u32, offset: u16 },
}

impl BTreeValue {
    /// 从叶子值创建 TupleId
    pub fn as_tuple_id(&self) -> Option<(u32, u16)> {
        match self {
            BTreeValue::Tuple { block, offset } => Some((*block, *offset)),
            _ => None,
        }
    }
}

/// B-Tree 项（键 + 值）
#[derive(Debug, Clone)]
pub struct BTreeItem {
    pub key: BTreeKey,
    pub value: BTreeValue,
}

impl BTreeItem {
    pub fn serialized_size(&self) -> usize {
        self.key.serialized_size() + match &self.value {
            BTreeValue::Child(_) => 4,
            BTreeValue::Tuple { .. } => 6,
        }
    }

    /// 序列化为字节（用于 WAL 记录）
    /// 格式: [key_len:2][key_data][value_type:1][value_data]
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(3 + self.key.data.len() + 6);
        buf.extend_from_slice(&(self.key.data.len() as u16).to_le_bytes());
        buf.extend_from_slice(&self.key.data);
        match &self.value {
            BTreeValue::Child(page) => {
                buf.push(0); // type = Child
                buf.extend_from_slice(&page.to_le_bytes());
            }
            BTreeValue::Tuple { block, offset } => {
                buf.push(1); // type = Tuple
                buf.extend_from_slice(&block.to_le_bytes());
                buf.extend_from_slice(&offset.to_le_bytes());
            }
        }
        buf
    }

    /// 从字节反序列化
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 3 {
            return None;
        }
        let key_len = u16::from_le_bytes([data[0], data[1]]) as usize;
        if data.len() < 2 + key_len + 1 {
            return None;
        }
        let key = BTreeKey::new(data[2..2 + key_len].to_vec());
        let vtype = data[2 + key_len];
        let vdata = &data[2 + key_len + 1..];
        let value = match vtype {
            0 if vdata.len() >= 4 => BTreeValue::Child(u32::from_le_bytes([vdata[0], vdata[1], vdata[2], vdata[3]])),
            1 if vdata.len() >= 6 => BTreeValue::Tuple {
                block: u32::from_le_bytes([vdata[0], vdata[1], vdata[2], vdata[3]]),
                offset: u16::from_le_bytes([vdata[4], vdata[5]]),
            },
            _ => return None,
        };
        Some(Self { key, value })
    }
}

/// B-Tree 页面（8KB，直接映射到 buffer pool 页面）
#[repr(C, align(8192))]
pub struct BTreePage {
    pub data: [u8; PAGE_SIZE],
}

impl BTreePage {
    #[inline]
    pub const fn new() -> Self {
        Self { data: [0u8; PAGE_SIZE] }
    }

    #[inline]
    /// 从字节切片创建（用于 recovery）
    pub fn from_bytes(data: &mut [u8]) -> &mut Self {
        debug_assert!(data.len() >= PAGE_SIZE);
        unsafe { &mut *(data.as_mut_ptr() as *mut BTreePage) }
    }

    pub unsafe fn from_ptr(ptr: *mut u8) -> &'static mut Self {
        debug_assert!(!ptr.is_null());
        unsafe { &mut *(ptr as *mut BTreePage) }
    }

    #[inline]
    pub unsafe fn from_ptr_const(ptr: *mut u8) -> &'static Self {
        debug_assert!(!ptr.is_null());
        unsafe { &*(ptr as *const BTreePage) }
    }

    #[inline]
    pub fn header(&self) -> &BTreePageHeader {
        unsafe { &*(self.data.as_ptr() as *const BTreePageHeader) }
    }

    #[inline]
    pub fn header_mut(&mut self) -> &mut BTreePageHeader {
        unsafe { &mut *(self.data.as_mut_ptr() as *mut BTreePageHeader) }
    }

    /// 初始化页面
    pub fn init(&mut self, page_type: BTreePageType, level: u32) {
        self.data.fill(0);
        let header = self.header_mut();
        *header = BTreePageHeader::new(page_type, level);
    }

    #[inline]
    pub fn free_space(&self) -> u16 {
        let header = self.header();
        header.max_offset.saturating_sub(header.min_offset)
    }

    #[inline]
    pub fn has_free_space(&self, needed: u16) -> bool {
        self.free_space() >= needed
    }

    #[inline]
    pub fn is_leaf(&self) -> bool {
        self.header().page_type == BTreePageType::Leaf as u8
    }

    #[inline]
    pub fn nkeys(&self) -> u32 {
        self.header().nkeys
    }

    #[inline]
    pub fn level(&self) -> u32 {
        self.header().level
    }

    // ---- Sorted Insert ----

    /// 在页面中以有序方式插入项。
    /// 返回插入位置的索引，如果空间不足返回 None。
    pub fn insert_item_sorted(&mut self, item: &BTreeItem) -> Option<u16> {
        let item_size = item.serialized_size() as u16;
        let index_entry_size: u16 = 2; // u16 offset pointer

        if !self.has_free_space(item_size + index_entry_size) {
            return None;
        }

        let header = self.header();
        let nkeys = header.nkeys;
        let max_offset = header.max_offset;
        let min_offset = header.min_offset;

        // 计算新 item 的位置（从页面尾部向前）
        let new_item_offset = max_offset - item_size;
        let new_min_offset = min_offset + index_entry_size;

        if new_min_offset > new_item_offset {
            return None;
        }

        // 写入 item 数据
        self.write_item_at(new_item_offset as usize, item);

        // 二分查找插入位置
        let pos = self.find_insert_pos(&item.key);

        // 移动索引项（从 pos 到 nkeys-1 向右移 2 字节）
        if pos < nkeys as usize {
            let idx_base = std::mem::size_of::<BTreePageHeader>();
            let src_start = idx_base + pos * 2;
            let src_end = idx_base + nkeys as usize * 2;
            let dst_start = src_start + 2;
            // 后移
            for i in (src_start..src_end).rev() {
                self.data[i + 2] = self.data[i];
            }
        }

        // 写入新的 offset 到索引位置
        let idx_pos = std::mem::size_of::<BTreePageHeader>() + pos * 2;
        self.data[idx_pos..idx_pos + 2].copy_from_slice(&new_item_offset.to_le_bytes());

        // 更新头部
        let header = self.header_mut();
        header.nkeys = nkeys + 1;
        header.max_offset = new_item_offset;
        header.min_offset = new_min_offset;

        Some(pos as u16)
    }

    /// 二分查找 key 应该插入的位置
    fn find_insert_pos(&self, key: &BTreeKey) -> usize {
        let nkeys = self.header().nkeys as usize;
        let mut lo = 0;
        let mut hi = nkeys;
        while lo < hi {
            let mid = (lo + hi) / 2;
            if let Some(item) = self.get_item(mid as u16) {
                match item.key.compare(key) {
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater | std::cmp::Ordering::Equal => hi = mid,
                }
            } else {
                lo = mid + 1;
            }
        }
        lo
    }

    /// 在指定 offset 处写入 item
    fn write_item_at(&mut self, offset: usize, item: &BTreeItem) {
        let key_len = item.key.len() as u16;
        self.data[offset..offset + 2].copy_from_slice(&key_len.to_le_bytes());
        self.data[offset + 2..offset + 2 + item.key.len()].copy_from_slice(&item.key.data);
        let val_offset = offset + 2 + item.key.len();
        match &item.value {
            BTreeValue::Child(child) => {
                self.data[val_offset..val_offset + 4].copy_from_slice(&child.to_le_bytes());
            }
            BTreeValue::Tuple { block, offset: tuple_offset } => {
                self.data[val_offset..val_offset + 4].copy_from_slice(&block.to_le_bytes());
                self.data[val_offset + 4..val_offset + 6].copy_from_slice(&tuple_offset.to_le_bytes());
            }
        }
    }

    /// 获取指定索引位置的 item
    pub fn get_item(&self, index: u16) -> Option<BTreeItem> {
        let header = self.header();
        if index >= header.nkeys as u16 {
            return None;
        }

        let idx_pos = std::mem::size_of::<BTreePageHeader>() + index as usize * 2;
        if idx_pos + 2 > PAGE_SIZE {
            return None;
        }

        let item_offset = u16::from_le_bytes([self.data[idx_pos], self.data[idx_pos + 1]]) as usize;
        if item_offset >= PAGE_SIZE {
            return None;
        }

        // 读取 key_len
        if item_offset + 2 > PAGE_SIZE {
            return None;
        }
        let key_len = u16::from_le_bytes([self.data[item_offset], self.data[item_offset + 1]]) as usize;

        if item_offset + 2 + key_len > PAGE_SIZE {
            return None;
        }

        let key = BTreeKey::from_bytes(&self.data[item_offset + 2..item_offset + 2 + key_len]);

        let val_offset = item_offset + 2 + key_len;
        let value = if header.page_type == BTreePageType::Internal as u8 {
            if val_offset + 4 > PAGE_SIZE { return None; }
            let child = u32::from_le_bytes([
                self.data[val_offset], self.data[val_offset + 1],
                self.data[val_offset + 2], self.data[val_offset + 3],
            ]);
            BTreeValue::Child(child)
        } else {
            if val_offset + 6 > PAGE_SIZE { return None; }
            let block = u32::from_le_bytes([
                self.data[val_offset], self.data[val_offset + 1],
                self.data[val_offset + 2], self.data[val_offset + 3],
            ]);
            let offset = u16::from_le_bytes([
                self.data[val_offset + 4], self.data[val_offset + 5],
            ]);
            BTreeValue::Tuple { block, offset }
        };

        Some(BTreeItem { key, value })
    }

    /// 收集页面中的所有 items（按索引顺序）
    pub fn get_all_items(&self) -> Vec<BTreeItem> {
        let nkeys = self.header().nkeys;
        let mut items = Vec::with_capacity(nkeys as usize);
        for i in 0..nkeys {
            if let Some(item) = self.get_item(i as u16) {
                items.push(item);
            }
        }
        items
    }

    /// 获取最后一个 item（最大 key）
    pub fn get_last_item(&self) -> Option<BTreeItem> {
        let nkeys = self.header().nkeys;
        if nkeys == 0 { return None; }
        self.get_item(nkeys as u16 - 1)
    }

    /// 用有序 items 列表重建页面（页面必须已初始化）
    pub fn rebuild_from_sorted(&mut self, items: &[BTreeItem]) {
        let header = self.header();
        let page_type_val = header.page_type;
        let level = header.level;
        let right_link = header.right_link;
        let first_child = header.first_child;

        // 重新初始化页面
        self.init(
            if page_type_val == BTreePageType::Internal as u8 { BTreePageType::Internal } else { BTreePageType::Leaf },
            level,
        );

        // 恢复 right_link 和 first_child
        {
            let h = self.header_mut();
            h.right_link = right_link;
            h.first_child = first_child;
        }

        // 插入所有 items
        for item in items {
            if self.insert_item_sorted(item).is_none() {
                break; // 页面满了
            }
        }
    }

    /// 在内部节点中查找给定 key 应该进入的子节点页号
    pub fn find_child(&self, key: &BTreeKey) -> u32 {
        let header = self.header();
        let nkeys = header.nkeys;
        let first_child = header.first_child;

        // 从后向前找第一个 key <= search_key 的 item
        for i in (0..nkeys).rev() {
            if let Some(item) = self.get_item(i as u16) {
                if item.key.compare(key) != std::cmp::Ordering::Greater {
                    // key_i <= search_key, go to child_i
                    if let BTreeValue::Child(child) = item.value {
                        return child;
                    }
                }
            }
        }

        // 所有 key > search_key，去 first_child
        first_child
    }

    /// 在叶子节点中二分查找 key
    pub fn find_key(&self, key: &BTreeKey) -> Option<(u16, BTreeItem)> {
        let nkeys = self.header().nkeys;
        let mut lo: u16 = 0;
        let mut hi: u16 = nkeys as u16;

        while lo < hi {
            let mid = (lo + hi) / 2;
            if let Some(item) = self.get_item(mid) {
                match item.key.compare(key) {
                    std::cmp::Ordering::Equal => return Some((mid, item)),
                    std::cmp::Ordering::Less => lo = mid + 1,
                    std::cmp::Ordering::Greater => hi = mid,
                }
            } else {
                lo = mid + 1;
            }
        }
        None
    }

    /// 删除指定索引位置的 item（通过移动索引数组实现）
    pub fn remove_at(&mut self, index: u16) -> bool {
        let header = self.header();
        if index >= header.nkeys as u16 {
            return false;
        }
        let nkeys = header.nkeys;

        // 移动索引项：[index+1, nkeys-1] 向左移 2 字节
        if index + 1 < nkeys as u16 {
            let idx_base = std::mem::size_of::<BTreePageHeader>();
            let src_start = idx_base + (index as usize + 1) * 2;
            let src_end = idx_base + nkeys as usize * 2;
            let dst_start = idx_base + index as usize * 2;
            self.data.copy_within(src_start..src_end, dst_start);
        }

        let header = self.header_mut();
        header.nkeys -= 1;
        header.min_offset -= 2;

        true
    }
}

// 编译时验证
const _: () = assert!(std::mem::size_of::<BTreePage>() == PAGE_SIZE);

/// B-Tree 统计
#[derive(Debug, Default)]
pub struct BTreeStats {
    pub inserts: AtomicU64,
    pub lookups: AtomicU64,
    pub deletes: AtomicU64,
    pub splits: AtomicU64,
}

/// 分裂结果：推入父节点的 separator key 和新右子节点页号
struct SplitResult {
    separator: BTreeKey,
    right_page: u32,
}

/// B-Tree 索引
pub struct BTree {
    index_oid: u32,
    buffer_pool: Arc<BufferPool>,
    root_page: AtomicU32,
    next_page: AtomicU32,
    stats: BTreeStats,
    /// WAL Writer (optional)
    wal_writer: Option<Arc<WalWriter>>,
    /// Free page list（回收的空页面，allocate 优先从此取）
    free_pages: parking_lot::Mutex<Vec<u32>>,
}

impl BTree {
    pub fn new(index_oid: u32, buffer_pool: Arc<BufferPool>) -> Self {
        Self {
            index_oid,
            buffer_pool,
            root_page: AtomicU32::new(INVALID_PAGE),
            next_page: AtomicU32::new(0),
            stats: BTreeStats::default(),

            wal_writer: None,
            free_pages: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// 创建带 WAL 的 BTree
    pub fn with_wal(index_oid: u32, buffer_pool: Arc<BufferPool>, wal_writer: Arc<WalWriter>) -> Self {
        Self {
            index_oid,
            buffer_pool,
            root_page: AtomicU32::new(INVALID_PAGE),
            next_page: AtomicU32::new(0),
            stats: BTreeStats::default(),

            wal_writer: Some(wal_writer),
            free_pages: parking_lot::Mutex::new(Vec::new()),
        }
    }

    /// 设置 WAL Writer
    pub fn set_wal_writer(&mut self, writer: Arc<WalWriter>) {
        self.wal_writer = Some(writer);
    }

    fn make_page_id(&self, page_no: u32) -> PageId {
        PageId::new(0, 0, self.index_oid, page_no)
    }

    /// 写入 BTree WAL 记录（逻辑日志: page_id + key + value）
    fn wal_write_insert(&self, page_no: u32, item: &BTreeItem) -> ferrisdb_core::Result<()> {
        if let Some(ref writer) = self.wal_writer {
            let page_id = self.make_page_id(page_no);
            let item_bytes = item.serialize();
            let page_after_header = std::mem::size_of::<WalRecordForPage>() - std::mem::size_of::<WalRecordHeader>();
            let page_rec = WalRecordForPage::new(
                WalRecordType::BtreeInsertOnLeaf, page_id,
                (page_after_header + item_bytes.len()) as u16,
            );
            let page_rec_bytes = unsafe {
                std::slice::from_raw_parts(
                    &page_rec as *const WalRecordForPage as *const u8,
                    std::mem::size_of::<WalRecordForPage>(),
                )
            };
            let mut buf = Vec::with_capacity(page_rec_bytes.len() + item_bytes.len());
            buf.extend_from_slice(page_rec_bytes);
            buf.extend_from_slice(&item_bytes);
            writer.write(&buf)?;
        }
        Ok(())
    }

    /// 写入 BTree split WAL 记录
    fn wal_write_split(&self, left_page: u32, right_page: u32, separator: &BTreeKey) -> ferrisdb_core::Result<()> {
        if let Some(ref writer) = self.wal_writer {
            let page_id = self.make_page_id(left_page);
            let payload_len = 4 + 2 + separator.data.len();
            let page_after_header = std::mem::size_of::<WalRecordForPage>() - std::mem::size_of::<WalRecordHeader>();
            let page_rec = WalRecordForPage::new(
                WalRecordType::BtreeSplitLeaf, page_id,
                (page_after_header + payload_len) as u16,
            );
            let page_rec_bytes = unsafe {
                std::slice::from_raw_parts(
                    &page_rec as *const WalRecordForPage as *const u8,
                    std::mem::size_of::<WalRecordForPage>(),
                )
            };
            let mut buf = Vec::with_capacity(page_rec_bytes.len() + payload_len);
            buf.extend_from_slice(page_rec_bytes);
            buf.extend_from_slice(&right_page.to_le_bytes());
            buf.extend_from_slice(&(separator.data.len() as u16).to_le_bytes());
            buf.extend_from_slice(&separator.data);
            writer.write(&buf)?;
        }
        Ok(())
    }

    /// 初始化 B-Tree（创建根页面）
    pub fn init(&self) -> ferrisdb_core::Result<()> {
        let root_page = self.allocate_page(BTreePageType::Leaf, 0)?;
        self.root_page.store(root_page, Ordering::Release);
        Ok(())
    }

    fn make_tag(&self, page_no: u32) -> BufferTag {
        BufferTag::new(PdbId::new(0), self.index_oid as u16, page_no)
    }

    #[inline]
    /// 获取 root page 号
    pub fn root_page(&self) -> u32 {
        self.root_page.load(Ordering::Acquire)
    }

    /// 设置 root page（从 catalog 恢复时使用）
    pub fn set_root_page(&self, page: u32) {
        self.root_page.store(page, Ordering::Release);
    }

    /// 设置 next page 分配计数器（从 catalog 恢复）
    pub fn set_next_page(&self, page: u32) {
        self.next_page.store(page, Ordering::Release);
    }

    /// 获取 next page 计数器
    pub fn next_page_count(&self) -> u32 {
        self.next_page.load(Ordering::Acquire)
    }

    /// 获取当前 free pages 列表（用于持久化）
    pub fn get_free_pages(&self) -> Vec<u32> {
        self.free_pages.lock().clone()
    }

    /// 设置 free pages 列表（startup 时从 catalog 恢复）
    pub fn set_free_pages(&self, pages: Vec<u32>) {
        *self.free_pages.lock() = pages;
    }

    /// 回收空页面到 free list
    fn recycle_page(&self, page_no: u32) {
        self.free_pages.lock().push(page_no);
    }

    /// 分配页面（优先从 free list 取，否则分配新页面）
    fn allocate_page(&self, page_type: BTreePageType, level: u32) -> ferrisdb_core::Result<u32> {
        let page_no = self.free_pages.lock().pop()
            .unwrap_or_else(|| self.next_page.fetch_add(1, Ordering::AcqRel));
        let tag = self.make_tag(page_no);

        let mut pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();
        let _lock = pinned.lock_exclusive();

        let page = unsafe { BTreePage::from_ptr(page_ptr) };
        page.init(page_type, level);

        drop(_lock);
        pinned.mark_dirty();

        Ok(page_no)
    }

    /// 插入键值对
    ///
    /// 使用乐观插入 + 按需分裂。不再使用全局 split_mutex，
    /// 分裂仅在目标页面的排他锁下进行，允许不同子树并发分裂。
    /// 插入键值对（唯一约束：key 已存在时返回错误）
    pub fn insert_unique(&self, key: BTreeKey, value: BTreeValue) -> ferrisdb_core::Result<()> {
        if let Ok(Some(_)) = self.lookup(&key) {
            return Err(FerrisDBError::Internal("Duplicate key".to_string()));
        }
        self.insert(key, value)
    }

    /// 插入键值对（允许重复 key）
    ///
    /// 页级锁 coupling 方案（无全局 split_mutex）：
    /// 1. 从 root 逐层下降，每层持页级排他锁，记录搜索路径
    /// 2. 到 leaf 后：有空间直接插入，满了就地 split
    /// 3. Split 向上传播：获取 parent 排他锁 → 插入 separator
    /// 4. Root split 使用 CAS 原子替换
    ///
    /// 参照 C++ dstore B-link tree + StepRightIfNeeded 协议。
    pub fn insert(&self, key: BTreeKey, value: BTreeValue) -> ferrisdb_core::Result<()> {
        self.stats.inserts.fetch_add(1, Ordering::Relaxed);

        // 确保 root 已初始化（双重检查）
        if self.root_page.load(Ordering::Acquire) == INVALID_PAGE {
            // 用 CAS 防止并发初始化
            if self.root_page.compare_exchange(
                INVALID_PAGE, INVALID_PAGE, Ordering::AcqRel, Ordering::Acquire,
            ).is_ok() {
                self.init()?;
            }
            // 等待其他线程完成初始化
            while self.root_page.load(Ordering::Acquire) == INVALID_PAGE {
                std::hint::spin_loop();
            }
        }

        let item = BTreeItem { key, value };

        for _attempt in 0..100 {
            let root = self.root_page.load(Ordering::Acquire);
            match self.insert_recursive(root, &item, &mut Vec::new()) {
                Ok(None) => return Ok(()),
                Ok(Some(split)) => {
                    // Root 本身需要 split — 创建新 root
                    self.handle_root_split(split)?;
                    continue;
                }
                Err(_) => continue,
            }
        }
        Err(FerrisDBError::Internal("BTree insert: too many retries".to_string()))
    }

    /// 递归插入：从当前 page 下降到 leaf，按需 split 并向上传播
    ///
    /// 返回值：
    /// - Ok(None) = 插入成功，无需上层操作
    /// - Ok(Some(SplitResult)) = 当前页 split，需要向 parent 插入 separator
    /// - Err = 需要从 root 重试
    /// 递归插入：internal 用 shared lock 搜索，leaf 用 exclusive lock 修改
    ///
    /// 参照 C++ dstore：搜索路径只持 shared lock（允许并发读），
    /// 仅在到达 leaf 时升级为 exclusive（最小化排他锁范围）。
    fn insert_recursive(
        &self,
        page_no: u32,
        item: &BTreeItem,
        path: &mut Vec<u32>,
    ) -> ferrisdb_core::Result<Option<SplitResult>> {
        let tag = self.make_tag(page_no);
        let pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();

        // 先用 shared lock 判断节点类型和路由
        {
            let lock = pinned.lock_shared();
            let page = unsafe { BTreePage::from_ptr(page_ptr) };

            if !page.is_leaf() {
                // Internal node: shared lock 足够做路由决策
                let child = page.find_child(&item.key);
                drop(lock);
                drop(pinned);

                if child == INVALID_PAGE {
                    return Err(FerrisDBError::Internal("Invalid child page".to_string()));
                }

                path.push(page_no);

                return match self.insert_recursive(child, item, path)? {
                    None => Ok(None),
                    Some(child_split) => {
                        self.insert_separator_into(page_no, child_split)
                    }
                };
            }
            // 是 leaf — 释放 shared lock，下面升级为 exclusive
            drop(lock);
        }

        // Leaf node: 升级为 exclusive lock 进行修改
        let lock = pinned.lock_exclusive();
        let page = unsafe { BTreePage::from_ptr(page_ptr) };

        // Right-link 路由修正（C++ StepRightIfNeeded 等价）
        let right = page.header().right_link;
        let nkeys = page.header().nkeys;
        if right != INVALID_PAGE && right != 0 && nkeys > 0 {
            if let Some(last) = page.get_last_item() {
                if item.key.compare(&last.key) == std::cmp::Ordering::Greater {
                    drop(lock);
                    drop(pinned);
                    return self.insert_recursive(right, item, path);
                }
            }
        }

        // 有空间 → 直接插入
        if page.has_free_space((item.serialized_size() + 2) as u16) {
            page.insert_item_sorted(item);
            drop(lock);
            pinned.mark_dirty();
            self.wal_write_insert(page_no, item)?;
            return Ok(None);
        }

        // 需要 split — 在当前页排他锁下执行
        self.split_page(page, &pinned, page_no, item)
    }

    /// 执行页面分裂（当前页持排他锁，无需全局锁）
    fn split_page(
        &self,
        page: &mut BTreePage,
        pinned: &crate::buffer::PinnedBuffer<'_>,
        page_no: u32,
        item: &BTreeItem,
    ) -> ferrisdb_core::Result<Option<SplitResult>> {
        let mut items = page.get_all_items();
        let level = page.level();
        let old_right_link = page.header().right_link;

        let insert_pos = items.iter()
            .position(|i| i.key.compare(&item.key) != std::cmp::Ordering::Less)
            .unwrap_or(items.len());
        items.insert(insert_pos, item.clone());

        let mid = items.len() / 2;
        let separator = items[mid].key.clone();
        let page_type = if page.is_leaf() { BTreePageType::Leaf } else { BTreePageType::Internal };
        let right_page_no = self.allocate_page(page_type, level)?;

        // 重建左页
        page.rebuild_from_sorted(&items[..mid]);
        page.header_mut().right_link = right_page_no;
        // 注意：左页的排他锁由 caller 的 lock guard 持有，在 return 后释放
        pinned.mark_dirty();

        // 填充右页
        {
            let right_tag = self.make_tag(right_page_no);
            let right_pinned = self.buffer_pool.pin(&right_tag)?;
            let right_lock = right_pinned.lock_exclusive();
            let right_page = unsafe { BTreePage::from_ptr(right_pinned.page_data()) };
            right_page.header_mut().right_link = old_right_link;
            for ri in &items[mid..] {
                right_page.insert_item_sorted(ri);
            }
            drop(right_lock);
            right_pinned.mark_dirty();
        }

        self.wal_write_split(page_no, right_page_no, &separator)?;
        self.stats.splits.fetch_add(1, Ordering::Relaxed);

        Ok(Some(SplitResult { right_page: right_page_no, separator }))
    }

    /// 将子节点分裂产生的 separator 插入到内部节点
    fn insert_separator_into(&self, page_no: u32, split: SplitResult) -> ferrisdb_core::Result<Option<SplitResult>> {
        let tag = self.make_tag(page_no);
        let pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();
        let lock = pinned.lock_exclusive();
        let page = unsafe { BTreePage::from_ptr(page_ptr) };

        let sep_item = BTreeItem {
            key: split.separator.clone(),
            value: BTreeValue::Child(split.right_page),
        };

        if page.has_free_space((sep_item.serialized_size() + 2) as u16) {
            page.insert_item_sorted(&sep_item);
            drop(lock);
            pinned.mark_dirty();
            self.wal_write_insert(page_no, &sep_item)?;
            return Ok(None);
        }

        // 内部节点也满了 — 就地 split（在当前页排他锁下）
        self.split_page(page, &pinned, page_no, &sep_item)
    }

    /// 处理 root 分裂 — 创建新 root（CAS 原子替换）
    fn handle_root_split(&self, split: SplitResult) -> ferrisdb_core::Result<()> {
        let old_root = self.root_page.load(Ordering::Acquire);

        // 读取旧 root 的 level
        let level = {
            let tag = self.make_tag(old_root);
            let pinned = self.buffer_pool.pin(&tag)?;
            let lock = pinned.lock_shared();
            let page = unsafe { BTreePage::from_ptr(pinned.page_data()) };
            let l = page.level();
            drop(lock);
            l
        };

        let new_root_page = self.allocate_page(BTreePageType::Internal, level + 1)?;
        {
            let tag = self.make_tag(new_root_page);
            let pinned = self.buffer_pool.pin(&tag)?;
            let lock = pinned.lock_exclusive();
            let page = unsafe { BTreePage::from_ptr(pinned.page_data()) };

            page.header_mut().first_child = old_root;
            let item = BTreeItem {
                key: split.separator,
                value: BTreeValue::Child(split.right_page),
            };
            page.insert_item_sorted(&item);

            drop(lock);
            pinned.mark_dirty();
        }

        // CAS 原子替换 root — 如果其他线程已经更新了 root，
        // 说明并发 root split 发生，本次 split 会在下次重试时通过
        // right_link 路由修正找到正确位置
        let _ = self.root_page.compare_exchange(
            old_root, new_root_page, Ordering::AcqRel, Ordering::Acquire,
        );
        Ok(())
    }

    /// 尝试在指定页面插入 item。
    /// 返回 Ok(true) = 需要分裂，Ok(false) = 成功，Err = 错误
    fn insert_into_page(&self, page_no: u32, item: &BTreeItem) -> ferrisdb_core::Result<bool> {
        let tag = self.make_tag(page_no);
        let pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();
        let lock = pinned.lock_exclusive();

        let page = unsafe { BTreePage::from_ptr(page_ptr) };

        if page.is_leaf() {
            // 叶子节点：尝试直接插入
            if page.has_free_space((item.serialized_size() + 2) as u16) {
                page.insert_item_sorted(item);
                drop(lock);
                pinned.mark_dirty();
                return Ok(false); // 成功
            } else {
                return Ok(true); // 需要分裂
            }
        }

        // 内部节点：导航到子节点
        let child_page = page.find_child(&item.key);
        drop(lock);
        drop(pinned);

        if child_page == INVALID_PAGE {
            return Ok(true);
        }

        // 递归插入到子节点
        self.insert_into_page(child_page, item)
    }

    /// 执行叶子分裂
    ///
    /// `expected_nkeys`: 读取 items 时页面的 nkeys，用于检测并发修改
    fn do_leaf_split(&self, page_no: u32, mut items: Vec<BTreeItem>, old_right_link: u32, level: u32, expected_nkeys: u32, item: &BTreeItem) -> ferrisdb_core::Result<SplitResult> {
        self.stats.splits.fetch_add(1, Ordering::Relaxed);

        // 插入新 item
        let insert_pos = items.iter().position(|i| i.key.compare(&item.key) != std::cmp::Ordering::Less).unwrap_or(items.len());
        items.insert(insert_pos, item.clone());

        let mid = items.len() / 2;
        let left_items = &items[..mid];
        let right_items = &items[mid..];
        let separator = right_items[0].key.clone();

        let right_page_no = self.allocate_page(BTreePageType::Leaf, level)?;

        // Rebuild left page (持锁检查并发修改)
        {
            let tag = self.make_tag(page_no);
            let pinned = self.buffer_pool.pin(&tag)?;
            let lock = pinned.lock_exclusive();
            let page = unsafe { BTreePage::from_ptr(pinned.page_data()) };

            // 检测并发修改：如果 nkeys 变了，说明有并发 split 发生
            // 放弃本次 split，返回 Err 让上层 insert() 从根重新查找正确叶子
            let current_nkeys = page.header().nkeys;
            if current_nkeys != expected_nkeys {
                drop(lock);
                drop(pinned);
                return Err(FerrisDBError::Internal("concurrent split detected, retry from root".to_string()));
            }

            page.rebuild_from_sorted(left_items);
            page.header_mut().right_link = right_page_no;
            drop(lock);
            pinned.mark_dirty();
        }

        // Build right page
        {
            let tag = self.make_tag(right_page_no);
            let pinned = self.buffer_pool.pin(&tag)?;
            let lock = pinned.lock_exclusive();
            let page = unsafe { BTreePage::from_ptr(pinned.page_data()) };
            page.header_mut().right_link = old_right_link;
            for ri in right_items {
                page.insert_item_sorted(ri);
            }
            drop(lock);
            pinned.mark_dirty();
        }

        self.wal_write_split(page_no, right_page_no, &separator)?;
        Ok(SplitResult { separator, right_page: right_page_no })
    }

    /// 执行内部节点分裂
    ///
    /// `expected_nkeys`: 读取 items 时的 nkeys，用于检测并发修改
    fn do_internal_split(&self, page_no: u32, mut items: Vec<BTreeItem>, level: u32, expected_nkeys: u32, new_item: BTreeItem) -> ferrisdb_core::Result<SplitResult> {
        self.stats.splits.fetch_add(1, Ordering::Relaxed);

        let insert_pos = items.iter().position(|i| i.key.compare(&new_item.key) != std::cmp::Ordering::Less).unwrap_or(items.len());
        items.insert(insert_pos, new_item.clone());

        let mid = items.len() / 2;
        let left_items = &items[..mid];
        let up_key = items[mid].key.clone();
        let right_first_child = if let BTreeValue::Child(c) = items[mid].value { c } else {
            return Err(FerrisDBError::Internal("Internal node must have Child value".to_string()));
        };
        let right_items = &items[mid + 1..];

        let right_page_no = self.allocate_page(BTreePageType::Internal, level)?;

        // Rebuild left page (持锁检查并发修改)
        {
            let tag = self.make_tag(page_no);
            let pinned = self.buffer_pool.pin(&tag)?;
            let lock = pinned.lock_exclusive();
            let page = unsafe { BTreePage::from_ptr(pinned.page_data()) };

            let current_nkeys = page.header().nkeys;
            if current_nkeys != expected_nkeys {
                drop(lock);
                drop(pinned);
                return Err(FerrisDBError::Internal("concurrent split detected, retry from root".to_string()));
            }

            page.rebuild_from_sorted(left_items);
            drop(lock);
            pinned.mark_dirty();
        }

        // Build right page
        {
            let tag = self.make_tag(right_page_no);
            let pinned = self.buffer_pool.pin(&tag)?;
            let lock = pinned.lock_exclusive();
            let page = unsafe { BTreePage::from_ptr(pinned.page_data()) };
            page.header_mut().first_child = right_first_child;
            for ri in right_items {
                page.insert_item_sorted(ri);
            }
            drop(lock);
            pinned.mark_dirty();
        }

        Ok(SplitResult { separator: up_key, right_page: right_page_no })
    }

    /// 旧接口：保留兼容，委托给新逻辑
    #[allow(dead_code)]
    fn do_split(&self, page_no: u32, item: &BTreeItem) -> ferrisdb_core::Result<()> {
        let tag = self.make_tag(page_no);
        let mut pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();
        let lock = pinned.lock_exclusive();

        let page = unsafe { BTreePage::from_ptr(page_ptr) };

        if page.is_leaf() {
            // 如果现在有空间了（其他线程可能已分裂），直接插入
            if page.has_free_space((item.serialized_size() + 2) as u16) {
                page.insert_item_sorted(item);
                drop(lock);
                pinned.mark_dirty();
                return Ok(());
            }
            drop(lock);
            drop(pinned);
            let split_result = self.split_leaf(page_no, item)?;
            self.insert_separator(page_no, split_result.separator, split_result.right_page, 0)?;
        } else {
            // 内部节点：先导航到子节点
            let child_page = page.find_child(&item.key);
            drop(lock);
            drop(pinned);

            if child_page == INVALID_PAGE {
                return Err(FerrisDBError::Internal("Invalid child page".to_string()));
            }

            // 递归处理子节点
            self.do_split(child_page, item)?;
        }

        Ok(())
    }

    /// 分裂叶子页面并插入新 item
    fn split_leaf(&self, page_no: u32, item: &BTreeItem) -> ferrisdb_core::Result<SplitResult> {
        self.stats.splits.fetch_add(1, Ordering::Relaxed);

        // 读取当前页面的所有 items
        let tag = self.make_tag(page_no);
        let pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();
        let lock = pinned.lock_exclusive();
        let page = unsafe { BTreePage::from_ptr(page_ptr) };

        let mut items = page.get_all_items();
        let level = page.level();
        let old_right_link = page.header().right_link;

        drop(lock);
        drop(pinned);

        // 插入新 item（保持有序）
        let insert_pos = items.iter().position(|i| i.key.compare(&item.key) != std::cmp::Ordering::Less).unwrap_or(items.len());
        items.insert(insert_pos, item.clone());

        // 分裂点
        let mid = items.len() / 2;
        let left_items = &items[..mid];
        let right_items = &items[mid..];
        let separator = right_items[0].key.clone();

        // 分配新的右页面
        let right_page_no = self.allocate_page(BTreePageType::Leaf, level)?;

        // 重建左页面
        {
            let tag = self.make_tag(page_no);
            let mut pinned = self.buffer_pool.pin(&tag)?;
            let page_ptr = pinned.page_data();
            let lock = pinned.lock_exclusive();
            let page = unsafe { BTreePage::from_ptr(page_ptr) };

            page.rebuild_from_sorted(left_items);
            page.header_mut().right_link = right_page_no; // 指向新右兄弟

            drop(lock);
            pinned.mark_dirty();
        }

        // 构建右页面
        {
            let tag = self.make_tag(right_page_no);
            let mut pinned = self.buffer_pool.pin(&tag)?;
            let page_ptr = pinned.page_data();
            let lock = pinned.lock_exclusive();
            let page = unsafe { BTreePage::from_ptr(page_ptr) };

            page.header_mut().right_link = old_right_link; // 继承旧的 right_link

            for ri in right_items {
                page.insert_item_sorted(ri);
            }

            drop(lock);
            pinned.mark_dirty();
        }

        Ok(SplitResult {
            separator,
            right_page: right_page_no,
        })
    }

    /// 将 separator 插入到 page_no 的父节点中。
    /// 如果 page_no 是 root，创建新 root。
    fn insert_separator(
        &self,
        child_page_no: u32,
        separator: BTreeKey,
        right_child: u32,
        child_level: u32,
    ) -> ferrisdb_core::Result<()> {
        let root = self.root_page.load(Ordering::Acquire);

        // 如果 child 就是 root，需要创建新 root
        if child_page_no == root {
            let new_root_page = self.allocate_page(BTreePageType::Internal, child_level + 1)?;

            {
                let tag = self.make_tag(new_root_page);
                let mut pinned = self.buffer_pool.pin(&tag)?;
                let page_ptr = pinned.page_data();
                let lock = pinned.lock_exclusive();
                let page = unsafe { BTreePage::from_ptr(page_ptr) };

                // 新 root: first_child = old_root, items = [(separator, right_child)]
                page.header_mut().first_child = root;

                let item = BTreeItem {
                    key: separator,
                    value: BTreeValue::Child(right_child),
                };
                page.insert_item_sorted(&item);

                drop(lock);
                pinned.mark_dirty();
            }

            self.root_page.store(new_root_page, Ordering::Release);
            return Ok(());
        }

        // 查找 child_page_no 的父节点
        let (parent_page, _child_idx) = self.find_parent(root, child_page_no);

        if parent_page == INVALID_PAGE {
            // 找不到父节点（不应该发生），重新创建 root
            return self.insert_separator(root, separator, right_child, child_level);
        }

        // 尝试插入到父节点
        let tag = self.make_tag(parent_page);
        let pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();
        let lock = pinned.lock_exclusive();
        let page = unsafe { BTreePage::from_ptr(page_ptr) };

        let item = BTreeItem {
            key: separator.clone(),
            value: BTreeValue::Child(right_child),
        };

        if page.has_free_space((item.serialized_size() + 2) as u16) {
            page.insert_item_sorted(&item);
            drop(lock);
            pinned.mark_dirty();
            return Ok(());
        }

        // 父节点也满了，需要分裂
        drop(lock);
        drop(pinned);
        self.split_internal_and_insert(parent_page, item, child_level + 1)
    }

    /// 分裂内部节点并插入新 item
    fn split_internal_and_insert(
        &self,
        page_no: u32,
        item: BTreeItem,
        level: u32,
    ) -> ferrisdb_core::Result<()> {
        self.stats.splits.fetch_add(1, Ordering::Relaxed);

        // 读取当前页面的所有 items
        let tag = self.make_tag(page_no);
        let pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();
        let lock = pinned.lock_exclusive();
        let page = unsafe { BTreePage::from_ptr(page_ptr) };

        let mut items = page.get_all_items();
        let old_first_child = page.header().first_child;

        drop(lock);
        drop(pinned);

        // 插入新 item（保持有序）
        let insert_pos = items.iter().position(|i| i.key.compare(&item.key) != std::cmp::Ordering::Less).unwrap_or(items.len());
        items.insert(insert_pos, item);

        // 分裂点：中间的 key 会上推
        let mid = items.len() / 2;
        let left_items = &items[..mid];
        let up_key = items[mid].key.clone();
        let right_items = &items[mid + 1..];
        let right_first_child = if let BTreeValue::Child(c) = items[mid].value {
            c
        } else {
            return Err(FerrisDBError::Internal("Internal node item must have Child value".to_string()));
        };

        // 分配新右页面
        let right_page_no = self.allocate_page(BTreePageType::Internal, level)?;

        // 重建左页面
        {
            let tag = self.make_tag(page_no);
            let mut pinned = self.buffer_pool.pin(&tag)?;
            let page_ptr = pinned.page_data();
            let lock = pinned.lock_exclusive();
            let page = unsafe { BTreePage::from_ptr(page_ptr) };

            page.rebuild_from_sorted(left_items);
            // left page keeps old first_child

            drop(lock);
            pinned.mark_dirty();
        }

        // 构建右页面
        {
            let tag = self.make_tag(right_page_no);
            let mut pinned = self.buffer_pool.pin(&tag)?;
            let page_ptr = pinned.page_data();
            let lock = pinned.lock_exclusive();
            let page = unsafe { BTreePage::from_ptr(page_ptr) };

            page.header_mut().first_child = right_first_child;

            for ri in right_items {
                page.insert_item_sorted(ri);
            }

            drop(lock);
            pinned.mark_dirty();
        }

        // 将 up_key 推入父节点
        self.insert_separator(page_no, up_key, right_page_no, level)
    }

    /// 查找给定页面的父节点
    fn find_parent(&self, root: u32, target: u32) -> (u32, u16) {
        if root == target {
            return (INVALID_PAGE, 0);
        }
        self._find_parent(root, target)
    }

    fn _find_parent(&self, page_no: u32, target: u32) -> (u32, u16) {
        let tag = self.make_tag(page_no);
        let pinned = match self.buffer_pool.pin(&tag) {
            Ok(p) => p,
            Err(_) => return (INVALID_PAGE, 0),
        };
        let page_ptr = pinned.page_data();
        let _lock = pinned.lock_shared();
        let page = unsafe { BTreePage::from_ptr_const(page_ptr) };

        if page.is_leaf() {
            return (INVALID_PAGE, 0);
        }

        let nkeys = page.nkeys();
        let first_child = page.header().first_child;

        // 检查 first_child
        if first_child == target {
            return (page_no, 0);
        }

        // 检查每个 item 的 child
        for i in 0..nkeys {
            if let Some(item) = page.get_item(i as u16) {
                if let BTreeValue::Child(child) = item.value {
                    if child == target {
                        return (page_no, i as u16);
                    }
                    // 递归搜索
                    let (p, idx) = self._find_parent(child, target);
                    if p != INVALID_PAGE {
                        return (p, idx);
                    }
                }
            }
        }

        (INVALID_PAGE, 0)
    }

    /// 查找键
    pub fn lookup(&self, key: &BTreeKey) -> ferrisdb_core::Result<Option<BTreeValue>> {
        self.stats.lookups.fetch_add(1, Ordering::Relaxed);

        let root = self.root_page.load(Ordering::Acquire);
        if root == INVALID_PAGE {
            return Ok(None);
        }

        let mut page_no = root;
        loop {
            let tag = self.make_tag(page_no);
            let pinned = match self.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => return Ok(None),
            };
            let page_ptr = pinned.page_data();
            let _lock = pinned.lock_shared();
            let page = unsafe { BTreePage::from_ptr_const(page_ptr) };

            if page.is_leaf() {
                if let Some((_, item)) = page.find_key(key) {
                    return Ok(Some(item.value));
                }
                // B-link tree: key 可能因并发 split 被移到 right sibling
                let right = page.header().right_link;
                drop(_lock);
                drop(pinned);
                if right != INVALID_PAGE && right != 0 {
                    page_no = right;
                    continue; // 跟随 right-link
                }
                return Ok(None);
            }

            // 内部节点：导航到子节点
            let child = page.find_child(key);
            drop(_lock);
            drop(pinned);
            if child == INVALID_PAGE {
                return Ok(None);
            }
            page_no = child;
        }
    }

    /// 删除指定 key
    pub fn delete(&self, key: &BTreeKey) -> ferrisdb_core::Result<bool> {
        self.stats.deletes.fetch_add(1, Ordering::Relaxed);
        // delete 只修改叶子页面（标记 item），不修改树结构
        // page-level exclusive lock 足够保护并发

        let root = self.root_page.load(Ordering::Acquire);
        if root == INVALID_PAGE {
            return Ok(false);
        }

        let mut page_no = root;
        loop {
            let tag = self.make_tag(page_no);
            let mut pinned = match self.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => return Ok(false),
            };
            let page_ptr = pinned.page_data();
            let lock = pinned.lock_exclusive();
            let page = unsafe { BTreePage::from_ptr(page_ptr) };

            if page.is_leaf() {
                if let Some((idx, _)) = page.find_key(key) {
                    page.remove_at(idx);
                    let nkeys = page.header().nkeys;
                    let right_link = page.header().right_link;
                    drop(lock);
                    pinned.mark_dirty();

                    // 尝试合并：如果页面 key 数低于 25% 容量且有右兄弟
                    if nkeys > 0 && nkeys < 5 && right_link != INVALID_PAGE {
                        let _ = self.try_merge_leaf(page_no, right_link);
                    }
                    return Ok(true);
                }
                return Ok(false);
            }

            let child = page.find_child(key);
            drop(lock);
            drop(pinned);
            page_no = child;
        }
    }

    /// 尝试合并两个相邻叶子页面
    ///
    /// 如果两个页面的 items 合并后能放入一页，则执行合并。
    fn try_merge_leaf(&self, left_page: u32, right_page: u32) -> ferrisdb_core::Result<bool> {
        let left_tag = self.make_tag(left_page);
        let right_tag = self.make_tag(right_page);

        let left_pinned = self.buffer_pool.pin(&left_tag)?;
        let right_pinned = self.buffer_pool.pin(&right_tag)?;

        let left_lock = left_pinned.lock_exclusive();
        let right_lock = right_pinned.lock_exclusive();

        let left_pg = unsafe { BTreePage::from_ptr(left_pinned.page_data()) };
        let right_pg = unsafe { BTreePage::from_ptr(right_pinned.page_data()) };

        let left_items = left_pg.get_all_items();
        let right_items = right_pg.get_all_items();

        // 检查合并后是否能放入一页（预留 20% 空间防止立即再分裂）
        let total_size: usize = left_items.iter().chain(right_items.iter())
            .map(|i| i.serialized_size() + 2)
            .sum();
        let header_size = std::mem::size_of::<BTreePageHeader>();
        if total_size + header_size > PAGE_SIZE * 80 / 100 {
            return Ok(false); // 放不下
        }

        // 执行合并：所有 items 放到左页，右页清空
        let right_right_link = right_pg.header().right_link;
        left_pg.rebuild_from_sorted(&[left_items, right_items].concat());
        left_pg.header_mut().right_link = right_right_link;

        // 右页标记为空并回收
        right_pg.header_mut().nkeys = 0;

        drop(left_lock);
        drop(right_lock);
        left_pinned.mark_dirty();
        right_pinned.mark_dirty();

        // 回收空的右页面
        self.recycle_page(right_page);

        Ok(true)
    }

    /// 前缀扫描：收集所有以 prefix 开头的键值对
    pub fn scan_prefix(&self, prefix: &[u8]) -> ferrisdb_core::Result<Vec<(Vec<u8>, BTreeValue)>> {
        let root = self.root_page.load(Ordering::Acquire);
        if root == INVALID_PAGE {
            return Ok(Vec::new());
        }

        // 导航到第一个可能包含 prefix 的叶子
        let mut page_no = root;
        let search_key = BTreeKey::new(prefix.to_vec());
        loop {
            let tag = self.make_tag(page_no);
            let pinned = match self.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => return Ok(Vec::new()),
            };
            let page_ptr = pinned.page_data();
            let _lock = pinned.lock_shared();
            let page = unsafe { BTreePage::from_ptr_const(page_ptr) };

            if page.is_leaf() {
                break;
            }

            let child = page.find_child(&search_key);
            drop(_lock);
            drop(pinned);
            page_no = child;
        }

        // 在叶子链表中扫描
        let mut results = Vec::new();
        let mut current = page_no;

        while current != INVALID_PAGE {
            let tag = self.make_tag(current);
            let pinned = match self.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => break,
            };
            let page_ptr = pinned.page_data();
            let _lock = pinned.lock_shared();
            let page = unsafe { BTreePage::from_ptr_const(page_ptr) };

            let nkeys = page.nkeys();
            let mut still_in_range = false;

            for i in 0..nkeys {
                if let Some(item) = page.get_item(i as u16) {
                    if item.key.starts_with(prefix) {
                        results.push((item.key.data.clone(), item.value));
                        still_in_range = true;
                    } else if item.key.data.as_slice() > prefix {
                        // 超出前缀范围，继续检查（因为可能有间隔）
                        still_in_range = true;
                    }
                }
            }

            let right_link = page.header().right_link;
            drop(_lock);
            drop(pinned);

            // 如果页面中有超出前缀的 key 且不再有匹配，停止扫描
            if !still_in_range && !results.is_empty() {
                // 已经超出前缀范围
                // 但我们需要检查是否完全超出
                break;
            }

            current = right_link;
        }

        Ok(results)
    }

    /// 反向前缀扫描：收集所有以 prefix 开头的键值对，按 key 降序排列
    pub fn scan_prefix_reverse(&self, prefix: &[u8]) -> ferrisdb_core::Result<Vec<(Vec<u8>, BTreeValue)>> {
        let mut results = self.scan_prefix(prefix)?;
        results.reverse();
        Ok(results)
    }

    /// 范围扫描：收集 [start_key, end_key) 范围内的键值对
    pub fn scan_range(&self, start_key: &[u8], end_key: &[u8]) -> ferrisdb_core::Result<Vec<(Vec<u8>, BTreeValue)>> {
        let all = self.scan_prefix(&[])?; // 全表扫描（简化实现）
        Ok(all.into_iter()
            .filter(|(k, _)| k.as_slice() >= start_key && k.as_slice() < end_key)
            .collect())
    }

    /// 查找以 prefix 开头的最小键
    pub fn find_min_with_prefix(&self, prefix: &[u8]) -> ferrisdb_core::Result<Option<(Vec<u8>, BTreeValue)>> {
        let root = self.root_page.load(Ordering::Acquire);
        if root == INVALID_PAGE {
            return Ok(None);
        }

        // 导航到第一个可能包含 prefix 的叶子
        let mut page_no = root;
        let search_key = BTreeKey::new(prefix.to_vec());
        loop {
            let tag = self.make_tag(page_no);
            let pinned = match self.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => return Ok(None),
            };
            let page_ptr = pinned.page_data();
            let _lock = pinned.lock_shared();
            let page = unsafe { BTreePage::from_ptr_const(page_ptr) };

            if page.is_leaf() {
                break;
            }

            let child = page.find_child(&search_key);
            drop(_lock);
            drop(pinned);
            page_no = child;
        }

        // 在叶子中查找最小匹配
        let mut current = page_no;
        while current != INVALID_PAGE {
            let tag = self.make_tag(current);
            let pinned = match self.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => return Ok(None),
            };
            let page_ptr = pinned.page_data();
            let _lock = pinned.lock_shared();
            let page = unsafe { BTreePage::from_ptr_const(page_ptr) };

            let nkeys = page.nkeys();
            for i in 0..nkeys {
                if let Some(item) = page.get_item(i as u16) {
                    if item.key.starts_with(prefix) {
                        return Ok(Some((item.key.data.clone(), item.value)));
                    }
                }
            }

            let right_link = page.header().right_link;
            drop(_lock);
            drop(pinned);
            current = right_link;
        }

        Ok(None)
    }

    #[inline]
    pub fn stats(&self) -> &BTreeStats {
        &self.stats
    }

    #[inline]
    pub fn index_oid(&self) -> u32 {
        self.index_oid
    }
}

/// B-Tree 游标
pub struct BTreeCursor<'a> {
    btree: &'a BTree,
    current_page: u32,
    current_index: u16,
}

impl<'a> BTreeCursor<'a> {
    pub fn new(btree: &'a BTree) -> Self {
        Self {
            btree,
            current_page: btree.root_page(),
            current_index: 0,
        }
    }

    /// 移动到第一个叶子页面
    pub fn seek_to_first(&mut self) {
        let mut page_no = self.btree.root_page.load(Ordering::Acquire);
        if page_no == INVALID_PAGE {
            self.current_page = INVALID_PAGE;
            return;
        }

        loop {
            let tag = self.btree.make_tag(page_no);
            let pinned = match self.btree.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => { self.current_page = INVALID_PAGE; return; }
            };
            let page_ptr = pinned.page_data();
            let _lock = pinned.lock_shared();
            let page = unsafe { BTreePage::from_ptr_const(page_ptr) };

            if page.is_leaf() {
                self.current_page = page_no;
                self.current_index = 0;
                return;
            }

            let first_child = page.header().first_child;
            drop(_lock);
            drop(pinned);
            page_no = first_child;
        }
    }

    pub fn next(&mut self) -> ferrisdb_core::Result<Option<BTreeItem>> {
        if self.current_page == INVALID_PAGE {
            return Ok(None);
        }

        loop {
            let tag = self.btree.make_tag(self.current_page);
            let pinned = match self.btree.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => return Ok(None),
            };
            let page_ptr = pinned.page_data();
            let _lock = pinned.lock_shared();
            let page = unsafe { BTreePage::from_ptr_const(page_ptr) };

            if self.current_index < page.nkeys() as u16 {
                if let Some(item) = page.get_item(self.current_index) {
                    self.current_index += 1;
                    return Ok(Some(item));
                }
            }

            let right_link = page.header().right_link;
            if right_link == INVALID_PAGE {
                return Ok(None);
            }

            drop(_lock);
            drop(pinned);

            self.current_page = right_link;
            self.current_index = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_btree_key() {
        let key1 = BTreeKey::new(vec![1, 2, 3]);
        let key2 = BTreeKey::new(vec![1, 2, 3]);
        let key3 = BTreeKey::new(vec![1, 2, 4]);
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert!(key1 < key3);
    }

    #[test]
    fn test_btree_page_sorted_insert() {
        let mut page = BTreePage::new();
        page.init(BTreePageType::Leaf, 0);

        // 乱序插入
        let items = vec![
            (vec![3], BTreeValue::Tuple { block: 3, offset: 1 }),
            (vec![1], BTreeValue::Tuple { block: 1, offset: 1 }),
            (vec![2], BTreeValue::Tuple { block: 2, offset: 1 }),
            (vec![5], BTreeValue::Tuple { block: 5, offset: 1 }),
            (vec![4], BTreeValue::Tuple { block: 4, offset: 1 }),
        ];

        for (key_data, value) in items {
            let item = BTreeItem { key: BTreeKey::new(key_data), value };
            assert!(page.insert_item_sorted(&item).is_some());
        }

        // 验证有序
        assert_eq!(page.nkeys(), 5);
        for i in 0..4u16 {
            let a = page.get_item(i).unwrap();
            let b = page.get_item(i + 1).unwrap();
            assert!(a.key < b.key, "Items not sorted at index {}", i);
        }

        // 验证查找
        let search = BTreeKey::new(vec![3]);
        let (idx, found) = page.find_key(&search).unwrap();
        assert_eq!(idx, 2);
        if let BTreeValue::Tuple { block, .. } = found.value {
            assert_eq!(block, 3);
        }
    }

    #[test]
    fn test_btree_page_find_child() {
        let mut page = BTreePage::new();
        page.init(BTreePageType::Internal, 1);
        page.header_mut().first_child = 10; // keys < 10

        // items: (key=10, child=20), (key=20, child=30)
        page.insert_item_sorted(&BTreeItem {
            key: BTreeKey::new(vec![10]),
            value: BTreeValue::Child(20),
        });
        page.insert_item_sorted(&BTreeItem {
            key: BTreeKey::new(vec![20]),
            value: BTreeValue::Child(30),
        });

        // key < 10 -> first_child = 10
        assert_eq!(page.find_child(&BTreeKey::new(vec![5])), 10);
        // key = 10 -> child 20
        assert_eq!(page.find_child(&BTreeKey::new(vec![10])), 20);
        // key = 15 -> child 20 (10 <= 15 < 20)
        assert_eq!(page.find_child(&BTreeKey::new(vec![15])), 20);
        // key = 20 -> child 30
        assert_eq!(page.find_child(&BTreeKey::new(vec![20])), 30);
        // key = 25 -> child 30
        assert_eq!(page.find_child(&BTreeKey::new(vec![25])), 30);
    }

    #[test]
    fn test_btree_page_remove() {
        let mut page = BTreePage::new();
        page.init(BTreePageType::Leaf, 0);

        for i in 1..=5u8 {
            let item = BTreeItem {
                key: BTreeKey::new(vec![i]),
                value: BTreeValue::Tuple { block: i as u32, offset: 1 },
            };
            page.insert_item_sorted(&item);
        }

        assert_eq!(page.nkeys(), 5);

        // 删除中间的
        assert!(page.remove_at(2)); // 删除 key=3
        assert_eq!(page.nkeys(), 4);

        // 验证剩余有序
        let items = page.get_all_items();
        let keys: Vec<u8> = items.iter().map(|i| i.key.data[0]).collect();
        assert_eq!(keys, vec![1, 2, 4, 5]);
    }
}
