//! Buffer Descriptor
//!
//! 描述缓冲区中的页面状态。
//!
//! # 布局（128 字节，128 对齐）
//!
//! ```text
//! Offset 0-7:     buf_block (AtomicPtr)
//! Offset 8-19:    buf_tag (BufferTag, 12 bytes packed)
//! Offset 20-23:   padding
//! Offset 24-31:   controller (AtomicPtr)
//! Offset 32-39:   state (AtomicU64)
//! Offset 40-71:   lru_node (32 bytes)
//! Offset 72-95:   cr_info (24 bytes)
//! Offset 96-111:  content_lock (ContentLock, 16 bytes)
//! Offset 112-127: reserved (16 bytes)
//! ```
//!
//! 注：C++ 版本的 contentLwLock (LWLock) 也是内嵌在 BufferDesc 中（占 128 字节）。
//! Rust 版本使用 16 字节的紧凑 ContentLock 替代，功能等价但更省空间。

use ferrisdb_core::atomic::{AtomicU32, AtomicU64, AtomicPtr, AtomicBool, Ordering};
use ferrisdb_core::BufferTag;
use ferrisdb_core::lock::ContentLock;
use std::cell::UnsafeCell;
use std::mem::ManuallyDrop;

/// 脏页队列最大大小
pub const DIRTY_PAGE_QUEUE_MAX_SIZE: usize = 2;

/// Buffer 状态字位定义
mod state_bits {
    /// 引用计数掩码 (bits 0-31)
    pub const REF_COUNT_MASK: u64 = (1 << 32) - 1;

    /// Header 锁定标志 (bit 32)
    pub const BM_LOCKED: u64 = 1 << 32;

    /// 脏页标志 (bit 33)
    pub const BM_DIRTY: u64 = 1 << 33;

    /// 有效标志 (bit 34)
    pub const BM_VALID: u64 = 1 << 34;

    /// Tag 有效标志 (bit 35)
    pub const BM_TAG_VALID: u64 = 1 << 35;

    /// IO 进行中标志 (bit 36)
    pub const BM_IO_IN_PROGRESS: u64 = 1 << 36;

    /// IO 错误标志 (bit 37)
    pub const BM_IO_ERROR: u64 = 1 << 37;

    /// Hint 脏标志 (bit 38)
    pub const BM_HINT_DIRTY: u64 = 1 << 38;

    /// 检查点标志 (bit 39)
    pub const BM_CHECKPOINT_NEEDED: u64 = 1 << 39;
}

/// CRInfo 联合体（用于 CR Buffer）
#[repr(C)]
pub union CRInfo {
    /// Base buffer 信息
    pub base: ManuallyDrop<CRInfoBase>,
    /// CR buffer 信息
    pub cr: ManuallyDrop<CRInfoCr>,
}

impl CRInfo {
    /// 创建默认的 CRInfo
    fn new() -> Self {
        Self {
            base: ManuallyDrop::new(CRInfoBase::default()),
        }
    }
}

impl Default for CRInfo {
    fn default() -> Self {
        Self::new()
    }
}

/// Base buffer 的 CR 信息
#[derive(Debug, Default)]
#[repr(C)]
pub struct CRInfoBase {
    /// CR buffer 指针
    pub cr_buffer: AtomicPtr<u8>,
    /// CR 页面最大 CSN
    pub cr_page_max_csn: AtomicU64,
    /// 是否可用
    pub is_usable: AtomicBool,
    /// 填充到 24 字节
    pub _padding: [u8; 7],
}

const _: () = assert!(std::mem::size_of::<CRInfoBase>() == 24);

/// CR buffer 的信息
#[derive(Debug, Default)]
#[repr(C)]
pub struct CRInfoCr {
    /// Base buffer 指针
    pub base_buffer_desc: AtomicPtr<u8>,
    /// 填充到 24 字节
    pub _padding: [u8; 16],
}

const _: () = assert!(std::mem::size_of::<CRInfoCr>() == 24);

/// Buffer 状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferState {
    /// 原始状态字
    pub raw: u64,
}

impl BufferState {
    /// 创建新的状态
    #[inline]
    pub const fn new() -> Self {
        Self { raw: 0 }
    }

    /// 从原始值创建
    #[inline]
    pub const fn from_raw(raw: u64) -> Self {
        Self { raw }
    }

    /// 获取引用计数
    #[inline]
    pub const fn ref_count(&self) -> u32 {
        (self.raw & state_bits::REF_COUNT_MASK) as u32
    }

    /// 检查 header 是否锁定
    #[inline]
    pub const fn is_locked(&self) -> bool {
        (self.raw & state_bits::BM_LOCKED) != 0
    }

    /// 检查是否脏页
    #[inline]
    pub const fn is_dirty(&self) -> bool {
        (self.raw & state_bits::BM_DIRTY) != 0
    }

    /// 检查是否有效
    #[inline]
    pub const fn is_valid(&self) -> bool {
        (self.raw & state_bits::BM_VALID) != 0
    }

    /// 检查 tag 是否有效
    #[inline]
    pub const fn is_tag_valid(&self) -> bool {
        (self.raw & state_bits::BM_TAG_VALID) != 0
    }

    /// 检查是否有 IO 进行中
    #[inline]
    pub const fn is_io_in_progress(&self) -> bool {
        (self.raw & state_bits::BM_IO_IN_PROGRESS) != 0
    }

    /// 检查是否需要检查点
    #[inline]
    pub const fn is_checkpoint_needed(&self) -> bool {
        (self.raw & state_bits::BM_CHECKPOINT_NEEDED) != 0
    }
}

impl Default for BufferState {
    fn default() -> Self {
        Self::new()
    }
}

/// LRU 节点数据（32 字节）
///
/// 字段顺序按对齐要求排列，避免填充。
#[derive(Debug)]
#[repr(C)]
pub struct LruNodeData {
    /// 最近使用时间（8字节对齐，放最前）
    pub last_used: AtomicU64,
    /// 前驱索引
    pub prev: AtomicU32,
    /// 后继索引
    pub next: AtomicU32,
    /// LRU 队列 ID
    pub queue_id: AtomicU32,
    /// 填充到 32 字节
    pub _padding: [u8; 12],
}

impl Default for LruNodeData {
    fn default() -> Self {
        Self {
            last_used: AtomicU64::new(0),
            prev: AtomicU32::new(u32::MAX),
            next: AtomicU32::new(u32::MAX),
            queue_id: AtomicU32::new(0),
            _padding: [0; 12],
        }
    }
}

const _: () = assert!(std::mem::size_of::<LruNodeData>() == 32);

/// Buffer Descriptor
///
/// 描述缓冲区中的页面状态。
///
/// # 内存布局
///
/// 128 字节，128 对齐。ContentLock 内嵌在 offset 96-111，
/// 功能上与 C++ 版本的 `contentLwLock` 等价。
#[repr(C, align(128))]
pub struct BufferDesc {
    // === Offset 0-7 (8 bytes) ===
    /// 页面数据指针
    pub buf_block: AtomicPtr<u8>,

    // === Offset 8-19 (12 bytes) ===
    /// Buffer 标签（packed 12 字节）
    pub buf_tag: UnsafeCell<BufferTag>,

    // === Offset 20-23 (4 bytes padding) ===
    /// 填充字段
    pub _pad1: [u8; 4],

    // === Offset 24-31 (8 bytes) ===
    /// 控制器指针
    pub controller: AtomicPtr<u8>,

    // === Offset 32-39 (8 bytes) ===
    /// 状态字
    pub state: AtomicU64,

    // === Offset 40-71 (32 bytes) ===
    /// LRU 节点
    pub lru_node: LruNodeData,

    // === Offset 72-95 (24 bytes) ===
    /// CR 信息
    pub cr_info: UnsafeCell<CRInfo>,

    // === Offset 96-111 (16 bytes) ===
    /// 内容锁（紧凑 16 字节，与 C++ contentLwLock 功能等价）
    pub content_lock: ContentLock,

    // === Offset 112-127 (16 bytes reserved) ===
    /// 保留字段（预留给脏页指针、page_version 等后续扩展）
    pub _reserved: [u8; 16],
}

impl BufferDesc {
    /// 创建新的 BufferDesc
    #[inline]
    pub fn new() -> Self {
        Self {
            buf_block: AtomicPtr::new(std::ptr::null_mut()),
            buf_tag: UnsafeCell::new(BufferTag::INVALID),
            _pad1: [0; 4],
            controller: AtomicPtr::new(std::ptr::null_mut()),
            state: AtomicU64::new(0),
            lru_node: LruNodeData::default(),
            cr_info: UnsafeCell::new(CRInfo {
                base: ManuallyDrop::new(CRInfoBase {
                    cr_buffer: AtomicPtr::new(std::ptr::null_mut()),
                    cr_page_max_csn: AtomicU64::new(0),
                    is_usable: AtomicBool::new(false),
                    _padding: [0; 7],
                }),
            }),
            content_lock: ContentLock::new(),
            _reserved: [0; 16],
        }
    }

    /// 获取当前状态
    #[inline]
    pub fn get_state(&self) -> BufferState {
        BufferState::from_raw(self.state.load(Ordering::Acquire))
    }

    /// 获取引用计数
    #[inline]
    pub fn ref_count(&self) -> u32 {
        self.get_state().ref_count()
    }

    /// 检查是否被 pin
    #[inline]
    pub fn is_pinned(&self) -> bool {
        self.ref_count() > 0
    }

    /// 增加引用计数（Pin）
    ///
    /// 返回新的引用计数
    #[inline]
    pub fn pin(&self) -> u32 {
        let old_state = self.state.fetch_add(1, Ordering::AcqRel);
        BufferState::from_raw(old_state).ref_count() + 1
    }

    /// 减少引用计数（Unpin）
    ///
    /// 返回新的引用计数
    #[inline]
    pub fn unpin(&self) -> u32 {
        let old_state = self.state.fetch_sub(1, Ordering::AcqRel);
        let old_count = BufferState::from_raw(old_state).ref_count();
        old_count.saturating_sub(1)
    }

    /// 检查是否脏页
    #[inline]
    pub fn is_dirty(&self) -> bool {
        self.get_state().is_dirty()
    }

    /// 检查是否有效
    #[inline]
    pub fn is_valid(&self) -> bool {
        self.get_state().is_valid()
    }

    /// 获取 BufferTag
    #[inline]
    pub fn tag(&self) -> BufferTag {
        // SAFETY: reading BufferTag is safe, but modifying requires lock
        unsafe { *self.buf_tag.get() }
    }

    /// 设置 BufferTag
    ///
    /// # Safety
    /// 必须持有 header lock
    #[inline]
    pub unsafe fn set_tag(&self, tag: BufferTag) {
        // SAFETY: Caller ensures we hold the header lock
        unsafe {
            *self.buf_tag.get() = tag;
        }
    }

    /// 尝试锁定 header
    ///
    /// 使用 CAS 操作获取 header lock
    #[inline]
    pub fn try_lock_header(&self) -> bool {
        let old_state = self.state.load(Ordering::Acquire);
        if BufferState::from_raw(old_state).is_locked() {
            return false;
        }
        let desired = old_state | state_bits::BM_LOCKED;
        self.state.compare_exchange_weak(
            old_state,
            desired,
            Ordering::AcqRel,
            Ordering::Acquire,
        ).is_ok()
    }

    /// 解锁 header（使用原子 fetch_and 避免覆盖并发 pin/dirty 更新）
    #[inline]
    pub fn unlock_header(&self) {
        self.state.fetch_and(!state_bits::BM_LOCKED, Ordering::Release);
    }

    /// 设置脏页标志
    #[inline]
    pub fn set_dirty(&self) {
        loop {
            let old_state = self.state.load(Ordering::Acquire);
            if (old_state & state_bits::BM_DIRTY) != 0 {
                return; // Already dirty
            }
            let new_state = old_state | state_bits::BM_DIRTY;
            if self.state.compare_exchange_weak(
                old_state,
                new_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ).is_ok() {
                return;
            }
        }
    }

    /// 清除脏页标志
    #[inline]
    pub fn clear_dirty(&self) {
        loop {
            let old_state = self.state.load(Ordering::Acquire);
            if (old_state & state_bits::BM_DIRTY) == 0 {
                return; // Not dirty
            }
            let new_state = old_state & !state_bits::BM_DIRTY;
            if self.state.compare_exchange_weak(
                old_state,
                new_state,
                Ordering::AcqRel,
                Ordering::Acquire,
            ).is_ok() {
                return;
            }
        }
    }

    /// 获取页面数据指针
    #[inline]
    pub fn page_data(&self) -> *mut u8 {
        self.buf_block.load(Ordering::Acquire)
    }

}

impl Default for BufferDesc {
    fn default() -> Self {
        Self::new()
    }
}

unsafe impl Send for BufferDesc {}
unsafe impl Sync for BufferDesc {}

// 编译时验证大小和对齐
const _: () = assert!(std::mem::size_of::<BufferDesc>() == 128);
const _: () = assert!(std::mem::align_of::<BufferDesc>() == 128);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_state() {
        let state = BufferState::new();
        assert_eq!(state.ref_count(), 0);
        assert!(!state.is_dirty());
        assert!(!state.is_valid());
        assert!(!state.is_locked());
    }

    #[test]
    fn test_buffer_desc_size() {
        assert_eq!(std::mem::size_of::<BufferDesc>(), 128);
        assert_eq!(std::mem::align_of::<BufferDesc>(), 128);
    }

    #[test]
    fn test_buffer_desc_pin_unpin() {
        let desc = BufferDesc::new();

        assert_eq!(desc.ref_count(), 0);
        assert!(!desc.is_pinned());

        assert_eq!(desc.pin(), 1);
        assert!(desc.is_pinned());

        assert_eq!(desc.pin(), 2);
        assert_eq!(desc.ref_count(), 2);

        assert_eq!(desc.unpin(), 1);
        assert_eq!(desc.ref_count(), 1);

        assert_eq!(desc.unpin(), 0);
        assert!(!desc.is_pinned());
    }

    #[test]
    fn test_buffer_desc_header_lock() {
        let desc = BufferDesc::new();

        // 初始未锁定
        assert!(!desc.get_state().is_locked());

        // 获取锁
        assert!(desc.try_lock_header());
        assert!(desc.get_state().is_locked());

        // 再次获取失败
        assert!(!desc.try_lock_header());

        // 解锁
        desc.unlock_header();
        assert!(!desc.get_state().is_locked());
    }

    #[test]
    fn test_lru_node_size() {
        assert_eq!(std::mem::size_of::<LruNodeData>(), 32);
    }

    #[test]
    fn test_cr_info_size() {
        assert_eq!(std::mem::size_of::<CRInfoBase>(), 24);
        assert_eq!(std::mem::size_of::<CRInfoCr>(), 24);
        assert_eq!(std::mem::size_of::<CRInfo>(), 24);
    }

    #[test]
    fn test_buf_state_accessors() {
        let s = BufferState::new();
        assert!(!s.is_valid());
        assert!(!s.is_io_in_progress());
        assert!(!s.is_checkpoint_needed());
        let s2: BufferState = Default::default();
        let _ = s2;
    }

    #[test]
    fn test_buffer_desc_accessors() {
        let desc = BufferDesc::new();
        assert!(!desc.is_valid());
        let _ = desc.page_data();
        desc.pin();
        assert!(desc.is_pinned());
        desc.unpin();
    }
}
