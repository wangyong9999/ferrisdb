//! 共享内存兼容的双向链表

mod dlist_shmem;

pub use dlist_shmem::{ShmDListHead, ShmDListNode, INVALID_NODE_INDEX};

/// 标准双向链表节点（非共享内存版本）
#[derive(Debug, Default)]
#[repr(C)]
pub struct DListNode {
    /// 前驱指针
    pub prev: *mut DListNode,
    /// 后继指针
    pub next: *mut DListNode,
}

/// 标准双向链表头（非共享内存版本）
#[derive(Debug, Default)]
#[repr(C)]
pub struct DListHead {
    /// 头节点
    pub head: *mut DListNode,
    /// 尾节点
    pub tail: *mut DListNode,
}

// 安全实现：链表节点可以在线程间共享
unsafe impl Send for DListNode {}
unsafe impl Sync for DListNode {}
unsafe impl Send for DListHead {}
unsafe impl Sync for DListHead {}

impl DListHead {
    /// 创建空链表
    pub const fn new() -> Self {
        Self {
            head: std::ptr::null_mut(),
            tail: std::ptr::null_mut(),
        }
    }

    /// 检查链表是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.is_null()
    }
}
