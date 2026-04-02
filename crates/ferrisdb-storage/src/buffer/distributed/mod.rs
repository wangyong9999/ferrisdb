//! 分布式缓冲管理
//!
//! 实现分布式缓冲架构，支持 PO/PD/RN 三角色模式。
//!
//! # 架构
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────┐
//! │                     Distributed Buffer Manager                    │
//! ├──────────────────────────────────────────────────────────────────┤
//! │  Page Owner (PO)  │  Page Directory (PD)  │  Read Node (RN)     │
//! │  - 拥有页面       │  - 跟踪页面位置         │  - 只读缓存页面     │
//! │  - 可写           │  - 分发读授权           │  - 请求页面到 PD/PO │
//! │  - 刷脏页         │  - 管理页面所有权       │  - 释放页面         │
//! └──────────────────────────────────────────────────────────────────┘
//! ```
//!
//! # 使用示例
//!
//! ```ignore
//! use ferrisdb_storage::buffer::distributed::{DistributedBufferManager, BufferRole};
//!
//! // 创建 Page Owner
//! let po = DistributedBufferManager::new(BufferRole::PageOwner, config);
//!
//! // 创建 Page Directory
//! let pd = DistributedBufferManager::new(BufferRole::PageDirectory, config);
//!
//! // 创建 Read Node
//! let rn = DistributedBufferManager::new(BufferRole::ReadNode, config);
//! ```

pub mod page_owner;
pub mod page_directory;
pub mod read_node;
pub mod rpc;

pub use page_owner::PageOwner;
pub use page_directory::PageDirectory;
pub use read_node::ReadNode;
pub use rpc::{BufRpcMessage, BufRpcMessageType, BufRpcResult};

use ferrisdb_core::BufferTag;
use ferrisdb_core::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::sync::atomic::AtomicPtr as StdAtomicPtr;
use std::cell::UnsafeCell;

/// 缓冲角色
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BufferRole {
    /// Page Owner: 拥有页面，可写
    PageOwner = 0,
    /// Page Directory: 跟踪页面位置
    PageDirectory = 1,
    /// Read Node: 只读缓存
    ReadNode = 2,
}

impl Default for BufferRole {
    fn default() -> Self {
        Self::ReadNode
    }
}

/// 页面位置信息
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct PageLocation {
    /// 节点 ID（Page Owner 或 Page Directory）
    pub node_id: u32,
    /// 页面状态
    pub state: PageState,
    /// 最后更新时间戳
    pub last_update: u64,
}

impl Default for PageLocation {
    fn default() -> Self {
        Self {
            node_id: 0,
            state: PageState::Invalid,
            last_update: 0,
        }
    }
}

/// 页面状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum PageState {
    /// 无效状态
    Invalid = 0,
    /// 干净页面
    Clean = 1,
    /// 脏页
    Dirty = 2,
    /// 正在加载
    Loading = 3,
    /// 正在刷盘
    Flushing = 4,
}

/// 分布式缓冲描述符
///
/// 扩展本地 BufferDesc，增加分布式信息。
#[repr(C)]
pub struct DistributedBufferDesc {
    /// 页面标签
    pub tag: BufferTag,
    /// 当前角色
    pub role: AtomicU32,
    /// 页面位置（PD 维护）
    pub location: UnsafeCell<PageLocation>,
    /// 远程引用计数
    pub remote_ref_count: AtomicU32,
    /// 本地引用计数
    pub local_ref_count: AtomicU32,
    /// 最后同步 CSN
    pub last_sync_csn: AtomicU64,
    /// RPC 上下文指针
    pub rpc_context: AtomicPtr<u8>,
}

impl DistributedBufferDesc {
    /// 创建新的分布式缓冲描述符
    pub fn new(tag: BufferTag) -> Self {
        Self {
            tag,
            role: AtomicU32::new(BufferRole::ReadNode as u32),
            location: UnsafeCell::new(PageLocation::default()),
            remote_ref_count: AtomicU32::new(0),
            local_ref_count: AtomicU32::new(0),
            last_sync_csn: AtomicU64::new(0),
            rpc_context: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// 获取角色
    #[inline]
    pub fn role(&self) -> BufferRole {
        match self.role.load(Ordering::Acquire) {
            0 => BufferRole::PageOwner,
            1 => BufferRole::PageDirectory,
            _ => BufferRole::ReadNode,
        }
    }

    /// 设置角色
    #[inline]
    pub fn set_role(&self, role: BufferRole) {
        self.role.store(role as u32, Ordering::Release);
    }

    /// 获取页面位置
    ///
    /// # Safety
    /// 调用者必须持有适当的锁
    #[inline]
    pub unsafe fn location(&self) -> PageLocation {
        // SAFETY: Caller ensures proper synchronization
        unsafe { *self.location.get() }
    }

    /// 设置页面位置
    ///
    /// # Safety
    /// 调用者必须持有适当的锁
    #[inline]
    pub unsafe fn set_location(&self, location: PageLocation) {
        // SAFETY: Caller ensures proper synchronization
        unsafe {
            *self.location.get() = location;
        }
    }

    /// 增加远程引用
    #[inline]
    pub fn inc_remote_ref(&self) -> u32 {
        self.remote_ref_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// 减少远程引用
    #[inline]
    pub fn dec_remote_ref(&self) -> u32 {
        self.remote_ref_count.fetch_sub(1, Ordering::AcqRel).saturating_sub(1)
    }

    /// 增加本地引用
    #[inline]
    pub fn inc_local_ref(&self) -> u32 {
        self.local_ref_count.fetch_add(1, Ordering::AcqRel) + 1
    }

    /// 减少本地引用
    #[inline]
    pub fn dec_local_ref(&self) -> u32 {
        self.local_ref_count.fetch_sub(1, Ordering::AcqRel).saturating_sub(1)
    }
}

unsafe impl Send for DistributedBufferDesc {}
unsafe impl Sync for DistributedBufferDesc {}

/// 分布式缓冲配置
#[derive(Debug, Clone)]
pub struct DistributedBufferConfig {
    /// 本节点 ID
    pub node_id: u32,
    /// 角色
    pub role: BufferRole,
    /// 缓冲区大小
    pub buffer_size: usize,
    /// RPC 超时（毫秒）
    pub rpc_timeout_ms: u64,
    /// 最大重试次数
    pub max_retries: u32,
}

impl Default for DistributedBufferConfig {
    fn default() -> Self {
        Self {
            node_id: 0,
            role: BufferRole::ReadNode,
            buffer_size: 1024,
            rpc_timeout_ms: 5000,
            max_retries: 3,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_role() {
        assert_eq!(BufferRole::default(), BufferRole::ReadNode);
        assert_eq!(BufferRole::PageOwner as u8, 0);
        assert_eq!(BufferRole::PageDirectory as u8, 1);
        assert_eq!(BufferRole::ReadNode as u8, 2);
    }

    #[test]
    fn test_page_state() {
        assert_eq!(PageState::Invalid as u8, 0);
        assert_eq!(PageState::Clean as u8, 1);
        assert_eq!(PageState::Dirty as u8, 2);
    }

    #[test]
    fn test_distributed_buffer_desc() {
        let tag = BufferTag::new(
            ferrisdb_core::PdbId::new(1),
            1,
            100,
        );
        let desc = DistributedBufferDesc::new(tag);

        assert_eq!(desc.role(), BufferRole::ReadNode);
        desc.set_role(BufferRole::PageOwner);
        assert_eq!(desc.role(), BufferRole::PageOwner);

        assert_eq!(desc.inc_remote_ref(), 1);
        assert_eq!(desc.inc_remote_ref(), 2);
        assert_eq!(desc.dec_remote_ref(), 1);

        assert_eq!(desc.inc_local_ref(), 1);
        assert_eq!(desc.dec_local_ref(), 0);
    }

    #[test]
    fn test_distributed_buffer_config() {
        let config = DistributedBufferConfig::default();
        assert_eq!(config.role, BufferRole::ReadNode);
        assert_eq!(config.buffer_size, 1024);
    }
}
