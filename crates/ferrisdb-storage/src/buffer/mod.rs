//! Buffer Pool 模块
//!
//! 管理内存中的页面缓存。
//!
//! # 分布式缓冲
//!
//! 支持分布式缓冲架构（PO/PD/RN 三角色模式）。

mod desc;
mod table;
mod lru;
mod pool;
pub mod distributed;

pub use desc::{BufferDesc, BufferState, CRInfo, LruNodeData};
pub use table::{BufTable, BufTableEntry};
pub use lru::{LruQueue, LruNode};
pub use pool::{BufferPool, BufferPoolConfig, PinnedBuffer, BackgroundWriter};
pub use distributed::{
    BufferRole, DistributedBufferConfig, DistributedBufferDesc,
    PageLocation, PageState, PageOwner, PageDirectory, ReadNode,
    BufRpcMessage, BufRpcMessageType, BufRpcResult,
};
