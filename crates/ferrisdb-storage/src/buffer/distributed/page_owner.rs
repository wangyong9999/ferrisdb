//! Page Owner 实现
//!
//! Page Owner (PO) 负责拥有页面并进行写操作。
//!
//! # 职责
//!
//! - 拥有页面的所有权
//! - 处理写操作
//! - 刷脏页
//! - 响应读请求

use super::rpc::{BufRpcMessage, BufRpcMessageType, BufRpcResult};
use super::{BufferRole, DistributedBufferConfig, DistributedBufferDesc, PageLocation, PageState};
use ferrisdb_core::{BufferTag, Lsn};
use ferrisdb_core::atomic::{AtomicU32, AtomicU64, AtomicPtr, Ordering};
use ferrisdb_core::error::{FerrisDBError, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// 脏页队列
struct DirtyPageQueue {
    /// 队列容量
    capacity: usize,
    /// 队列数据
    pages: RwLock<Vec<BufferTag>>,
}

impl DirtyPageQueue {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            pages: RwLock::new(Vec::with_capacity(capacity)),
        }
    }

    fn push(&self, tag: BufferTag) {
        let mut pages = self.pages.write();
        if pages.len() >= self.capacity {
            // 队列满，触发刷盘
            pages.clear();
        }
        if !pages.contains(&tag) {
            pages.push(tag);
        }
    }

    fn pop_batch(&self, batch_size: usize) -> Vec<BufferTag> {
        let mut pages = self.pages.write();
        let count = batch_size.min(pages.len());
        let batch: Vec<_> = pages.drain(..count).collect();
        batch
    }

    fn len(&self) -> usize {
        self.pages.read().len()
    }
}

/// Page Owner
///
/// 拥有页面的节点，负责处理写操作。
pub struct PageOwner {
    /// 配置
    config: DistributedBufferConfig,
    /// 页面描述符表
    page_table: RwLock<HashMap<BufferTag, Arc<DistributedBufferDesc>>>,
    /// 脏页队列
    dirty_queue: DirtyPageQueue,
    /// 当前 LSN
    current_lsn: AtomicU64,
    /// 刷盘计数
    flush_count: AtomicU64,
}

impl PageOwner {
    /// 创建新的 Page Owner
    pub fn new(config: DistributedBufferConfig) -> Self {
        let capacity = config.buffer_size;
        Self {
            config,
            page_table: RwLock::new(HashMap::new()),
            dirty_queue: DirtyPageQueue::new(capacity),
            current_lsn: AtomicU64::new(0),
            flush_count: AtomicU64::new(0),
        }
    }

    /// 获取页面
    pub fn get_page(&self, tag: BufferTag) -> Option<Arc<DistributedBufferDesc>> {
        let table = self.page_table.read();
        table.get(&tag).cloned()
    }

    /// 创建页面
    pub fn create_page(&self, tag: BufferTag) -> Result<Arc<DistributedBufferDesc>> {
        let desc = Arc::new(DistributedBufferDesc::new(tag));
        desc.set_role(BufferRole::PageOwner);

        {
            let mut table = self.page_table.write();
            if let Some(existing) = table.insert(tag, Arc::clone(&desc)) {
                // 页面已存在
                return Ok(existing);
            }
        }

        Ok(desc)
    }

    /// 标记脏页
    pub fn mark_dirty(&self, tag: BufferTag) {
        self.dirty_queue.push(tag);

        // 更新页面状态
        if let Some(desc) = self.get_page(tag) {
            unsafe {
                let mut location = desc.location();
                location.state = PageState::Dirty;
                desc.set_location(location);
            }
        }
    }

    /// 刷脏页
    pub fn flush_dirty_pages(&self, batch_size: usize) -> Result<usize> {
        let batch = self.dirty_queue.pop_batch(batch_size);
        let count = batch.len();

        for tag in &batch {
            self.flush_page(*tag)?;
        }

        self.flush_count.fetch_add(count as u64, Ordering::Relaxed);
        Ok(count)
    }

    /// 刷单个页面
    fn flush_page(&self, tag: BufferTag) -> Result<()> {
        if let Some(desc) = self.get_page(tag) {
            unsafe {
                let mut location = desc.location();
                location.state = PageState::Flushing;
                desc.set_location(location);
            }

            // 模拟刷盘操作
            // 在实际实现中，这里会调用 WAL 和存储层

            unsafe {
                let mut location = desc.location();
                location.state = PageState::Clean;
                desc.set_location(location);
            }
        }
        Ok(())
    }

    /// 处理 RPC 请求
    pub fn handle_rpc(&self, msg: BufRpcMessage) -> BufRpcResult {
        match msg.msg_type() {
            BufRpcMessageType::RequestPageToPageOwner => {
                self.handle_page_request(msg)
            }
            BufRpcMessageType::RequestOwnership => {
                self.handle_ownership_request(msg)
            }
            BufRpcMessageType::ReleasePage => {
                self.handle_release_page(msg)
            }
            BufRpcMessageType::HeartbeatRequest => {
                BufRpcResult::Success
            }
            _ => BufRpcResult::Failed,
        }
    }

    /// 处理页面请求
    fn handle_page_request(&self, msg: BufRpcMessage) -> BufRpcResult {
        if msg.payload.len() < 20 {
            return BufRpcResult::Failed;
        }

        // 解析 BufferTag
        let tag = BufferTag::from_bytes(&msg.payload[0..12]);
        let _lsn = Lsn::from_raw(u64::from_le_bytes(
            msg.payload[12..20].try_into().unwrap_or([0; 8])
        ));

        // 查找页面
        if self.get_page(tag).is_some() {
            // 在实际实现中，这里会构造包含页面数据的响应
            BufRpcResult::Success
        } else {
            BufRpcResult::Failed
        }
    }

    /// 处理所有权请求
    fn handle_ownership_request(&self, _msg: BufRpcMessage) -> BufRpcResult {
        // 在实际实现中，这里会转移页面所有权
        BufRpcResult::Failed
    }

    /// 处理页面释放
    fn handle_release_page(&self, msg: BufRpcMessage) -> BufRpcResult {
        if msg.payload.len() < 12 {
            return BufRpcResult::Failed;
        }

        let tag = BufferTag::from_bytes(&msg.payload[0..12]);

        // 减少远程引用计数
        if let Some(desc) = self.get_page(tag) {
            desc.dec_remote_ref();
        }

        BufRpcResult::Success
    }

    /// 获取统计信息
    pub fn stats(&self) -> PageOwnerStats {
        PageOwnerStats {
            page_count: self.page_table.read().len(),
            dirty_count: self.dirty_queue.len(),
            flush_count: self.flush_count.load(Ordering::Relaxed),
            current_lsn: self.current_lsn.load(Ordering::Relaxed),
        }
    }
}

/// Page Owner 统计信息
#[derive(Debug, Clone, Copy, Default)]
pub struct PageOwnerStats {
    /// 页面数量
    pub page_count: usize,
    /// 脏页数量
    pub dirty_count: usize,
    /// 刷盘次数
    pub flush_count: u64,
    /// 当前 LSN
    pub current_lsn: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrisdb_core::{PageId, PdbId};

    fn test_config() -> DistributedBufferConfig {
        DistributedBufferConfig {
            node_id: 1,
            role: BufferRole::PageOwner,
            buffer_size: 100,
            rpc_timeout_ms: 1000,
            max_retries: 3,
        }
    }

    fn test_tag() -> BufferTag {
        BufferTag::new(PdbId::new(1), 1, 100)
    }

    #[test]
    fn test_page_owner_create() {
        let owner = PageOwner::new(test_config());
        let stats = owner.stats();
        assert_eq!(stats.page_count, 0);
    }

    #[test]
    fn test_page_owner_create_page() {
        let owner = PageOwner::new(test_config());
        let tag = test_tag();

        let desc = owner.create_page(tag).unwrap();
        assert_eq!(desc.role(), BufferRole::PageOwner);
        assert_eq!(owner.stats().page_count, 1);
    }

    #[test]
    fn test_page_owner_mark_dirty() {
        let owner = PageOwner::new(test_config());
        let tag = test_tag();

        owner.create_page(tag).unwrap();
        owner.mark_dirty(tag);

        let stats = owner.stats();
        assert_eq!(stats.dirty_count, 1);
    }

    #[test]
    fn test_page_owner_flush() {
        let owner = PageOwner::new(test_config());
        let tag = test_tag();

        owner.create_page(tag).unwrap();
        owner.mark_dirty(tag);

        let count = owner.flush_dirty_pages(10).unwrap();
        assert_eq!(count, 1);

        let stats = owner.stats();
        assert_eq!(stats.dirty_count, 0);
        assert_eq!(stats.flush_count, 1);
    }

    #[test]
    fn test_page_owner_handle_page_request() {
        let owner = PageOwner::new(test_config());
        let tag = test_tag();

        // 先创建页面
        owner.create_page(tag).unwrap();

        // 构造请求消息
        let mut payload = Vec::new();
        payload.extend_from_slice(&tag.to_bytes());
        payload.extend_from_slice(&0u64.to_le_bytes());

        let msg = BufRpcMessage::new(
            BufRpcMessageType::RequestPageToPageOwner,
            2, // src
            1, // dst
            payload,
        );

        let result = owner.handle_rpc(msg);
        assert_eq!(result, BufRpcResult::Success);
    }
}
