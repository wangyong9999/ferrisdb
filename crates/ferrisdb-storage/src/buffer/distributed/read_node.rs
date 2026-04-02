//! Read Node 实现
//!
//! Read Node (RN) 负责只读缓存页面。
//!
//! # 职责
//!
//! - 缓存只读页面
//! - 请求页面到 PD/PO
//! - 释放页面
//! - 本地读取优化

use super::rpc::{BufRpcMessage, BufRpcMessageType, BufRpcResult, RpcState, BufRpcContext};
use super::{BufferRole, DistributedBufferConfig, DistributedBufferDesc, PageLocation, PageState};
use ferrisdb_core::{BufferTag, Lsn};
use ferrisdb_core::atomic::{AtomicU32, AtomicU64, AtomicPtr, Ordering};
use ferrisdb_core::error::{FerrisDBError, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

/// 页面缓存条目
struct CacheEntry {
    /// 页面描述符
    desc: Arc<DistributedBufferDesc>,
    /// 页面数据
    data: Option<Vec<u8>>,
    /// 最后访问时间
    last_access: u64,
}

/// Read Node
///
/// 只读缓存节点。
pub struct ReadNode {
    /// 配置
    config: DistributedBufferConfig,
    /// 本地缓存
    cache: RwLock<HashMap<BufferTag, CacheEntry>>,
    /// RPC 上下文表
    rpc_contexts: RwLock<HashMap<u32, Arc<BufRpcContext>>>,
    /// Page Directory 节点 ID
    pd_node_id: AtomicU32,
    /// 缓存命中
    cache_hits: AtomicU64,
    /// 缓存未命中
    cache_misses: AtomicU64,
    /// 请求计数
    request_count: AtomicU64,
}

impl ReadNode {
    /// 创建新的 Read Node
    pub fn new(config: DistributedBufferConfig) -> Self {
        Self {
            config,
            cache: RwLock::new(HashMap::new()),
            rpc_contexts: RwLock::new(HashMap::new()),
            pd_node_id: AtomicU32::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
            request_count: AtomicU64::new(0),
        }
    }

    /// 设置 Page Directory 节点 ID
    pub fn set_pd_node(&self, node_id: u32) {
        self.pd_node_id.store(node_id, Ordering::Release);
    }

    /// 读取页面（本地缓存优先）
    pub fn read_page(&self, tag: BufferTag, _lsn: Lsn) -> Result<Option<Vec<u8>>> {
        self.request_count.fetch_add(1, Ordering::Relaxed);

        // 1. 检查本地缓存
        {
            let cache = self.cache.read();
            if let Some(entry) = cache.get(&tag) {
                self.cache_hits.fetch_add(1, Ordering::Relaxed);

                // 更新访问时间
                unsafe {
                    let desc = &entry.desc;
                    if desc.inc_local_ref() == 1 {
                        // 第一次引用
                    }
                }

                return Ok(entry.data.clone());
            }
        }

        // 2. 缓存未命中
        self.cache_misses.fetch_add(1, Ordering::Relaxed);

        // 3. 请求页面
        let data = self.request_page_from_remote(tag)?;

        // 4. 缓存结果
        if let Some(ref page_data) = data {
            self.cache_page(tag, page_data.clone())?;
        }

        Ok(data)
    }

    /// 从远程请求页面
    fn request_page_from_remote(&self, tag: BufferTag) -> Result<Option<Vec<u8>>> {
        let pd_node = self.pd_node_id.load(Ordering::Acquire);
        if pd_node == 0 {
            return Err(FerrisDBError::InvalidState("PD node not configured".to_string()));
        }

        // 1. 向 PD 请求页面位置
        let location = self.request_page_location(tag, pd_node)?;

        // 2. 向 PO 请求页面数据
        if let Some(loc) = location {
            self.request_page_data(tag, loc.node_id)
        } else {
            Ok(None)
        }
    }

    /// 请求页面位置
    fn request_page_location(&self, tag: BufferTag, pd_node: u32) -> Result<Option<PageLocation>> {
        // 构造请求消息
        let mut payload = Vec::new();
        payload.extend_from_slice(&tag.to_bytes());
        payload.extend_from_slice(&0u64.to_le_bytes()); // LSN

        let msg = BufRpcMessage::new(
            BufRpcMessageType::RequestPageToPdOwner,
            self.config.node_id,
            pd_node,
            payload,
        );

        // 在实际实现中，这里会通过网络发送消息并等待响应
        // 简化实现：假设请求成功
        let _ = self.track_rpc(msg)?;

        // 返回模拟的位置信息
        Ok(Some(PageLocation {
            node_id: pd_node,
            state: PageState::Clean,
            last_update: 0,
        }))
    }

    /// 请求页面数据
    fn request_page_data(&self, tag: BufferTag, owner_node: u32) -> Result<Option<Vec<u8>>> {
        let mut payload = Vec::new();
        payload.extend_from_slice(&tag.to_bytes());
        payload.extend_from_slice(&0u64.to_le_bytes());

        let msg = BufRpcMessage::new(
            BufRpcMessageType::RequestPageToPageOwner,
            self.config.node_id,
            owner_node,
            payload,
        );

        let _ = self.track_rpc(msg)?;

        // 在实际实现中，这里会通过网络发送消息并等待响应
        // 简化实现：返回空数据
        Ok(None)
    }

    /// 跟踪 RPC 请求
    fn track_rpc(&self, msg: BufRpcMessage) -> Result<Arc<BufRpcContext>> {
        let ctx = Arc::new(BufRpcContext::new(msg.header.msg_id));

        {
            let mut contexts = self.rpc_contexts.write();
            contexts.insert(msg.header.msg_id, Arc::clone(&ctx));
        }

        Ok(ctx)
    }

    /// 缓存页面
    fn cache_page(&self, tag: BufferTag, data: Vec<u8>) -> Result<()> {
        let desc = Arc::new(DistributedBufferDesc::new(tag));
        desc.set_role(BufferRole::ReadNode);

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let entry = CacheEntry {
            desc,
            data: Some(data),
            last_access: timestamp,
        };

        {
            let mut cache = self.cache.write();
            cache.insert(tag, entry);
        }

        Ok(())
    }

    /// 释放页面
    pub fn release_page(&self, tag: BufferTag) -> Result<()> {
        let mut cache = self.cache.write();

        if let Some(entry) = cache.get(&tag) {
            entry.desc.dec_local_ref();

            // 如果没有更多引用，从缓存移除
            if entry.desc.dec_local_ref() == 0 {
                cache.remove(&tag);
            }
        }

        Ok(())
    }

    /// 通知 PD 释放页面
    fn notify_release(&self, tag: BufferTag) -> Result<()> {
        let pd_node = self.pd_node_id.load(Ordering::Acquire);
        if pd_node == 0 {
            return Ok(());
        }

        let mut payload = Vec::new();
        payload.extend_from_slice(&tag.to_bytes());

        let msg = BufRpcMessage::new(
            BufRpcMessageType::ReleasePage,
            self.config.node_id,
            pd_node,
            payload,
        );

        let _ = self.track_rpc(msg)?;

        Ok(())
    }

    /// 处理 RPC 请求
    pub fn handle_rpc(&self, msg: BufRpcMessage) -> BufRpcResult {
        match msg.msg_type() {
            BufRpcMessageType::HeartbeatRequest => {
                BufRpcResult::Success
            }
            BufRpcMessageType::StateSyncRequest => {
                BufRpcResult::Success
            }
            BufRpcMessageType::ResponsePageReplied => {
                self.handle_page_response(msg)
            }
            _ => BufRpcResult::Failed,
        }
    }

    /// 处理页面响应
    fn handle_page_response(&self, msg: BufRpcMessage) -> BufRpcResult {
        if msg.payload.len() < 12 {
            return BufRpcResult::Failed;
        }

        let tag = BufferTag::from_bytes(&msg.payload[0..12]);
        let page_data = &msg.payload[12..];

        // 缓存页面
        let _ = self.cache_page(tag, page_data.to_vec());

        BufRpcResult::Success
    }

    /// 清理过期缓存
    pub fn evict_expired(&self, max_age_ms: u64) -> usize {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        let mut cache = self.cache.write();
        let before = cache.len();

        cache.retain(|_, entry| {
            timestamp - entry.last_access < max_age_ms
        });

        before - cache.len()
    }

    /// 获取统计信息
    pub fn stats(&self) -> ReadNodeStats {
        ReadNodeStats {
            cache_size: self.cache.read().len(),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            cache_misses: self.cache_misses.load(Ordering::Relaxed),
            request_count: self.request_count.load(Ordering::Relaxed),
            hit_rate: self.calculate_hit_rate(),
        }
    }

    fn calculate_hit_rate(&self) -> f64 {
        let hits = self.cache_hits.load(Ordering::Relaxed);
        let misses = self.cache_misses.load(Ordering::Relaxed);
        let total = hits + misses;

        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// Read Node 统计信息
#[derive(Debug, Clone, Copy, Default)]
pub struct ReadNodeStats {
    /// 缓存大小
    pub cache_size: usize,
    /// 缓存命中
    pub cache_hits: u64,
    /// 缓存未命中
    pub cache_misses: u64,
    /// 请求次数
    pub request_count: u64,
    /// 命中率
    pub hit_rate: f64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrisdb_core::{PageId, PdbId};

    fn test_config() -> DistributedBufferConfig {
        DistributedBufferConfig {
            node_id: 2,
            role: BufferRole::ReadNode,
            buffer_size: 100,
            rpc_timeout_ms: 1000,
            max_retries: 3,
        }
    }

    fn test_tag() -> BufferTag {
        BufferTag::new(PdbId::new(1), 1, 100)
    }

    #[test]
    fn test_read_node_create() {
        let rn = ReadNode::new(test_config());
        let stats = rn.stats();
        assert_eq!(stats.cache_size, 0);
    }

    #[test]
    fn test_read_node_cache_page() {
        let rn = ReadNode::new(test_config());
        let tag = test_tag();
        let data = vec![1, 2, 3, 4, 5];

        rn.cache_page(tag, data.clone()).unwrap();

        let stats = rn.stats();
        assert_eq!(stats.cache_size, 1);
    }

    #[test]
    fn test_read_node_read_cached() {
        let rn = ReadNode::new(test_config());
        let tag = test_tag();
        let data = vec![1, 2, 3, 4, 5];

        // 先缓存
        rn.cache_page(tag, data.clone()).unwrap();

        // 读取（应该命中缓存）
        let result = rn.read_page(tag, Lsn::INVALID).unwrap();
        assert_eq!(result, Some(data));

        let stats = rn.stats();
        assert_eq!(stats.cache_hits, 1);
        assert!(stats.hit_rate > 0.0);
    }

    #[test]
    fn test_read_node_release_page() {
        let rn = ReadNode::new(test_config());
        let tag = test_tag();
        let data = vec![1, 2, 3, 4, 5];

        rn.cache_page(tag, data).unwrap();
        rn.release_page(tag).unwrap();

        let stats = rn.stats();
        // 根据实现，可能会被移除
    }

    #[test]
    fn test_read_node_evict_expired() {
        let rn = ReadNode::new(test_config());
        let tag = test_tag();

        rn.cache_page(tag, vec![1, 2, 3]).unwrap();

        // 立即清理（max_age_ms = 0 会清理所有）
        let evicted = rn.evict_expired(0);
        assert!(evicted >= 1);

        let stats = rn.stats();
        assert_eq!(stats.cache_size, 0);
    }

    #[test]
    fn test_read_node_handle_page_response() {
        let rn = ReadNode::new(test_config());
        let tag = test_tag();

        let mut payload = Vec::new();
        payload.extend_from_slice(&tag.to_bytes());
        payload.extend_from_slice(&[1u8, 2, 3, 4, 5]);

        let msg = BufRpcMessage::new(
            BufRpcMessageType::ResponsePageReplied,
            1,
            2,
            payload,
        );

        let result = rn.handle_rpc(msg);
        assert_eq!(result, BufRpcResult::Success);
        assert_eq!(rn.stats().cache_size, 1);
    }
}
