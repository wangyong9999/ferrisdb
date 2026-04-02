//! Page Directory 实现
//!
//! Page Directory (PD) 负责跟踪页面位置并分发读授权。
//!
//! # 职责
//!
//! - 跟踪页面位置（哪个 PO 拥有页面）
//! - 分发读授权
//! - 管理页面所有权
//! - 处理页面查找请求

use super::rpc::{BufRpcMessage, BufRpcMessageType, BufRpcResult};
use super::{BufferRole, DistributedBufferConfig, PageLocation, PageState};
use ferrisdb_core::{BufferTag, Lsn};
use ferrisdb_core::atomic::{AtomicU32, AtomicU64, Ordering};
use ferrisdb_core::error::{FerrisDBError, BufferError, Result};
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;

/// 页面目录条目
#[derive(Debug, Clone)]
struct PageDirectoryEntry {
    /// 页面位置
    location: PageLocation,
    /// 最后访问时间
    last_access: u64,
    /// 访问计数
    access_count: u64,
}

impl PageDirectoryEntry {
    fn new(location: PageLocation) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            location,
            last_access: timestamp,
            access_count: 0,
        }
    }
}

/// Page Directory
///
/// 跟踪页面位置并分发读授权。
pub struct PageDirectory {
    /// 配置
    config: DistributedBufferConfig,
    /// 页面目录表
    directory: RwLock<HashMap<BufferTag, PageDirectoryEntry>>,
    /// 节点状态表（节点 ID -> 是否活跃）
    node_status: RwLock<HashMap<u32, bool>>,
    /// 查找计数
    lookup_count: AtomicU64,
    /// 缓存命中
    cache_hits: AtomicU64,
}

impl PageDirectory {
    /// 创建新的 Page Directory
    pub fn new(config: DistributedBufferConfig) -> Self {
        Self {
            config,
            directory: RwLock::new(HashMap::new()),
            node_status: RwLock::new(HashMap::new()),
            lookup_count: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
        }
    }

    /// 注册页面
    pub fn register_page(&self, tag: BufferTag, owner_node_id: u32) -> Result<()> {
        let location = PageLocation {
            node_id: owner_node_id,
            state: PageState::Clean,
            last_update: 0,
        };

        let entry = PageDirectoryEntry::new(location);

        let mut directory = self.directory.write();
        directory.insert(tag, entry);

        Ok(())
    }

    /// 注销页面
    pub fn unregister_page(&self, tag: BufferTag) -> Result<()> {
        let mut directory = self.directory.write();
        directory.remove(&tag);
        Ok(())
    }

    /// 查找页面位置
    pub fn lookup(&self, tag: BufferTag) -> Option<PageLocation> {
        self.lookup_count.fetch_add(1, Ordering::Relaxed);

        let directory = self.directory.read();
        if let Some(entry) = directory.get(&tag) {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.location)
        } else {
            None
        }
    }

    /// 更新页面状态
    pub fn update_state(&self, tag: BufferTag, state: PageState) -> Result<()> {
        let mut directory = self.directory.write();
        if let Some(entry) = directory.get_mut(&tag) {
            entry.location.state = state;
            entry.location.last_update = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
        }
        Ok(())
    }

    /// 转移页面所有权
    pub fn transfer_ownership(&self, tag: BufferTag, new_owner: u32) -> Result<()> {
        let mut directory = self.directory.write();
        if let Some(entry) = directory.get_mut(&tag) {
            entry.location.node_id = new_owner;
            entry.location.last_update = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            Ok(())
        } else {
            Err(FerrisDBError::Buffer(BufferError::NotFound))
        }
    }

    /// 注册节点
    pub fn register_node(&self, node_id: u32) {
        let mut status = self.node_status.write();
        status.insert(node_id, true);
    }

    /// 注销节点
    pub fn unregister_node(&self, node_id: u32) {
        let mut status = self.node_status.write();
        status.insert(node_id, false);
    }

    /// 检查节点是否活跃
    pub fn is_node_active(&self, node_id: u32) -> bool {
        let status = self.node_status.read();
        status.get(&node_id).copied().unwrap_or(false)
    }

    /// 处理 RPC 请求
    pub fn handle_rpc(&self, msg: BufRpcMessage) -> BufRpcResult {
        match msg.msg_type() {
            BufRpcMessageType::RequestPageToPdOwner => {
                self.handle_page_location_request(msg)
            }
            BufRpcMessageType::RequestPageList => {
                self.handle_page_list_request(msg)
            }
            BufRpcMessageType::PushPageList => {
                self.handle_push_page_list(msg)
            }
            BufRpcMessageType::HeartbeatRequest => {
                BufRpcResult::Success
            }
            BufRpcMessageType::StateSyncRequest => {
                self.handle_state_sync(msg)
            }
            _ => BufRpcResult::Failed,
        }
    }

    /// 处理页面位置请求
    fn handle_page_location_request(&self, msg: BufRpcMessage) -> BufRpcResult {
        if msg.payload.len() < 12 {
            return BufRpcResult::Failed;
        }

        let tag = BufferTag::from_bytes(&msg.payload[0..12]);

        match self.lookup(tag) {
            Some(location) => {
                // 检查节点是否活跃
                if self.is_node_active(location.node_id) {
                    BufRpcResult::Success
                } else {
                    BufRpcResult::Failed
                }
            }
            None => BufRpcResult::Failed,
        }
    }

    /// 处理页面列表请求
    fn handle_page_list_request(&self, _msg: BufRpcMessage) -> BufRpcResult {
        // 在实际实现中，这里会返回页面列表
        BufRpcResult::Success
    }

    /// 处理推送页面列表
    fn handle_push_page_list(&self, msg: BufRpcMessage) -> BufRpcResult {
        // 解析页面列表并更新目录
        // 每个条目 16 字节：12 字节 BufferTag + 4 字节 node_id
        let entry_size = 16;
        if msg.payload.len() % entry_size != 0 {
            return BufRpcResult::Failed;
        }

        let count = msg.payload.len() / entry_size;
        for i in 0..count {
            let offset = i * entry_size;
            let tag = BufferTag::from_bytes(&msg.payload[offset..offset + 12]);
            let node_id = u32::from_le_bytes(
                msg.payload[offset + 12..offset + 16].try_into().unwrap_or([0; 4])
            );

            let _ = self.register_page(tag, node_id);
        }

        BufRpcResult::Success
    }

    /// 处理状态同步
    fn handle_state_sync(&self, _msg: BufRpcMessage) -> BufRpcResult {
        // 在实际实现中，这里会同步状态信息
        BufRpcResult::Success
    }

    /// 获取统计信息
    pub fn stats(&self) -> PageDirectoryStats {
        PageDirectoryStats {
            page_count: self.directory.read().len(),
            node_count: self.node_status.read().len(),
            lookup_count: self.lookup_count.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
        }
    }

    /// 获取所有页面
    pub fn get_all_pages(&self) -> Vec<(BufferTag, PageLocation)> {
        let directory = self.directory.read();
        directory
            .iter()
            .map(|(tag, entry)| (*tag, entry.location))
            .collect()
    }
}

/// Page Directory 统计信息
#[derive(Debug, Clone, Copy, Default)]
pub struct PageDirectoryStats {
    /// 页面数量
    pub page_count: usize,
    /// 节点数量
    pub node_count: usize,
    /// 查找次数
    pub lookup_count: u64,
    /// 缓存命中
    pub cache_hits: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrisdb_core::{PageId, PdbId};

    fn test_config() -> DistributedBufferConfig {
        DistributedBufferConfig {
            node_id: 0,
            role: BufferRole::PageDirectory,
            buffer_size: 100,
            rpc_timeout_ms: 1000,
            max_retries: 3,
        }
    }

    fn test_tag() -> BufferTag {
        BufferTag::new(PdbId::new(1), 1, 100)
    }

    #[test]
    fn test_page_directory_create() {
        let pd = PageDirectory::new(test_config());
        let stats = pd.stats();
        assert_eq!(stats.page_count, 0);
    }

    #[test]
    fn test_page_directory_register() {
        let pd = PageDirectory::new(test_config());
        let tag = test_tag();

        pd.register_page(tag, 1).unwrap();
        assert_eq!(pd.stats().page_count, 1);

        let location = pd.lookup(tag).unwrap();
        assert_eq!(location.node_id, 1);
    }

    #[test]
    fn test_page_directory_unregister() {
        let pd = PageDirectory::new(test_config());
        let tag = test_tag();

        pd.register_page(tag, 1).unwrap();
        pd.unregister_page(tag).unwrap();
        assert_eq!(pd.stats().page_count, 0);
    }

    #[test]
    fn test_page_directory_transfer() {
        let pd = PageDirectory::new(test_config());
        let tag = test_tag();

        pd.register_page(tag, 1).unwrap();
        pd.transfer_ownership(tag, 2).unwrap();

        let location = pd.lookup(tag).unwrap();
        assert_eq!(location.node_id, 2);
    }

    #[test]
    fn test_page_directory_node_status() {
        let pd = PageDirectory::new(test_config());

        pd.register_node(1);
        assert!(pd.is_node_active(1));

        pd.unregister_node(1);
        assert!(!pd.is_node_active(1));
    }

    #[test]
    fn test_page_directory_handle_page_list() {
        let pd = PageDirectory::new(test_config());
        let tag = test_tag();

        // 构造页面列表
        let mut payload = Vec::new();
        payload.extend_from_slice(&tag.to_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes());

        let msg = BufRpcMessage::new(
            BufRpcMessageType::PushPageList,
            1,
            0,
            payload,
        );

        let result = pd.handle_rpc(msg);
        assert_eq!(result, BufRpcResult::Success);
        assert_eq!(pd.stats().page_count, 1);
    }
}
