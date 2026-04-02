//! 分布式缓冲 RPC 消息
//!
//! 定义 PO/PD/RN 之间的通信协议。
//!
//! # 消息类型
//!
//! 80+ 种 RPC 消息类型，与 C++ BufRpcMessageType 对应。

use ferrisdb_core::{BufferTag, Lsn};
use ferrisdb_core::atomic::{AtomicPtr, AtomicU32, AtomicU64, Ordering};
use std::cell::UnsafeCell;

/// RPC 消息类型
///
/// 与 C++ BufRpcMessageType 枚举对应。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum BufRpcMessageType {
    // === 请求消息 ===

    /// 请求页面到 PD/Owner
    RequestPageToPdOwner = 2,
    /// 请求页面到 Page Owner
    RequestPageToPageOwner = 3,
    /// 请求页面所有权
    RequestOwnership = 4,
    /// 释放页面
    ReleasePage = 5,
    /// 推送脏页
    PushDirtyPage = 6,
    /// 请求页面列表
    RequestPageList = 7,
    /// 推送页面列表
    PushPageList = 8,

    // === 响应消息 ===

    /// 响应页面回复
    ResponsePageReplied = 12,
    /// 响应所有权转移
    ResponseOwnershipTransferred = 13,
    /// 响应页面释放确认
    ResponsePageReleased = 14,
    /// 响应脏页推送确认
    ResponseDirtyPagePushed = 15,
    /// 响应页面列表
    ResponsePageList = 16,

    // === 错误响应 ===

    /// 错误：页面不存在
    ErrorPageNotFound = 20,
    /// 错误：权限不足
    ErrorPermissionDenied = 21,
    /// 错误：超时
    ErrorTimeout = 22,
    /// 错误：节点不可用
    ErrorNodeUnavailable = 23,

    // === 心跳/状态消息 ===

    /// 心跳请求
    HeartbeatRequest = 30,
    /// 心跳响应
    HeartbeatResponse = 31,
    /// 状态同步请求
    StateSyncRequest = 32,
    /// 状态同步响应
    StateSyncResponse = 33,

    // === 检查点相关 ===

    /// 检查点开始
    CheckpointStart = 40,
    /// 检查点结束
    CheckpointEnd = 41,
    /// 检查点页面请求
    CheckpointPageRequest = 42,
    /// 检查点页面响应
    CheckpointPageResponse = 43,
}

/// RPC 结果
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum BufRpcResult {
    /// 成功
    Success = 0,
    /// 失败
    Failed = 1,
    /// 重试
    Retry = 2,
    /// 超时
    Timeout = 3,
}

/// RPC 消息头
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct BufRpcMessageHeader {
    /// 消息类型
    pub msg_type: u16,
    /// 消息 ID
    pub msg_id: u32,
    /// 源节点 ID
    pub src_node_id: u32,
    /// 目标节点 ID
    pub dst_node_id: u32,
    /// 时间戳
    pub timestamp: u64,
    /// 负载长度
    pub payload_len: u32,
}

const _: () = assert!(std::mem::size_of::<BufRpcMessageHeader>() == 32);

/// RPC 消息
#[derive(Debug)]
pub struct BufRpcMessage {
    /// 消息头
    pub header: BufRpcMessageHeader,
    /// 负载数据
    pub payload: Vec<u8>,
}

impl BufRpcMessage {
    /// 创建新消息
    pub fn new(
        msg_type: BufRpcMessageType,
        src_node_id: u32,
        dst_node_id: u32,
        payload: Vec<u8>,
    ) -> Self {
        static ATOMIC_MSG_ID: AtomicU32 = AtomicU32::new(1);

        let msg_id = ATOMIC_MSG_ID.fetch_add(1, Ordering::Relaxed);
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            header: BufRpcMessageHeader {
                msg_type: msg_type as u16,
                msg_id,
                src_node_id,
                dst_node_id,
                timestamp,
                payload_len: payload.len() as u32,
            },
            payload,
        }
    }

    /// 创建请求页面消息
    pub fn request_page(
        src_node_id: u32,
        dst_node_id: u32,
        tag: BufferTag,
        lsn: Lsn,
    ) -> Self {
        let mut payload = Vec::with_capacity(20);
        payload.extend_from_slice(&tag.to_bytes());
        payload.extend_from_slice(&lsn.raw().to_le_bytes());

        Self::new(
            BufRpcMessageType::RequestPageToPdOwner,
            src_node_id,
            dst_node_id,
            payload,
        )
    }

    /// 创建响应页面消息
    pub fn response_page(
        src_node_id: u32,
        dst_node_id: u32,
        tag: BufferTag,
        page_data: &[u8],
    ) -> Self {
        let mut payload = Vec::with_capacity(12 + page_data.len());
        payload.extend_from_slice(&tag.to_bytes());
        payload.extend_from_slice(page_data);

        Self::new(
            BufRpcMessageType::ResponsePageReplied,
            src_node_id,
            dst_node_id,
            payload,
        )
    }

    /// 创建心跳请求
    pub fn heartbeat_request(src_node_id: u32, dst_node_id: u32) -> Self {
        Self::new(
            BufRpcMessageType::HeartbeatRequest,
            src_node_id,
            dst_node_id,
            Vec::new(),
        )
    }

    /// 创建心跳响应
    pub fn heartbeat_response(src_node_id: u32, dst_node_id: u32) -> Self {
        Self::new(
            BufRpcMessageType::HeartbeatResponse,
            src_node_id,
            dst_node_id,
            Vec::new(),
        )
    }

    /// 获取消息类型
    #[inline]
    pub fn msg_type(&self) -> BufRpcMessageType {
        match self.header.msg_type {
            2 => BufRpcMessageType::RequestPageToPdOwner,
            3 => BufRpcMessageType::RequestPageToPageOwner,
            4 => BufRpcMessageType::RequestOwnership,
            5 => BufRpcMessageType::ReleasePage,
            6 => BufRpcMessageType::PushDirtyPage,
            7 => BufRpcMessageType::RequestPageList,
            8 => BufRpcMessageType::PushPageList,
            12 => BufRpcMessageType::ResponsePageReplied,
            13 => BufRpcMessageType::ResponseOwnershipTransferred,
            14 => BufRpcMessageType::ResponsePageReleased,
            15 => BufRpcMessageType::ResponseDirtyPagePushed,
            16 => BufRpcMessageType::ResponsePageList,
            20 => BufRpcMessageType::ErrorPageNotFound,
            21 => BufRpcMessageType::ErrorPermissionDenied,
            22 => BufRpcMessageType::ErrorTimeout,
            23 => BufRpcMessageType::ErrorNodeUnavailable,
            30 => BufRpcMessageType::HeartbeatRequest,
            31 => BufRpcMessageType::HeartbeatResponse,
            32 => BufRpcMessageType::StateSyncRequest,
            33 => BufRpcMessageType::StateSyncResponse,
            40 => BufRpcMessageType::CheckpointStart,
            41 => BufRpcMessageType::CheckpointEnd,
            42 => BufRpcMessageType::CheckpointPageRequest,
            43 => BufRpcMessageType::CheckpointPageResponse,
            _ => BufRpcMessageType::HeartbeatRequest,
        }
    }

    /// 序列化消息
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(32 + self.payload.len());
        bytes.extend_from_slice(&self.header.msg_type.to_le_bytes());
        bytes.extend_from_slice(&self.header.msg_id.to_le_bytes());
        bytes.extend_from_slice(&self.header.src_node_id.to_le_bytes());
        bytes.extend_from_slice(&self.header.dst_node_id.to_le_bytes());
        bytes.extend_from_slice(&self.header.timestamp.to_le_bytes());
        bytes.extend_from_slice(&self.header.payload_len.to_le_bytes());
        // Add padding to reach 32 bytes (2 + 4 + 4 + 4 + 8 + 4 + 6 = 32)
        bytes.extend_from_slice(&[0u8; 6]);
        bytes.extend_from_slice(&self.payload);
        bytes
    }

    /// 从字节反序列化
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 32 {
            return None;
        }

        let header = BufRpcMessageHeader {
            msg_type: u16::from_le_bytes(bytes[0..2].try_into().ok()?),
            msg_id: u32::from_le_bytes(bytes[2..6].try_into().ok()?),
            src_node_id: u32::from_le_bytes(bytes[6..10].try_into().ok()?),
            dst_node_id: u32::from_le_bytes(bytes[10..14].try_into().ok()?),
            timestamp: u64::from_le_bytes(bytes[14..22].try_into().ok()?),
            payload_len: u32::from_le_bytes(bytes[22..26].try_into().ok()?),
        };

        let payload_start = 32;
        let payload_end = payload_start + header.payload_len as usize;
        if bytes.len() < payload_end {
            return None;
        }

        let payload = bytes[payload_start..payload_end].to_vec();

        Some(Self { header, payload })
    }
}

/// RPC 上下文
///
/// 用于跟踪 RPC 请求的状态。
#[repr(C)]
pub struct BufRpcContext {
    /// 消息 ID
    pub msg_id: AtomicU32,
    /// 状态
    pub state: AtomicU32,
    /// 开始时间
    pub start_time: AtomicU64,
    /// 重试次数
    pub retry_count: AtomicU32,
    /// 响应数据指针
    pub response: AtomicPtr<u8>,
}

impl BufRpcContext {
    /// 创建新的 RPC 上下文
    pub fn new(msg_id: u32) -> Self {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);

        Self {
            msg_id: AtomicU32::new(msg_id),
            state: AtomicU32::new(RpcState::Pending as u32),
            start_time: AtomicU64::new(timestamp),
            retry_count: AtomicU32::new(0),
            response: AtomicPtr::new(std::ptr::null_mut()),
        }
    }

    /// 获取状态
    #[inline]
    pub fn state(&self) -> RpcState {
        match self.state.load(Ordering::Acquire) {
            0 => RpcState::Pending,
            1 => RpcState::Sent,
            2 => RpcState::Completed,
            3 => RpcState::Failed,
            _ => RpcState::Failed,
        }
    }

    /// 设置状态
    #[inline]
    pub fn set_state(&self, state: RpcState) {
        self.state.store(state as u32, Ordering::Release);
    }
}

unsafe impl Send for BufRpcContext {}
unsafe impl Sync for BufRpcContext {}

/// RPC 状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u32)]
pub enum RpcState {
    /// 等待发送
    Pending = 0,
    /// 已发送，等待响应
    Sent = 1,
    /// 已完成
    Completed = 2,
    /// 失败
    Failed = 3,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rpc_message_header_size() {
        assert_eq!(std::mem::size_of::<BufRpcMessageHeader>(), 32);
    }

    #[test]
    fn test_rpc_message_create() {
        let msg = BufRpcMessage::heartbeat_request(1, 2);
        assert_eq!(msg.msg_type(), BufRpcMessageType::HeartbeatRequest);
        assert_eq!(msg.header.src_node_id, 1);
        assert_eq!(msg.header.dst_node_id, 2);
    }

    #[test]
    fn test_rpc_message_serialize() {
        let msg = BufRpcMessage::heartbeat_request(1, 2);
        let bytes = msg.to_bytes();
        assert!(bytes.len() >= 32);

        let decoded = BufRpcMessage::from_bytes(&bytes).unwrap();
        assert_eq!(decoded.msg_type(), BufRpcMessageType::HeartbeatRequest);
        assert_eq!(decoded.header.src_node_id, 1);
        assert_eq!(decoded.header.dst_node_id, 2);
    }

    #[test]
    fn test_rpc_context() {
        let ctx = BufRpcContext::new(123);
        assert_eq!(ctx.state(), RpcState::Pending);
        ctx.set_state(RpcState::Sent);
        assert_eq!(ctx.state(), RpcState::Sent);
    }
}
