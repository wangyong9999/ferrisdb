//! LRU 队列
//!
//! 用于 Buffer Pool 的页面淘汰策略。

use ferrisdb_core::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Mutex;

/// LRU 队列数量
pub const NUM_LRU_QUEUES: usize = 16;

/// LRU 节点（用于链表管理）
#[derive(Debug)]
#[repr(C)]
pub struct LruNode {
    /// 前驱节点索引
    pub prev: AtomicU32,
    /// 后继节点索引
    pub next: AtomicU32,
    /// 最近访问时间
    pub last_access: AtomicU64,
}

impl LruNode {
    /// 创建新的 LRU 节点
    #[inline]
    pub const fn new() -> Self {
        Self {
            prev: AtomicU32::new(u32::MAX),
            next: AtomicU32::new(u32::MAX),
            last_access: AtomicU64::new(0),
        }
    }
}

impl Default for LruNode {
    fn default() -> Self {
        Self::new()
    }
}

/// LRU 队列
///
/// 管理一个 LRU 链表，用于页面淘汰。
#[derive(Debug)]
pub struct LruQueue {
    /// 头节点索引
    head: AtomicU32,
    /// 尾节点索引
    tail: AtomicU32,
    /// 节点数量
    count: AtomicU32,
    /// 队列 ID
    queue_id: u32,
    /// 保护操作的互斥锁
    lock: Mutex<()>,
}

impl LruQueue {
    /// 创建新的 LRU 队列
    #[inline]
    pub const fn new(queue_id: u32) -> Self {
        Self {
            head: AtomicU32::new(u32::MAX),
            tail: AtomicU32::new(u32::MAX),
            count: AtomicU32::new(0),
            queue_id,
            lock: Mutex::new(()),
        }
    }

    /// 检查队列是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.count.load(Ordering::Acquire) == 0
    }

    /// 获取节点数量
    #[inline]
    pub fn len(&self) -> u32 {
        self.count.load(Ordering::Acquire)
    }

    /// 获取头节点索引
    #[inline]
    pub fn head(&self) -> u32 {
        self.head.load(Ordering::Acquire)
    }

    /// 获取尾节点索引
    #[inline]
    pub fn tail(&self) -> u32 {
        self.tail.load(Ordering::Acquire)
    }

    /// 添加节点到头部（最近使用）
    ///
    /// # Safety
    /// - nodes 数组必须包含有效的 LruNode
    /// - node_idx 必须是有效的索引
    /// - 调用者必须持有 lock
    pub unsafe fn add_to_head(&self, nodes: &[crate::buffer::desc::LruNodeData], node_idx: u32) {
        let node = &nodes[node_idx as usize];
        node.prev.store(u32::MAX, Ordering::Relaxed);
        node.next.store(self.head.load(Ordering::Relaxed), Ordering::Relaxed);
        node.queue_id.store(self.queue_id, Ordering::Relaxed);

        let old_head = self.head.swap(node_idx, Ordering::AcqRel);

        if old_head == u32::MAX {
            // 队列为空
            self.tail.store(node_idx, Ordering::Release);
        } else {
            // 更新旧头节点的 prev
            nodes[old_head as usize].prev.store(node_idx, Ordering::Release);
        }

        self.count.fetch_add(1, Ordering::AcqRel);
    }

    /// 从队列中移除节点
    ///
    /// # Safety
    /// - nodes 数组必须包含有效的 LruNode
    /// - node_idx 必须是有效的索引
    /// - 调用者必须持有 lock
    pub unsafe fn remove(&self, nodes: &[crate::buffer::desc::LruNodeData], node_idx: u32) {
        let node = &nodes[node_idx as usize];
        let prev_idx = node.prev.load(Ordering::Acquire);
        let next_idx = node.next.load(Ordering::Acquire);

        if prev_idx == u32::MAX {
            // node 是 head
            self.head.store(next_idx, Ordering::Release);
        } else {
            nodes[prev_idx as usize].next.store(next_idx, Ordering::Release);
        }

        if next_idx == u32::MAX {
            // node 是 tail
            self.tail.store(prev_idx, Ordering::Release);
        } else {
            nodes[next_idx as usize].prev.store(prev_idx, Ordering::Release);
        }

        // 清理被移除节点的链接
        node.prev.store(u32::MAX, Ordering::Release);
        node.next.store(u32::MAX, Ordering::Release);

        self.count.fetch_sub(1, Ordering::AcqRel);
    }

    /// 将节点移动到头部（访问时调用）
    ///
    /// # Safety
    /// - nodes 数组必须包含有效的 LruNode
    /// - node_idx 必须是有效的索引且在队列中
    /// - 调用者必须持有 lock
    pub unsafe fn move_to_head(&self, nodes: &[crate::buffer::desc::LruNodeData], node_idx: u32) {
        // SAFETY: Caller ensures all safety requirements
        unsafe {
            // 先移除
            self.remove(nodes, node_idx);
            // 再添加到头部
            self.add_to_head(nodes, node_idx);
        }
    }

    /// 获取淘汰候选（尾部节点）
    ///
    /// # Safety
    /// - nodes 数组必须包含有效的 LruNode
    /// - 调用者必须持有 lock
    pub unsafe fn get_victim(&self, nodes: &[super::desc::BufferDesc]) -> Option<u32> {
        let mut idx = self.tail.load(Ordering::Acquire);

        while idx != u32::MAX {
            let desc = &nodes[idx as usize];
            // 检查是否可以被淘汰（未被 pin）
            if !desc.is_pinned() {
                return Some(idx);
            }
            // 移动到前一个节点
            idx = desc.lru_node.prev.load(Ordering::Acquire);
        }

        None
    }

    /// 获取锁
    #[inline]
    pub fn lock(&self) -> std::sync::MutexGuard<'_, ()> {
        self.lock.lock().unwrap()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lru_queue_new() {
        let queue = LruQueue::new(0);
        assert!(queue.is_empty());
        assert_eq!(queue.len(), 0);
        assert_eq!(queue.head(), u32::MAX);
        assert_eq!(queue.tail(), u32::MAX);
    }

    #[test]
    fn test_lru_node_new() {
        let node = LruNode::new();
        assert_eq!(node.prev.load(Ordering::Relaxed), u32::MAX);
        assert_eq!(node.next.load(Ordering::Relaxed), u32::MAX);
    }

    #[test]
    fn test_lru_queue_add_remove() {
        let queue = LruQueue::new(0);
        let nodes: Vec<crate::buffer::desc::LruNodeData> = (0..5)
            .map(|_| crate::buffer::desc::LruNodeData::default())
            .collect();
        let _guard = queue.lock();

        unsafe {
            queue.add_to_head(&nodes, 0);
            assert_eq!(queue.len(), 1);
            assert_eq!(queue.head(), 0);
            assert_eq!(queue.tail(), 0);

            queue.add_to_head(&nodes, 1);
            assert_eq!(queue.len(), 2);
            assert_eq!(queue.head(), 1);
            assert_eq!(queue.tail(), 0);

            queue.add_to_head(&nodes, 2);
            assert_eq!(queue.len(), 3);

            // Remove middle node (1)
            queue.remove(&nodes, 1);
            assert_eq!(queue.len(), 2);

            // Remove head (2)
            queue.remove(&nodes, 2);
            assert_eq!(queue.len(), 1);
            assert_eq!(queue.head(), 0);

            // Remove last (0)
            queue.remove(&nodes, 0);
            assert!(queue.is_empty());
        }
    }

    #[test]
    fn test_lru_queue_move_to_head() {
        let queue = LruQueue::new(0);
        let nodes: Vec<crate::buffer::desc::LruNodeData> = (0..3)
            .map(|_| crate::buffer::desc::LruNodeData::default())
            .collect();
        let _guard = queue.lock();

        unsafe {
            queue.add_to_head(&nodes, 0);
            queue.add_to_head(&nodes, 1);
            queue.add_to_head(&nodes, 2);
            // Order: 2 → 1 → 0
            assert_eq!(queue.head(), 2);
            assert_eq!(queue.tail(), 0);

            // Move tail (0) to head
            queue.move_to_head(&nodes, 0);
            assert_eq!(queue.head(), 0);
            assert_eq!(queue.len(), 3);
        }
    }
}
