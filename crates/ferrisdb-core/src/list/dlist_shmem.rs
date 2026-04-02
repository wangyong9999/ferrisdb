//! 共享内存兼容的双向链表
//!
//! 使用索引而非指针，支持跨进程共享。

use crate::atomic::{AtomicU32, Ordering};

/// 无效节点索引
pub const INVALID_NODE_INDEX: u32 = u32::MAX;

/// 共享内存双向链表节点
///
/// 使用索引而非指针，支持跨进程共享。
#[derive(Debug)]
#[repr(C)]
pub struct ShmDListNode {
    /// 前驱节点索引
    pub prev: AtomicU32,
    /// 后继节点索引
    pub next: AtomicU32,
}

impl ShmDListNode {
    /// 创建新的链表节点
    #[inline]
    pub const fn new() -> Self {
        Self {
            prev: AtomicU32::new(INVALID_NODE_INDEX),
            next: AtomicU32::new(INVALID_NODE_INDEX),
        }
    }

    /// 检查节点是否在链表中
    #[inline]
    pub fn is_linked(&self) -> bool {
        self.prev.load(Ordering::Acquire) != INVALID_NODE_INDEX
            || self.next.load(Ordering::Acquire) != INVALID_NODE_INDEX
    }

    /// 初始化节点（从链表中移除）
    #[inline]
    pub fn init(&self) {
        self.prev.store(INVALID_NODE_INDEX, Ordering::Release);
        self.next.store(INVALID_NODE_INDEX, Ordering::Release);
    }
}

impl Default for ShmDListNode {
    fn default() -> Self {
        Self::new()
    }
}

/// 共享内存双向链表头
#[derive(Debug)]
#[repr(C)]
pub struct ShmDListHead {
    /// 头节点索引
    pub head: AtomicU32,
    /// 尾节点索引
    pub tail: AtomicU32,
}

impl ShmDListHead {
    /// 创建空链表
    #[inline]
    pub const fn new() -> Self {
        Self {
            head: AtomicU32::new(INVALID_NODE_INDEX),
            tail: AtomicU32::new(INVALID_NODE_INDEX),
        }
    }

    /// 检查链表是否为空
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.head.load(Ordering::Acquire) == INVALID_NODE_INDEX
    }

    /// 获取头节点索引
    #[inline]
    pub fn head_index(&self) -> u32 {
        self.head.load(Ordering::Acquire)
    }

    /// 获取尾节点索引
    #[inline]
    pub fn tail_index(&self) -> u32 {
        self.tail.load(Ordering::Acquire)
    }

    /// 添加节点到尾部
    ///
    /// # Safety
    /// - nodes 数组必须包含有效的节点
    /// - node_idx 必须是有效的索引
    /// - 调用者必须持有适当的锁来保护此操作
    #[inline]
    pub unsafe fn push_tail(&self, nodes: &[ShmDListNode], node_idx: u32) {
        debug_assert!(node_idx != INVALID_NODE_INDEX);
        debug_assert!((node_idx as usize) < nodes.len());

        let node = &nodes[node_idx as usize];
        node.prev.store(INVALID_NODE_INDEX, Ordering::Relaxed);
        node.next.store(INVALID_NODE_INDEX, Ordering::Relaxed);

        let old_tail = self.tail.swap(node_idx, Ordering::AcqRel);

        if old_tail == INVALID_NODE_INDEX {
            // 链表为空，设置 head
            self.head.store(node_idx, Ordering::Release);
        } else {
            // 链接新节点到旧尾部
            let old_tail_node = &nodes[old_tail as usize];
            old_tail_node.next.store(node_idx, Ordering::Release);
            node.prev.store(old_tail, Ordering::Release);
        }
    }

    /// 添加节点到头部
    ///
    /// # Safety
    /// - nodes 数组必须包含有效的节点
    /// - node_idx 必须是有效的索引
    /// - 调用者必须持有适当的锁来保护此操作
    #[inline]
    pub unsafe fn push_head(&self, nodes: &[ShmDListNode], node_idx: u32) {
        debug_assert!(node_idx != INVALID_NODE_INDEX);
        debug_assert!((node_idx as usize) < nodes.len());

        let node = &nodes[node_idx as usize];
        node.prev.store(INVALID_NODE_INDEX, Ordering::Relaxed);
        node.next.store(INVALID_NODE_INDEX, Ordering::Relaxed);

        let old_head = self.head.swap(node_idx, Ordering::AcqRel);

        if old_head == INVALID_NODE_INDEX {
            // 链表为空，设置 tail
            self.tail.store(node_idx, Ordering::Release);
        } else {
            // 链接新节点到旧头部
            let old_head_node = &nodes[old_head as usize];
            old_head_node.prev.store(node_idx, Ordering::Release);
            node.next.store(old_head, Ordering::Release);
        }
    }

    /// 从头部移除节点
    ///
    /// # Safety
    /// - nodes 数组必须包含有效的节点
    /// - 调用者必须持有适当的锁来保护此操作
    ///
    /// # Returns
    /// 移除的节点索引，如果链表为空则返回 None
    #[inline]
    pub unsafe fn pop_head(&self, nodes: &[ShmDListNode]) -> Option<u32> {
        let head_idx = self.head.load(Ordering::Acquire);
        if head_idx == INVALID_NODE_INDEX {
            return None;
        }

        let head_node = &nodes[head_idx as usize];
        let next_idx = head_node.next.load(Ordering::Acquire);

        if next_idx == INVALID_NODE_INDEX {
            // 链表只有一个节点
            self.head.store(INVALID_NODE_INDEX, Ordering::Release);
            self.tail.store(INVALID_NODE_INDEX, Ordering::Release);
        } else {
            // 更新 head 和 next 节点的 prev
            self.head.store(next_idx, Ordering::Release);
            let next_node = &nodes[next_idx as usize];
            next_node.prev.store(INVALID_NODE_INDEX, Ordering::Release);
        }

        // 清理被移除节点的链接
        head_node.prev.store(INVALID_NODE_INDEX, Ordering::Release);
        head_node.next.store(INVALID_NODE_INDEX, Ordering::Release);

        Some(head_idx)
    }

    /// 从链表中移除指定节点
    ///
    /// # Safety
    /// - nodes 数组必须包含有效的节点
    /// - node_idx 必须是有效的索引且在链表中
    /// - 调用者必须持有适当的锁来保护此操作
    #[inline]
    pub unsafe fn remove(&self, nodes: &[ShmDListNode], node_idx: u32) {
        debug_assert!(node_idx != INVALID_NODE_INDEX);
        debug_assert!((node_idx as usize) < nodes.len());

        let node = &nodes[node_idx as usize];
        let prev_idx = node.prev.load(Ordering::Acquire);
        let next_idx = node.next.load(Ordering::Acquire);

        if prev_idx == INVALID_NODE_INDEX {
            // node 是 head
            self.head.store(next_idx, Ordering::Release);
        } else {
            let prev_node = &nodes[prev_idx as usize];
            prev_node.next.store(next_idx, Ordering::Release);
        }

        if next_idx == INVALID_NODE_INDEX {
            // node 是 tail
            self.tail.store(prev_idx, Ordering::Release);
        } else {
            let next_node = &nodes[next_idx as usize];
            next_node.prev.store(prev_idx, Ordering::Release);
        }

        // 清理被移除节点的链接
        node.prev.store(INVALID_NODE_INDEX, Ordering::Release);
        node.next.store(INVALID_NODE_INDEX, Ordering::Release);
    }
}

impl Default for ShmDListHead {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shm_dlist_node() {
        let node = ShmDListNode::new();
        assert!(!node.is_linked());
    }

    #[test]
    fn test_shm_dlist_head_empty() {
        let list = ShmDListHead::new();
        assert!(list.is_empty());
        assert_eq!(list.head_index(), INVALID_NODE_INDEX);
        assert_eq!(list.tail_index(), INVALID_NODE_INDEX);
    }

    #[test]
    fn test_shm_dlist_push_pop() {
        let mut nodes = [ShmDListNode::new(), ShmDListNode::new(), ShmDListNode::new()];
        let list = ShmDListHead::new();

        unsafe {
            // Push tail
            list.push_tail(&nodes, 0);
            assert!(!list.is_empty());
            assert_eq!(list.head_index(), 0);
            assert_eq!(list.tail_index(), 0);

            list.push_tail(&nodes, 1);
            assert_eq!(list.head_index(), 0);
            assert_eq!(list.tail_index(), 1);

            list.push_tail(&nodes, 2);
            assert_eq!(list.tail_index(), 2);

            // Pop head
            let head = list.pop_head(&nodes);
            assert_eq!(head, Some(0));
            assert_eq!(list.head_index(), 1);

            let head = list.pop_head(&nodes);
            assert_eq!(head, Some(1));
            assert_eq!(list.head_index(), 2);

            let head = list.pop_head(&nodes);
            assert_eq!(head, Some(2));
            assert!(list.is_empty());
        }
    }

    #[test]
    fn test_shm_dlist_remove() {
        let mut nodes = [ShmDListNode::new(), ShmDListNode::new(), ShmDListNode::new()];
        let list = ShmDListHead::new();

        unsafe {
            list.push_tail(&nodes, 0);
            list.push_tail(&nodes, 1);
            list.push_tail(&nodes, 2);

            // Remove middle
            list.remove(&nodes, 1);
            assert_eq!(list.head_index(), 0);
            assert_eq!(list.tail_index(), 2);
            assert_eq!(nodes[0].next.load(Ordering::Acquire), 2);
            assert_eq!(nodes[2].prev.load(Ordering::Acquire), 0);
        }
    }
}
