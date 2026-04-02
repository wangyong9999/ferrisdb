//! Deadlock Detector
//!
//! 死锁检测实现。

use ferrisdb_core::Xid;
use std::collections::{HashMap, HashSet, VecDeque};

/// 死锁检测器
///
/// 使用 Wait-For Graph 检测死锁。
pub struct DeadlockDetector {
    /// Wait-For Graph: XID -> 等待的 XID 集合
    wait_for_graph: parking_lot::RwLock<HashMap<u64, HashSet<u64>>>,
}

impl DeadlockDetector {
    /// 创建新的死锁检测器
    pub fn new() -> Self {
        Self {
            wait_for_graph: parking_lot::RwLock::new(HashMap::new()),
        }
    }

    /// 添加等待关系
    pub fn add_wait(&self, waiter: Xid, blocking: Xid) {
        let mut graph = self.wait_for_graph.write();
        graph.entry(waiter.raw()).or_default().insert(blocking.raw());
    }

    /// 移除等待关系
    pub fn remove_wait(&self, waiter: Xid) {
        let mut graph = self.wait_for_graph.write();
        graph.remove(&waiter.raw());
    }

    /// 检查死锁
    ///
    /// 使用 BFS 检测从给定 XID 开始是否存在环。
    pub fn check_deadlock(&self, xid: Xid) -> bool {
        let graph = self.wait_for_graph.read();
        let start = xid.raw();

        // BFS 检测环
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back(start);
        visited.insert(start);

        while let Some(current) = queue.pop_front() {
            if let Some(waiting_for) = graph.get(&current) {
                for &blocking in waiting_for {
                    if blocking == start {
                        // 找到环
                        return true;
                    }
                    if visited.insert(blocking) {
                        queue.push_back(blocking);
                    }
                }
            }
        }

        false
    }

    /// 获取等待图大小
    pub fn graph_size(&self) -> usize {
        self.wait_for_graph.read().len()
    }
}

impl Default for DeadlockDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deadlock_detector_new() {
        let detector = DeadlockDetector::new();
        assert_eq!(detector.graph_size(), 0);
    }

    #[test]
    fn test_deadlock_detector_no_deadlock() {
        let detector = DeadlockDetector::new();

        let xid1 = Xid::new(0, 1);
        let xid2 = Xid::new(0, 2);

        detector.add_wait(xid1, xid2);
        assert!(!detector.check_deadlock(xid1));
    }

    #[test]
    fn test_deadlock_detector_deadlock() {
        let detector = DeadlockDetector::new();

        let xid1 = Xid::new(0, 1);
        let xid2 = Xid::new(0, 2);

        // T1 等待 T2，T2 等待 T1 -> 死锁
        detector.add_wait(xid1, xid2);
        detector.add_wait(xid2, xid1);

        assert!(detector.check_deadlock(xid1));
    }

    #[test]
    fn test_deadlock_detector_remove_wait() {
        let detector = DeadlockDetector::new();

        let xid1 = Xid::new(0, 1);
        let xid2 = Xid::new(0, 2);

        detector.add_wait(xid1, xid2);
        detector.remove_wait(xid1);

        assert_eq!(detector.graph_size(), 0);
    }
}
