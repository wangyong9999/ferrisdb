//! 快照定义

use std::sync::atomic::{AtomicU64, Ordering};

use super::{Csn, Xid};

/// 快照数据
///
/// 存储 MVCC 可见性判断所需的信息。
#[derive(Debug, Clone)]
pub struct SnapshotData {
    /// 快照 CSN（用于可见性判断）
    pub snapshot_csn: Csn,
    /// 活跃事务列表
    pub active_xids: Vec<Xid>,
    /// 快照创建时间（用于超时检查）
    pub created_at: std::time::Instant,
}

/// 快照
///
/// 用于 MVCC 可见性判断的快照。
#[derive(Debug, Clone)]
pub struct Snapshot {
    /// 内部数据
    inner: SnapshotData,
}

impl Snapshot {
    /// 创建新快照
    pub fn new(snapshot_csn: Csn, active_xids: Vec<Xid>) -> Self {
        Self {
            inner: SnapshotData {
                snapshot_csn,
                active_xids,
                created_at: std::time::Instant::now(),
            },
        }
    }

    /// 获取快照 CSN
    #[inline]
    pub fn snapshot_csn(&self) -> Csn {
        self.inner.snapshot_csn
    }

    /// 获取活跃事务列表
    #[inline]
    pub fn active_xids(&self) -> &[Xid] {
        &self.inner.active_xids
    }

    /// 检查事务是否在快照中活跃
    #[inline]
    pub fn is_active(&self, xid: Xid) -> bool {
        self.inner.active_xids.contains(&xid)
    }

    /// 检查元组是否可见
    ///
    /// # 参数
    /// - `xmin`: 创建元组的事务 ID
    /// - `xmin_csn`: 创建事务的 CSN（已提交时）
    /// - `xmax`: 删除/更新元组的事务 ID（如果有）
    /// - `xmax_csn`: 删除事务的 CSN
    /// - `current_xid`: 当前事务 ID
    #[inline]
    pub fn is_visible(
        &self,
        xmin: Xid,
        xmin_csn: Csn,
        xmax: Xid,
        xmax_csn: Csn,
        current_xid: Xid,
    ) -> bool {
        // 规则 1: 自己创建的元组总是可见（除非被自己删除）
        if xmin == current_xid {
            return xmax != current_xid;
        }

        // 规则 2: 自己删除的元组不可见
        if xmax == current_xid {
            return false;
        }

        // 规则 3: xmin 必须已提交且 CSN < snapshot_csn
        if xmin_csn.is_invalid() {
            return false; // xmin 未提交
        }
        if xmin_csn.precedes(self.inner.snapshot_csn) {
            return false; // xmin 在快照之后提交
        }

        // 规则 4: 检查 xmax
        if xmax_csn.is_invalid() {
            return true; // xmax 未提交，可见
        }
        if xmax_csn.precedes(self.inner.snapshot_csn) {
            return false; // xmax 在快照之前提交，不可见
        }

        // xmax 在快照之后提交，可见
        true
    }

    /// 获取快照年龄（以秒为单位）
    pub fn age(&self) -> std::time::Duration {
        self.inner.created_at.elapsed()
    }
}

/// 全局快照管理器
///
/// 用于 CSN 模式下的快照管理。
pub struct SnapshotManager {
    /// 当前全局 CSN
    global_csn: AtomicU64,
}

impl SnapshotManager {
    /// 创建新快照管理器
    pub const fn new() -> Self {
        Self {
            global_csn: AtomicU64::new(Csn::FIRST_VALID.raw()),
        }
    }

    /// 获取当前快照
    pub fn get_snapshot(&self) -> Snapshot {
        let snapshot_csn = Csn::from_raw(self.global_csn.load(Ordering::Acquire));
        // 简化实现：不追踪活跃事务
        Snapshot::new(snapshot_csn, vec![])
    }

    /// 推进快照 CSN
    pub fn advance(&self) {
        self.global_csn.fetch_add(1, Ordering::AcqRel);
    }
}

impl Default for SnapshotManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_snapshot_new() {
        let snapshot = Snapshot::new(Csn::from_raw(100), vec![]);
        assert_eq!(snapshot.snapshot_csn().raw(), 100);
    }

    #[test]
    fn test_snapshot_is_active() {
        let xid = Xid::new(1, 100);
        let snapshot = Snapshot::new(Csn::from_raw(100), vec![xid]);

        assert!(snapshot.is_active(xid));
        assert!(!snapshot.is_active(Xid::new(1, 200)));
    }

    #[test]
    fn test_snapshot_visible_own_tuple() {
        let snapshot = Snapshot::new(Csn::from_raw(100), vec![]);
        let current_xid = Xid::new(1, 100);

        // 自己创建的元组
        assert!(snapshot.is_visible(
            current_xid,
            Csn::INVALID,
            Xid::INVALID,
            Csn::INVALID,
            current_xid
        ));
    }

    #[test]
    fn test_snapshot_visible_deleted_by_self() {
        let snapshot = Snapshot::new(Csn::from_raw(100), vec![]);
        let current_xid = Xid::new(1, 100);

        // 自己删除的元组
        assert!(!snapshot.is_visible(
            Xid::new(1, 50),
            Csn::from_raw(50),
            current_xid,
            Csn::INVALID,
            current_xid
        ));
    }

    #[test]
    fn test_snapshot_manager() {
        let mgr = SnapshotManager::new();

        let snap1 = mgr.get_snapshot();
        let csn1 = snap1.snapshot_csn();

        mgr.advance();

        let snap2 = mgr.get_snapshot();
        assert!(snap2.snapshot_csn().precedes(csn1) || snap2.snapshot_csn().raw() > csn1.raw());
    }
}
