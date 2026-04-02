//! Serializable Snapshot Isolation (SSI) — 冲突检测
//!
//! 基于读写集交集检测 serialization anomalies。
//! 当两个并发事务的 rw-dependency 形成危险结构时，中止后提交者。

use std::collections::{HashMap, HashSet};
use parking_lot::RwLock;
use ferrisdb_core::Xid;

/// 读写集中的访问粒度
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct AccessKey {
    /// 表 OID
    pub table_oid: u32,
    /// 页号（粗粒度谓词锁）
    pub page_no: u32,
}

/// SSI 冲突检测器
pub struct SsiTracker {
    /// 事务的读集
    read_sets: RwLock<HashMap<u64, HashSet<AccessKey>>>,
    /// 事务的写集
    write_sets: RwLock<HashMap<u64, HashSet<AccessKey>>>,
}

impl SsiTracker {
    /// 创建新的 SSI 跟踪器
    pub fn new() -> Self {
        Self {
            read_sets: RwLock::new(HashMap::new()),
            write_sets: RwLock::new(HashMap::new()),
        }
    }

    /// 记录读操作
    pub fn track_read(&self, xid: Xid, key: AccessKey) {
        self.read_sets.write().entry(xid.raw()).or_default().insert(key);
    }

    /// 记录写操作
    pub fn track_write(&self, xid: Xid, key: AccessKey) {
        self.write_sets.write().entry(xid.raw()).or_default().insert(key);
    }

    /// 检查事务提交时是否存在 serialization 冲突
    ///
    /// 如果存在其他已提交事务 T2，使得：
    ///   T1 reads something T2 wrote (rw-dependency T1→T2)
    ///   AND T2 reads something T1 wrote (rw-dependency T2→T1)
    /// 则存在 dangerous structure，返回 true（需要中止）
    pub fn check_conflict(&self, xid: Xid) -> bool {
        let xid_raw = xid.raw();
        let reads = self.read_sets.read();
        let writes = self.write_sets.read();

        let my_reads = match reads.get(&xid_raw) {
            Some(r) => r,
            None => return false,
        };
        let my_writes = match writes.get(&xid_raw) {
            Some(w) => w,
            None => return false,
        };

        // Check against all other transactions
        for (&other_xid, other_writes) in writes.iter() {
            if other_xid == xid_raw { continue; }
            // rw-dependency: I read something they wrote?
            let i_read_their_write = my_reads.iter().any(|k| other_writes.contains(k));
            if !i_read_their_write { continue; }

            // Reverse: they read something I wrote?
            if let Some(other_reads) = reads.get(&other_xid) {
                let they_read_my_write = other_reads.iter().any(|k| my_writes.contains(k));
                if they_read_my_write {
                    return true; // Dangerous structure detected
                }
            }
        }
        false
    }

    /// 清理已完成事务的跟踪数据
    pub fn cleanup(&self, xid: Xid) {
        self.read_sets.write().remove(&xid.raw());
        self.write_sets.write().remove(&xid.raw());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_conflict() {
        let ssi = SsiTracker::new();
        let x1 = Xid::new(0, 1);
        let x2 = Xid::new(0, 2);
        ssi.track_read(x1, AccessKey { table_oid: 1, page_no: 0 });
        ssi.track_write(x2, AccessKey { table_oid: 1, page_no: 1 }); // Different page
        assert!(!ssi.check_conflict(x1));
    }

    #[test]
    fn test_rw_conflict() {
        let ssi = SsiTracker::new();
        let x1 = Xid::new(0, 1);
        let x2 = Xid::new(0, 2);
        let key = AccessKey { table_oid: 1, page_no: 0 };
        // x1 reads page, x2 writes page → x1 reads x2's write
        ssi.track_read(x1, key.clone());
        ssi.track_write(x2, key.clone());
        // x2 reads page, x1 writes page → x2 reads x1's write
        ssi.track_read(x2, AccessKey { table_oid: 1, page_no: 5 });
        ssi.track_write(x1, AccessKey { table_oid: 1, page_no: 5 });
        assert!(ssi.check_conflict(x1), "Should detect serialization anomaly");
    }

    #[test]
    fn test_cleanup() {
        let ssi = SsiTracker::new();
        let x1 = Xid::new(0, 1);
        ssi.track_read(x1, AccessKey { table_oid: 1, page_no: 0 });
        ssi.cleanup(x1);
        assert!(!ssi.check_conflict(x1));
    }

    #[test]
    fn test_one_way_dependency_no_conflict() {
        let ssi = SsiTracker::new();
        let x1 = Xid::new(0, 1);
        let x2 = Xid::new(0, 2);
        let key = AccessKey { table_oid: 1, page_no: 0 };
        // Only one direction: x1 reads what x2 writes
        ssi.track_read(x1, key.clone());
        ssi.track_write(x2, key.clone());
        // x2 doesn't read what x1 writes → no cycle
        assert!(!ssi.check_conflict(x1));
    }
}
