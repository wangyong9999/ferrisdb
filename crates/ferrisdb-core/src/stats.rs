//! 统计收集 — 全局性能指标
//!
//! 原子计数器，零开销读取，用于监控生产状态。

use std::sync::atomic::{AtomicU64, Ordering};

/// 全局统计单例
static STATS: EngineStats = EngineStats::new();

/// 获取全局统计
pub fn stats() -> &'static EngineStats {
    &STATS
}

/// 引擎统计
pub struct EngineStats {
    // ===== 事务 =====
    /// 已提交事务数
    pub txn_committed: AtomicU64,
    /// 已回滚事务数
    pub txn_aborted: AtomicU64,
    /// 当前活跃事务数
    pub txn_active: AtomicU64,

    // ===== Buffer =====
    /// Buffer 命中数
    pub buf_hits: AtomicU64,
    /// Buffer 未命中数
    pub buf_misses: AtomicU64,
    /// 脏页刷写数
    pub buf_flushes: AtomicU64,
    /// Checkpoint 次数
    pub checkpoints: AtomicU64,

    // ===== WAL =====
    /// WAL 写入字节数
    pub wal_bytes_written: AtomicU64,
    /// WAL fsync 次数
    pub wal_syncs: AtomicU64,

    // ===== Lock =====
    /// 锁等待次数
    pub lock_waits: AtomicU64,
    /// 死锁检测次数
    pub deadlocks_detected: AtomicU64,

    // ===== DML =====
    /// INSERT 次数
    pub rows_inserted: AtomicU64,
    /// UPDATE 次数
    pub rows_updated: AtomicU64,
    /// DELETE 次数
    pub rows_deleted: AtomicU64,
    /// 扫描行数
    pub rows_scanned: AtomicU64,
}

impl EngineStats {
    const fn new() -> Self {
        Self {
            txn_committed: AtomicU64::new(0), txn_aborted: AtomicU64::new(0), txn_active: AtomicU64::new(0),
            buf_hits: AtomicU64::new(0), buf_misses: AtomicU64::new(0), buf_flushes: AtomicU64::new(0), checkpoints: AtomicU64::new(0),
            wal_bytes_written: AtomicU64::new(0), wal_syncs: AtomicU64::new(0),
            lock_waits: AtomicU64::new(0), deadlocks_detected: AtomicU64::new(0),
            rows_inserted: AtomicU64::new(0), rows_updated: AtomicU64::new(0), rows_deleted: AtomicU64::new(0), rows_scanned: AtomicU64::new(0),
        }
    }

    /// 重置所有计数器
    pub fn reset(&self) {
        self.txn_committed.store(0, Ordering::Relaxed);
        self.txn_aborted.store(0, Ordering::Relaxed);
        self.txn_active.store(0, Ordering::Relaxed);
        self.buf_hits.store(0, Ordering::Relaxed);
        self.buf_misses.store(0, Ordering::Relaxed);
        self.buf_flushes.store(0, Ordering::Relaxed);
        self.checkpoints.store(0, Ordering::Relaxed);
        self.wal_bytes_written.store(0, Ordering::Relaxed);
        self.wal_syncs.store(0, Ordering::Relaxed);
        self.lock_waits.store(0, Ordering::Relaxed);
        self.deadlocks_detected.store(0, Ordering::Relaxed);
        self.rows_inserted.store(0, Ordering::Relaxed);
        self.rows_updated.store(0, Ordering::Relaxed);
        self.rows_deleted.store(0, Ordering::Relaxed);
        self.rows_scanned.store(0, Ordering::Relaxed);
    }

    /// 快照所有指标（用于报告）
    pub fn snapshot(&self) -> Vec<(&'static str, u64)> {
        vec![
            ("txn_committed", self.txn_committed.load(Ordering::Relaxed)),
            ("txn_aborted", self.txn_aborted.load(Ordering::Relaxed)),
            ("txn_active", self.txn_active.load(Ordering::Relaxed)),
            ("buf_hits", self.buf_hits.load(Ordering::Relaxed)),
            ("buf_misses", self.buf_misses.load(Ordering::Relaxed)),
            ("buf_flushes", self.buf_flushes.load(Ordering::Relaxed)),
            ("checkpoints", self.checkpoints.load(Ordering::Relaxed)),
            ("wal_bytes_written", self.wal_bytes_written.load(Ordering::Relaxed)),
            ("wal_syncs", self.wal_syncs.load(Ordering::Relaxed)),
            ("lock_waits", self.lock_waits.load(Ordering::Relaxed)),
            ("deadlocks_detected", self.deadlocks_detected.load(Ordering::Relaxed)),
            ("rows_inserted", self.rows_inserted.load(Ordering::Relaxed)),
            ("rows_updated", self.rows_updated.load(Ordering::Relaxed)),
            ("rows_deleted", self.rows_deleted.load(Ordering::Relaxed)),
            ("rows_scanned", self.rows_scanned.load(Ordering::Relaxed)),
        ]
    }

    /// Buffer 命中率
    pub fn buf_hit_rate(&self) -> f64 {
        let h = self.buf_hits.load(Ordering::Relaxed);
        let m = self.buf_misses.load(Ordering::Relaxed);
        let total = h + m;
        if total == 0 { 0.0 } else { h as f64 / total as f64 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn test_stats_default_zero() { let s = EngineStats::new(); assert_eq!(s.txn_committed.load(Ordering::Relaxed), 0); }
    #[test] fn test_stats_increment() { let s = EngineStats::new(); s.txn_committed.fetch_add(1, Ordering::Relaxed); assert_eq!(s.txn_committed.load(Ordering::Relaxed), 1); }
    #[test] fn test_stats_reset() { let s = EngineStats::new(); s.rows_inserted.fetch_add(100, Ordering::Relaxed); s.reset(); assert_eq!(s.rows_inserted.load(Ordering::Relaxed), 0); }
    #[test] fn test_stats_snapshot() { let s = EngineStats::new(); s.buf_hits.fetch_add(90, Ordering::Relaxed); s.buf_misses.fetch_add(10, Ordering::Relaxed); let snap = s.snapshot(); assert!(snap.iter().any(|(k, v)| *k == "buf_hits" && *v == 90)); }
    #[test] fn test_stats_hit_rate() { let s = EngineStats::new(); s.buf_hits.fetch_add(9, Ordering::Relaxed); s.buf_misses.fetch_add(1, Ordering::Relaxed); assert!((s.buf_hit_rate() - 0.9).abs() < 0.01); }
    #[test] fn test_stats_hit_rate_zero() { let s = EngineStats::new(); assert_eq!(s.buf_hit_rate(), 0.0); }
    #[test] fn test_stats_singleton() { let s = stats(); s.rows_inserted.fetch_add(1, Ordering::Relaxed); assert!(s.rows_inserted.load(Ordering::Relaxed) >= 1); }
    #[test] fn test_stats_concurrent() { let s = std::sync::Arc::new(EngineStats::new()); let mut h = vec![]; for _ in 0..4 { let s = s.clone(); h.push(std::thread::spawn(move || { for _ in 0..100 { s.txn_committed.fetch_add(1, Ordering::Relaxed); } })); } for j in h { j.join().unwrap(); } assert_eq!(s.txn_committed.load(Ordering::Relaxed), 400); }
}
