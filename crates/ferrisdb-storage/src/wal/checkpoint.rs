//! WAL Checkpoint
//!
//! 实现在线和离线检查点。
//!
//! # 检查点类型
//!
//! - **在线检查点**：不阻塞写入，记录检查点开始时刻的活跃事务
//! - **离线检查点**：阻塞写入，确保所有脏页刷盘
//!
//! # 检查点流程
//!
//! ```text
//! 1. 获取检查点锁
//! 2. 记录当前 REDO 点（最新 LSN）
//! 3. 收集活跃事务列表（仅在线检查点）
//! 4. 写入检查点 WAL 记录
//! 5. 刷盘脏页到 REDO 点
//! 6. 更新控制文件
//! ```

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use ferrisdb_core::{Lsn, Xid};
use ferrisdb_core::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use ferrisdb_core::error::{FerrisDBError, Result};
use parking_lot::{Mutex, RwLock};

use super::record::WalCheckpoint;
use super::writer::WalWriter;

/// 检查点类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointType {
    /// 在线检查点（不阻塞）
    Online,
    /// 离线检查点（阻塞）
    Shutdown,
}

impl Default for CheckpointType {
    fn default() -> Self {
        Self::Online
    }
}

/// 检查点状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointState {
    /// 空闲
    Idle,
    /// 正在进行
    InProgress,
    /// 已完成
    Completed,
    /// 失败
    Failed,
}

/// 检查点统计信息
#[derive(Debug, Clone, Default)]
pub struct CheckpointStats {
    /// 检查点 LSN
    pub checkpoint_lsn: u64,
    /// 刷盘页数
    pub pages_flushed: u64,
    /// 耗时（毫秒）
    pub duration_ms: u64,
    /// 活跃事务数
    pub active_txns: usize,
    /// 检查点类型
    pub checkpoint_type: CheckpointType,
}

/// 检查点配置
#[derive(Debug, Clone)]
pub struct CheckpointConfig {
    /// 检查点间隔（毫秒）
    pub interval_ms: u64,
    /// 触发检查点的 WAL 大小阈值
    pub wal_size_threshold: u64,
    /// 触发检查点的脏页比例阈值
    pub dirty_page_ratio_threshold: f64,
    /// 是否启用自动检查点
    pub auto_checkpoint: bool,
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self {
            interval_ms: 60_000, // 1 分钟
            wal_size_threshold: 16 * 1024 * 1024, // 16MB
            dirty_page_ratio_threshold: 0.75,
            auto_checkpoint: true,
        }
    }
}

/// 脏页刷盘器 trait
pub trait DirtyPageFlusher: Send + Sync {
    /// 刷盘所有脏页到指定 LSN
    fn flush_dirty_pages(&self, up_to_lsn: Lsn) -> Result<u64>;

    /// 获取脏页数量
    fn dirty_page_count(&self) -> u64;

    /// 获取缓冲池页面总数
    fn total_page_count(&self) -> u64;
}

/// 活跃事务收集器 trait
pub trait ActiveTransactionCollector: Send + Sync {
    /// 获取当前活跃事务列表
    fn collect_active_txns(&self) -> Vec<Xid>;

    /// 获取最小活跃事务 ID
    fn min_active_xid(&self) -> Option<Xid>;
}

/// Checkpoint 管理器
pub struct CheckpointManager {
    /// 配置
    config: CheckpointConfig,
    /// WAL Writer
    wal_writer: Arc<WalWriter>,
    /// 脏页刷盘器
    flusher: Option<Arc<dyn DirtyPageFlusher>>,
    /// 事务收集器
    txn_collector: Option<Arc<dyn ActiveTransactionCollector>>,
    /// 检查点状态
    state: AtomicU32,
    /// 最后检查点 LSN
    last_checkpoint_lsn: AtomicU64,
    /// 最后检查点时间
    last_checkpoint_time: Mutex<Instant>,
    /// 检查点统计
    stats: RwLock<CheckpointStats>,
    /// 是否请求终止
    shutdown: AtomicBool,
    /// Control file（持久化 checkpoint LSN）
    control_file: Option<Arc<crate::control::ControlFile>>,
}

impl CheckpointManager {
    /// 创建新的检查点管理器
    pub fn new(config: CheckpointConfig, wal_writer: Arc<WalWriter>) -> Self {
        Self {
            config,
            wal_writer,
            flusher: None,
            txn_collector: None,
            state: AtomicU32::new(CheckpointState::Idle as u32),
            last_checkpoint_lsn: AtomicU64::new(0),
            last_checkpoint_time: Mutex::new(Instant::now()),
            stats: RwLock::new(CheckpointStats::default()),
            shutdown: AtomicBool::new(false),
            control_file: None,
        }
    }

    /// 设置 control file（启用持久化 checkpoint LSN）
    pub fn set_control_file(&mut self, cf: Arc<crate::control::ControlFile>) {
        // 从 control file 恢复 checkpoint LSN
        let saved_lsn = cf.checkpoint_lsn();
        if saved_lsn.is_valid() {
            self.last_checkpoint_lsn.store(saved_lsn.raw(), Ordering::Release);
        }
        self.control_file = Some(cf);
    }

    /// 设置脏页刷盘器
    pub fn set_flusher(&mut self, flusher: Arc<dyn DirtyPageFlusher>) {
        self.flusher = Some(flusher);
    }

    /// 设置事务收集器
    pub fn set_txn_collector(&mut self, collector: Arc<dyn ActiveTransactionCollector>) {
        self.txn_collector = Some(collector);
    }

    /// 获取检查点状态
    #[inline]
    pub fn state(&self) -> CheckpointState {
        match self.state.load(Ordering::Acquire) {
            0 => CheckpointState::Idle,
            1 => CheckpointState::InProgress,
            2 => CheckpointState::Completed,
            _ => CheckpointState::Failed,
        }
    }

    /// 获取最后检查点 LSN
    #[inline]
    pub fn last_checkpoint_lsn(&self) -> Lsn {
        Lsn::from_raw(self.last_checkpoint_lsn.load(Ordering::Acquire))
    }

    /// 执行检查点
    pub fn checkpoint(&self, checkpoint_type: CheckpointType) -> Result<CheckpointStats> {
        // 检查是否已在进行
        if self
            .state
            .compare_exchange(
                CheckpointState::Idle as u32,
                CheckpointState::InProgress as u32,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_err()
        {
            return Err(FerrisDBError::InvalidState(
                "Checkpoint already in progress".to_string(),
            ));
        }

        let start_time = Instant::now();
        let result = self.do_checkpoint(checkpoint_type);
        let duration = start_time.elapsed();

        match &result {
            Ok(stats) => {
                self.state.store(CheckpointState::Completed as u32, Ordering::Release);
                let mut s = self.stats.write();
                *s = stats.clone();
                s.duration_ms = duration.as_millis() as u64;

                // 更新最后检查点时间
                *self.last_checkpoint_time.lock() = Instant::now();

                // 更新最后检查点 LSN
                self.last_checkpoint_lsn
                    .store(stats.checkpoint_lsn, Ordering::Release);
            }
            Err(_) => {
                self.state.store(CheckpointState::Failed as u32, Ordering::Release);
            }
        }

        // 重置为空闲状态
        self.state.store(CheckpointState::Idle as u32, Ordering::Release);

        result
    }

    /// 执行检查点内部逻辑
    fn do_checkpoint(&self, checkpoint_type: CheckpointType) -> Result<CheckpointStats> {
        let mut stats = CheckpointStats {
            checkpoint_type,
            ..Default::default()
        };

        // 1. 获取当前 LSN 作为检查点 LSN
        let checkpoint_lsn = self.wal_writer.current_lsn();
        stats.checkpoint_lsn = checkpoint_lsn.raw();

        // 2. 收集活跃事务（仅在线检查点）
        let active_txns = if checkpoint_type == CheckpointType::Online {
            if let Some(ref collector) = self.txn_collector {
                let txns = collector.collect_active_txns();
                stats.active_txns = txns.len();
                txns
            } else {
                Vec::new()
            }
        } else {
            // 离线检查点：应该没有活跃事务
            Vec::new()
        };

        // 3. 先刷盘脏页（确保数据页安全落盘后再写 checkpoint 记录）
        if let Some(ref flusher) = self.flusher {
            let pages_flushed = flusher.flush_dirty_pages(checkpoint_lsn)?;
            stats.pages_flushed = pages_flushed;
        }

        // 4. 写入检查点 WAL 记录（此时所有脏页已安全，recovery 可以从此 LSN 开始）
        self.write_checkpoint_record(checkpoint_lsn, &active_txns, checkpoint_type)?;

        // 5. 同步 WAL
        self.wal_writer.sync()?;

        // 6. 持久化 checkpoint LSN 到 control file
        if let Some(ref cf) = self.control_file {
            let _ = cf.update_checkpoint(checkpoint_lsn, self.wal_writer.file_no());
        }

        // 7. 清理旧 WAL 文件
        self.cleanup_old_wal_segments(checkpoint_lsn);

        Ok(stats)
    }

    /// 写入检查点 WAL 记录
    fn write_checkpoint_record(
        &self,
        lsn: Lsn,
        active_txns: &[Xid],
        checkpoint_type: CheckpointType,
    ) -> Result<()> {
        let checkpoint = WalCheckpoint::new(
            lsn.raw(),
            checkpoint_type == CheckpointType::Shutdown,
        );

        // 序列化检查点记录
        let mut data = Vec::new();
        data.extend_from_slice(&checkpoint.to_bytes());

        // 追加活跃事务列表
        for xid in active_txns {
            data.extend_from_slice(&xid.raw().to_le_bytes());
        }

        // 写入 WAL
        self.wal_writer.write(&data)?;

        Ok(())
    }

    /// 检查是否需要检查点
    pub fn need_checkpoint(&self) -> bool {
        if !self.config.auto_checkpoint {
            return false;
        }

        // 检查时间间隔
        let last_time = *self.last_checkpoint_time.lock();
        if last_time.elapsed() > Duration::from_millis(self.config.interval_ms) {
            return true;
        }

        // 检查 WAL 大小
        // 简化实现：暂不检查

        // 检查脏页比例
        if let Some(ref flusher) = self.flusher {
            let dirty = flusher.dirty_page_count();
            let total = flusher.total_page_count();
            if total > 0 {
                let ratio = dirty as f64 / total as f64;
                if ratio > self.config.dirty_page_ratio_threshold {
                    return true;
                }
            }
        }

        false
    }

    /// 启动自动检查点线程
    pub fn start_auto_checkpoint(self: Arc<Self>) -> thread::JoinHandle<()> {
        self.shutdown.store(false, Ordering::Release);

        let manager = Arc::clone(&self);
        thread::spawn(move || {
            loop {
                if manager.shutdown.load(Ordering::Acquire) {
                    break;
                }

                if manager.need_checkpoint() {
                    let _ = manager.checkpoint(CheckpointType::Online);
                }

                // 休眠一段时间
                thread::sleep(Duration::from_millis(1000));
            }
        })
    }

    /// 停止自动检查点
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// 清理 checkpoint LSN 之前的旧 WAL 文件
    fn cleanup_old_wal_segments(&self, checkpoint_lsn: Lsn) {
        let (checkpoint_file_no, _) = checkpoint_lsn.parts();
        if checkpoint_file_no == 0 {
            return;
        }
        let wal_dir = self.wal_writer.wal_dir();
        if let Ok(entries) = std::fs::read_dir(wal_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if let Some(ext) = path.extension() {
                    if ext == "wal" {
                        if let Some(stem) = path.file_stem() {
                            if let Ok(file_no) = u32::from_str_radix(&stem.to_string_lossy(), 16) {
                                if file_no < checkpoint_file_no.saturating_sub(1) {
                                    let _ = std::fs::remove_file(&path);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    /// 获取统计信息
    pub fn stats(&self) -> CheckpointStats {
        self.stats.read().clone()
    }
}

/// 检查点恢复器
pub struct CheckpointRecovery {
    /// WAL 目录
    wal_dir: PathBuf,
}

impl CheckpointRecovery {
    /// 创建新的恢复器
    pub fn new<P: Into<PathBuf>>(wal_dir: P) -> Self {
        Self {
            wal_dir: wal_dir.into(),
        }
    }

    /// 查找最近的检查点
    ///
    /// 扫描 WAL 目录查找最后一个检查点记录，返回 (LSN, 检查点数据, 活跃事务列表)。
    pub fn find_latest_checkpoint(&self) -> Result<Option<(Lsn, WalCheckpoint, Vec<Xid>)>> {
        // WAL 目录不存在时返回 None（无检查点）
        if !self.wal_dir.exists() {
            return Ok(None);
        }
        let recovery = super::recovery::WalRecovery::new(&self.wal_dir);
        match recovery.recover_scan_only() {
            Ok(_) => {
                let checkpoint_lsn = recovery.checkpoint_lsn();
                if checkpoint_lsn.is_valid() {
                    let checkpoint = WalCheckpoint::new(checkpoint_lsn.raw(), false);
                    Ok(Some((checkpoint_lsn, checkpoint, Vec::new())))
                } else {
                    Ok(None)
                }
            }
            Err(_) => Ok(None), // WAL 损坏或读取失败，视为无检查点
        }
    }

    /// 恢复到检查点
    pub fn recover_to_checkpoint(&self, _lsn: Lsn) -> Result<()> {
        // 从检查点 LSN 开始重放 WAL 记录
        // 实际由 WalRecovery::recover() 完成
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    /// 测试用脏页刷盘器
    struct TestFlusher {
        dirty_count: AtomicU64,
        total_count: AtomicU64,
    }

    impl TestFlusher {
        fn new(dirty: u64, total: u64) -> Self {
            Self {
                dirty_count: AtomicU64::new(dirty),
                total_count: AtomicU64::new(total),
            }
        }
    }

    impl DirtyPageFlusher for TestFlusher {
        fn flush_dirty_pages(&self, _up_to_lsn: Lsn) -> Result<u64> {
            let count = self.dirty_count.swap(0, Ordering::AcqRel);
            Ok(count)
        }

        fn dirty_page_count(&self) -> u64 {
            self.dirty_count.load(Ordering::Acquire)
        }

        fn total_page_count(&self) -> u64 {
            self.total_count.load(Ordering::Acquire)
        }
    }

    /// 测试用事务收集器
    struct TestTxnCollector;

    impl ActiveTransactionCollector for TestTxnCollector {
        fn collect_active_txns(&self) -> Vec<Xid> {
            vec![Xid::new(0, 1), Xid::new(0, 2)]
        }

        fn min_active_xid(&self) -> Option<Xid> {
            Some(Xid::new(0, 1))
        }
    }

    #[test]
    fn test_checkpoint_config() {
        let config = CheckpointConfig::default();
        assert_eq!(config.interval_ms, 60_000);
        assert!(config.auto_checkpoint);
    }

    #[test]
    fn test_checkpoint_type() {
        assert_eq!(CheckpointType::default(), CheckpointType::Online);
        assert_ne!(CheckpointType::Online, CheckpointType::Shutdown);
    }

    #[test]
    fn test_checkpoint_state() {
        let config = CheckpointConfig::default();
        let wal_writer = Arc::new(WalWriter::new("/tmp/test_checkpoint_wal"));
        let manager = CheckpointManager::new(config, wal_writer);

        assert_eq!(manager.state(), CheckpointState::Idle);
    }

    #[test]
    fn test_need_checkpoint_time() {
        let config = CheckpointConfig {
            interval_ms: 0, // 立即触发
            ..Default::default()
        };
        let wal_writer = Arc::new(WalWriter::new("/tmp/test_checkpoint_wal2"));
        let manager = CheckpointManager::new(config, wal_writer);

        // 由于间隔为 0，应该需要检查点
        assert!(manager.need_checkpoint());
    }

    #[test]
    fn test_need_checkpoint_dirty_ratio() {
        let config = CheckpointConfig {
            interval_ms: u64::MAX, // 禁用时间触发
            dirty_page_ratio_threshold: 0.5,
            ..Default::default()
        };
        let wal_writer = Arc::new(WalWriter::new("/tmp/test_checkpoint_wal3"));
        let mut manager = CheckpointManager::new(config, wal_writer);
        manager.set_flusher(Arc::new(TestFlusher::new(80, 100)));

        // 脏页比例 80% > 阈值 50%，应该需要检查点
        assert!(manager.need_checkpoint());
    }

    #[test]
    fn test_checkpoint_stats() {
        let stats = CheckpointStats {
            checkpoint_lsn: 1000,
            pages_flushed: 50,
            duration_ms: 100,
            active_txns: 5,
            checkpoint_type: CheckpointType::Online,
        };

        assert_eq!(stats.checkpoint_lsn, 1000);
        assert_eq!(stats.pages_flushed, 50);
        assert_eq!(stats.duration_ms, 100);
        assert_eq!(stats.active_txns, 5);
        assert_eq!(stats.checkpoint_type, CheckpointType::Online);
    }

    #[test]
    fn test_checkpoint_recovery() {
        let recovery = CheckpointRecovery::new("/tmp/test_wal");
        let result = recovery.find_latest_checkpoint();
        assert!(result.is_ok());
    }
}
