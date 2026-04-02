//! GUC (Global Unified Configuration) — 运行时配置管理
//!
//! 精简版：覆盖存储引擎核心参数，支持运行时读取和启动时加载。

use std::sync::atomic::{AtomicU32, AtomicU64, AtomicBool, Ordering};

/// 全局配置单例
static CONFIG: GlobalConfig = GlobalConfig::new();

/// 获取全局配置
pub fn config() -> &'static GlobalConfig {
    &CONFIG
}

/// 全局配置（原子字段，无锁读取）
pub struct GlobalConfig {
    /// Buffer pool 页面数
    pub shared_buffers: AtomicU32,
    /// WAL buffer 大小（字节）
    pub wal_buffers: AtomicU32,
    /// 事务超时（毫秒，0=无限）
    pub transaction_timeout_ms: AtomicU64,
    /// Checkpoint 间隔（毫秒）
    pub checkpoint_interval_ms: AtomicU64,
    /// Checkpoint 脏页比例阈值（千分比）
    pub checkpoint_dirty_ratio_permille: AtomicU32,
    /// 最大连接/事务槽数
    pub max_connections: AtomicU32,
    /// 后台写线程刷页间隔（毫秒）
    pub bgwriter_delay_ms: AtomicU32,
    /// 后台写线程每轮最大刷页数
    pub bgwriter_max_pages: AtomicU32,
    /// WAL flusher 间隔（毫秒）
    pub wal_flusher_delay_ms: AtomicU32,
    /// 是否启用同步提交
    pub synchronous_commit: AtomicBool,
    /// 死锁检测间隔（毫秒）
    pub deadlock_timeout_ms: AtomicU32,
    /// 日志级别（0=OFF, 1=ERROR, 2=WARN, 3=INFO, 4=DEBUG）
    pub log_level: AtomicU32,
}

impl GlobalConfig {
    const fn new() -> Self {
        Self {
            shared_buffers: AtomicU32::new(50000),
            wal_buffers: AtomicU32::new(16384),
            transaction_timeout_ms: AtomicU64::new(0),
            checkpoint_interval_ms: AtomicU64::new(60000),
            checkpoint_dirty_ratio_permille: AtomicU32::new(750),
            max_connections: AtomicU32::new(64),
            bgwriter_delay_ms: AtomicU32::new(200),
            bgwriter_max_pages: AtomicU32::new(100),
            wal_flusher_delay_ms: AtomicU32::new(5),
            synchronous_commit: AtomicBool::new(true),
            deadlock_timeout_ms: AtomicU32::new(1000),
            log_level: AtomicU32::new(3),
        }
    }

    /// 从 key=value 格式加载配置
    pub fn load_from_str(&self, input: &str) {
        for line in input.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') { continue; }
            if let Some((k, v)) = line.split_once('=') {
                self.set(k.trim(), v.trim());
            }
        }
    }

    /// 设置单个参数
    pub fn set(&self, key: &str, value: &str) {
        match key {
            "shared_buffers" => { if let Ok(v) = value.parse() { self.shared_buffers.store(v, Ordering::Release); } }
            "wal_buffers" => { if let Ok(v) = value.parse() { self.wal_buffers.store(v, Ordering::Release); } }
            "transaction_timeout_ms" => { if let Ok(v) = value.parse() { self.transaction_timeout_ms.store(v, Ordering::Release); } }
            "checkpoint_interval_ms" => { if let Ok(v) = value.parse() { self.checkpoint_interval_ms.store(v, Ordering::Release); } }
            "max_connections" => { if let Ok(v) = value.parse() { self.max_connections.store(v, Ordering::Release); } }
            "bgwriter_delay_ms" => { if let Ok(v) = value.parse() { self.bgwriter_delay_ms.store(v, Ordering::Release); } }
            "bgwriter_max_pages" => { if let Ok(v) = value.parse() { self.bgwriter_max_pages.store(v, Ordering::Release); } }
            "wal_flusher_delay_ms" => { if let Ok(v) = value.parse() { self.wal_flusher_delay_ms.store(v, Ordering::Release); } }
            "synchronous_commit" => { self.synchronous_commit.store(value == "true" || value == "1", Ordering::Release); }
            "deadlock_timeout_ms" => { if let Ok(v) = value.parse() { self.deadlock_timeout_ms.store(v, Ordering::Release); } }
            "log_level" => { if let Ok(v) = value.parse() { self.log_level.store(v, Ordering::Release); } }
            _ => {} // 未知参数静默忽略
        }
    }

    /// 获取参数值（字符串形式）
    pub fn get(&self, key: &str) -> Option<String> {
        match key {
            "shared_buffers" => Some(self.shared_buffers.load(Ordering::Acquire).to_string()),
            "wal_buffers" => Some(self.wal_buffers.load(Ordering::Acquire).to_string()),
            "transaction_timeout_ms" => Some(self.transaction_timeout_ms.load(Ordering::Acquire).to_string()),
            "checkpoint_interval_ms" => Some(self.checkpoint_interval_ms.load(Ordering::Acquire).to_string()),
            "max_connections" => Some(self.max_connections.load(Ordering::Acquire).to_string()),
            "synchronous_commit" => Some(self.synchronous_commit.load(Ordering::Acquire).to_string()),
            "log_level" => Some(self.log_level.load(Ordering::Acquire).to_string()),
            _ => None,
        }
    }

    /// 列出所有参数
    pub fn list_all(&self) -> Vec<(&'static str, String)> {
        vec![
            ("shared_buffers", self.shared_buffers.load(Ordering::Acquire).to_string()),
            ("wal_buffers", self.wal_buffers.load(Ordering::Acquire).to_string()),
            ("transaction_timeout_ms", self.transaction_timeout_ms.load(Ordering::Acquire).to_string()),
            ("checkpoint_interval_ms", self.checkpoint_interval_ms.load(Ordering::Acquire).to_string()),
            ("max_connections", self.max_connections.load(Ordering::Acquire).to_string()),
            ("bgwriter_delay_ms", self.bgwriter_delay_ms.load(Ordering::Acquire).to_string()),
            ("bgwriter_max_pages", self.bgwriter_max_pages.load(Ordering::Acquire).to_string()),
            ("wal_flusher_delay_ms", self.wal_flusher_delay_ms.load(Ordering::Acquire).to_string()),
            ("synchronous_commit", self.synchronous_commit.load(Ordering::Acquire).to_string()),
            ("deadlock_timeout_ms", self.deadlock_timeout_ms.load(Ordering::Acquire).to_string()),
            ("log_level", self.log_level.load(Ordering::Acquire).to_string()),
        ]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn test_config_defaults() { let c = GlobalConfig::new(); assert_eq!(c.shared_buffers.load(Ordering::Acquire), 50000); assert!(c.synchronous_commit.load(Ordering::Acquire)); }
    #[test] fn test_config_set_get() { let c = GlobalConfig::new(); c.set("shared_buffers", "100000"); assert_eq!(c.get("shared_buffers").unwrap(), "100000"); }
    #[test] fn test_config_set_bool() { let c = GlobalConfig::new(); c.set("synchronous_commit", "false"); assert!(!c.synchronous_commit.load(Ordering::Acquire)); }
    #[test] fn test_config_set_invalid() { let c = GlobalConfig::new(); c.set("shared_buffers", "not_a_number"); assert_eq!(c.shared_buffers.load(Ordering::Acquire), 50000); }
    #[test] fn test_config_unknown_key() { let c = GlobalConfig::new(); c.set("nonexistent", "123"); assert!(c.get("nonexistent").is_none()); }
    #[test] fn test_config_load_str() { let c = GlobalConfig::new(); c.load_from_str("shared_buffers=200000\nlog_level=4\n# comment\n\nsynchronous_commit=false"); assert_eq!(c.shared_buffers.load(Ordering::Acquire), 200000); assert_eq!(c.log_level.load(Ordering::Acquire), 4); assert!(!c.synchronous_commit.load(Ordering::Acquire)); }
    #[test] fn test_config_list_all() { let c = GlobalConfig::new(); let params = c.list_all(); assert!(params.len() >= 10); }
    #[test] fn test_global_config_singleton() { let c = config(); assert_eq!(c.shared_buffers.load(Ordering::Acquire), 50000); }
}
