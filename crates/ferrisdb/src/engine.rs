//! Engine Facade — 存储引擎统一入口
//!
//! 将 BufferPool、WalWriter、TransactionManager、Catalog 等组件
//! 封装为单一入口，外部计算引擎只需与 `Engine` 交互。

use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrisdb_core::{Result, FerrisDBError};
use ferrisdb_storage::buffer::{BufferPool, BufferPoolConfig, BackgroundWriter};
use ferrisdb_storage::control::ControlFile;
use ferrisdb_storage::catalog::SystemCatalog;
use ferrisdb_storage::smgr::StorageManager;
use ferrisdb_storage::wal::{WalWriter, WalFlusher, WalRecovery, RecoveryMode};

/// 引擎配置
pub struct EngineConfig {
    /// 数据目录
    pub data_dir: PathBuf,
    /// Buffer pool 页面数
    pub shared_buffers: usize,
    /// 是否启用 WAL
    pub wal_enabled: bool,
    /// 后台写线程间隔（ms）
    pub bgwriter_delay_ms: u64,
    /// WAL flusher 间隔（ms）
    pub wal_flusher_delay_ms: u64,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("ferrisdb_data"),
            shared_buffers: 50000,
            wal_enabled: true,
            bgwriter_delay_ms: 200,
            wal_flusher_delay_ms: 5,
        }
    }
}

impl EngineConfig {
    /// 从数据目录创建默认配置
    pub fn with_data_dir<P: Into<PathBuf>>(dir: P) -> Self {
        Self { data_dir: dir.into(), ..Default::default() }
    }
}

/// FerrisDB 存储引擎 — 统一入口
///
/// 封装所有内部组件，提供简洁的外部 API。
///
/// ```ignore
/// let engine = Engine::open(EngineConfig::with_data_dir("/tmp/mydb"))?;
/// let table_id = engine.create_table("users")?;
/// let mut txn = engine.begin()?;
/// // ... use txn ...
/// txn.commit()?;
/// engine.shutdown()?;
/// ```
pub struct Engine {
    /// 存储管理器
    smgr: Arc<StorageManager>,
    /// Buffer Pool
    buffer_pool: Arc<BufferPool>,
    /// WAL Writer (None if WAL disabled)
    wal_writer: Option<Arc<WalWriter>>,
    /// 事务管理器
    txn_manager: Arc<ferrisdb_transaction::TransactionManager>,
    /// 系统目录
    catalog: Arc<SystemCatalog>,
    /// Control file
    control_file: Arc<ControlFile>,
    /// 后台写线程
    _bgwriter: Option<BackgroundWriter>,
    /// WAL flusher
    _wal_flusher: Option<WalFlusher>,
    /// 数据目录
    data_dir: PathBuf,
}

impl Engine {
    /// 打开或创建数据库引擎
    pub fn open(config: EngineConfig) -> Result<Self> {
        // 1. 初始化存储目录
        let smgr = Arc::new(StorageManager::new(&config.data_dir));
        smgr.init()?;

        // 2. 打开 control file
        let control_file = Arc::new(ControlFile::open(&config.data_dir)?);

        // 3. 创建 buffer pool
        let mut bp = BufferPool::new(BufferPoolConfig::new(config.shared_buffers))?;
        bp.set_smgr(Arc::clone(&smgr));

        // 4. WAL 设置
        let (wal_writer, wal_flusher) = if config.wal_enabled {
            let wal_dir = config.data_dir.join("wal");
            std::fs::create_dir_all(&wal_dir)
                .map_err(|e| FerrisDBError::Internal(format!("Failed to create WAL dir: {}", e)))?;
            let w = Arc::new(WalWriter::new(&wal_dir));
            bp.set_wal_writer(Arc::clone(&w));
            let f = w.start_flusher(config.wal_flusher_delay_ms);
            (Some(w), Some(f))
        } else {
            (None, None)
        };
        let bp = Arc::new(bp);

        // 5. 事务管理器
        let mut tm = ferrisdb_transaction::TransactionManager::new(
            ferrisdb_core::config::config().max_connections.load(std::sync::atomic::Ordering::Acquire) as usize,
        );
        tm.set_buffer_pool(Arc::clone(&bp));
        if let Some(ref w) = wal_writer {
            tm.set_wal_writer(Arc::clone(w));
        }
        let tm = Arc::new(tm);

        // 6. 后台写线程
        let bgwriter = Some(BackgroundWriter::start(
            Arc::clone(&bp),
            config.bgwriter_delay_ms,
            100,
        ));

        // 7. 系统目录（持久化到磁盘）
        let catalog = Arc::new(SystemCatalog::open(&config.data_dir)?);

        // 8. 崩溃恢复
        if config.wal_enabled {
            let wal_dir = config.data_dir.join("wal");
            if wal_dir.exists() {
                let recovery = WalRecovery::with_smgr(&wal_dir, Arc::clone(&smgr));
                let _ = recovery.recover(RecoveryMode::CrashRecovery);
            }
        }

        // 9. 标记为运行中
        control_file.set_state(1)?; // 1 = in production

        Ok(Self {
            smgr, buffer_pool: bp, wal_writer, txn_manager: tm,
            catalog, control_file,
            _bgwriter: bgwriter, _wal_flusher: wal_flusher,
            data_dir: config.data_dir,
        })
    }

    /// 干净关闭引擎
    pub fn shutdown(&self) -> Result<()> {
        // Flush all dirty pages
        self.buffer_pool.flush_all()?;
        // Sync WAL
        if let Some(ref w) = self.wal_writer {
            w.sync()?;
        }
        // Update control file
        self.control_file.set_state(0)?; // 0 = clean shutdown
        Ok(())
    }

    // ==================== 表管理 ====================

    // ==================== 表空间管理 ====================

    /// 创建表空间（路径名编码在 name 中：`ts_name::/path/to/dir`）
    pub fn create_tablespace(&self, name: &str, _path: &str) -> Result<u32> {
        // 表空间用 RelationType::System + name 前缀 "ts:" 标识
        let ts_name = format!("ts:{}", name);
        self.catalog.create_table(&ts_name, 0) // 复用 create_table 存储
    }

    /// 创建表（指定表空间，0 = 默认）
    pub fn create_table_in(&self, name: &str, tablespace_id: u32) -> Result<u32> {
        self.catalog.create_table(name, tablespace_id)
    }

    // ==================== 表管理 ====================

    /// 创建表（默认表空间），返回 table OID
    pub fn create_table(&self, name: &str) -> Result<u32> {
        self.catalog.create_table(name, 0)
    }

    /// 删除表
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let meta = self.catalog.lookup_by_name(name)
            .ok_or_else(|| FerrisDBError::NotFound(format!("Table '{}' not found", name)))?;
        self.catalog.drop_relation(meta.oid)
    }

    /// 获取表句柄（用于 DML 操作）
    ///
    /// 从 catalog 恢复 current_page，确保重启后 insert 不覆盖旧数据。
    pub fn open_table(&self, name: &str) -> Result<ferrisdb_transaction::HeapTable> {
        let meta = self.catalog.lookup_by_name(name)
            .ok_or_else(|| FerrisDBError::NotFound(format!("Table '{}' not found", name)))?;
        let mut table = ferrisdb_transaction::HeapTable::new(
            meta.oid, Arc::clone(&self.buffer_pool), Arc::clone(&self.txn_manager),
        );
        if let Some(ref w) = self.wal_writer {
            table.set_wal_writer(Arc::clone(w));
        }
        // 从 catalog 恢复页面计数
        if meta.current_pages > 0 {
            table.set_current_page(meta.current_pages);
        }
        // 加载持久化的 FSM
        let fsm_path = self.data_dir.join(format!("fsm_{}", meta.oid));
        let _ = table.load_fsm(&fsm_path);
        Ok(table)
    }

    /// 保存表状态到 catalog + FSM 到磁盘
    pub fn save_table_state(&self, name: &str, table: &ferrisdb_transaction::HeapTable) -> Result<()> {
        let meta = self.catalog.lookup_by_name(name)
            .ok_or_else(|| FerrisDBError::NotFound(format!("Table '{}' not found", name)))?;
        self.catalog.update_pages(meta.oid, table.get_current_page(), meta.root_page)?;
        let fsm_path = self.data_dir.join(format!("fsm_{}", meta.oid));
        table.save_fsm(&fsm_path)?;
        Ok(())
    }

    // ==================== 事务 ====================

    /// 开始事务
    pub fn begin(&self) -> Result<ferrisdb_transaction::Transaction> {
        self.txn_manager.begin()
    }

    // ==================== 索引 ====================

    /// 创建索引，返回 index OID
    pub fn create_index(&self, name: &str, table_name: &str) -> Result<u32> {
        let table_meta = self.catalog.lookup_by_name(table_name)
            .ok_or_else(|| FerrisDBError::NotFound(format!("Table '{}' not found", table_name)))?;
        self.catalog.create_index(name, table_meta.oid, 0)
    }

    /// 获取 B-Tree 索引句柄
    ///
    /// 从 catalog 恢复 root_page，确保重启后索引可用。
    pub fn open_index(&self, name: &str) -> Result<ferrisdb_storage::BTree> {
        let meta = self.catalog.lookup_by_name(name)
            .ok_or_else(|| FerrisDBError::NotFound(format!("Index '{}' not found", name)))?;
        let mut btree = ferrisdb_storage::BTree::new(meta.oid, Arc::clone(&self.buffer_pool));
        if let Some(ref w) = self.wal_writer {
            btree.set_wal_writer(Arc::clone(w));
        }
        // 从 catalog 恢复 root_page 和 next_page
        if meta.root_page != u32::MAX {
            btree.set_root_page(meta.root_page);
        }
        if meta.current_pages > 0 {
            btree.set_next_page(meta.current_pages);
        }
        Ok(btree)
    }

    /// 保存索引状态到 catalog（root_page + next_page）
    pub fn save_index_state(&self, name: &str, btree: &ferrisdb_storage::BTree) -> Result<()> {
        let meta = self.catalog.lookup_by_name(name)
            .ok_or_else(|| FerrisDBError::NotFound(format!("Index '{}' not found", name)))?;
        self.catalog.update_pages(meta.oid, btree.next_page_count(), btree.root_page())
    }

    // ==================== 访问内部组件 ====================

    /// Buffer Pool（高级用途）
    pub fn buffer_pool(&self) -> &Arc<BufferPool> { &self.buffer_pool }

    /// WAL Writer（高级用途）
    pub fn wal_writer(&self) -> Option<&Arc<WalWriter>> { self.wal_writer.as_ref() }

    /// 事务管理器（高级用途）
    pub fn txn_manager(&self) -> &Arc<ferrisdb_transaction::TransactionManager> { &self.txn_manager }

    /// 系统目录（高级用途）
    pub fn catalog(&self) -> &Arc<SystemCatalog> { &self.catalog }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_engine() -> (Engine, TempDir) {
        let td = TempDir::new().unwrap();
        let cfg = EngineConfig { data_dir: td.path().to_path_buf(), shared_buffers: 200, wal_enabled: false, ..Default::default() };
        (Engine::open(cfg).unwrap(), td)
    }

    #[test]
    fn test_engine_open_shutdown() {
        let (engine, _td) = test_engine();
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_create_table() {
        let (engine, _td) = test_engine();
        let oid = engine.create_table("users").unwrap();
        assert!(oid > 0);
    }

    #[test]
    fn test_engine_open_table() {
        let (engine, _td) = test_engine();
        engine.create_table("orders").unwrap();
        let table = engine.open_table("orders").unwrap();
        assert!(table.table_oid() > 0);
    }

    #[test]
    fn test_engine_drop_table() {
        let (engine, _td) = test_engine();
        engine.create_table("temp").unwrap();
        engine.drop_table("temp").unwrap();
        assert!(engine.open_table("temp").is_err());
    }

    #[test]
    fn test_engine_begin_commit() {
        let (engine, _td) = test_engine();
        let mut txn = engine.begin().unwrap();
        txn.commit().unwrap();
    }

    #[test]
    fn test_engine_insert_fetch() {
        let (engine, _td) = test_engine();
        engine.create_table("test").unwrap();
        let table = engine.open_table("test").unwrap();
        let tid = table.insert(b"hello world", ferrisdb_core::Xid::new(0, 1), 0).unwrap();
        let (_, data) = table.fetch(tid).unwrap().unwrap();
        assert_eq!(data, b"hello world");
    }

    #[test]
    fn test_engine_create_index() {
        let (engine, _td) = test_engine();
        engine.create_table("t").unwrap();
        let idx_oid = engine.create_index("t_idx", "t").unwrap();
        assert!(idx_oid > 0);
    }

    #[test]
    fn test_engine_with_wal() {
        let td = TempDir::new().unwrap();
        let cfg = EngineConfig { data_dir: td.path().to_path_buf(), shared_buffers: 200, wal_enabled: true, ..Default::default() };
        let engine = Engine::open(cfg).unwrap();
        engine.create_table("wal_test").unwrap();
        let table = engine.open_table("wal_test").unwrap();
        table.insert(b"persisted", ferrisdb_core::Xid::new(0, 1), 0).unwrap();
        engine.shutdown().unwrap();
    }

    #[test]
    fn test_engine_not_found() {
        let (engine, _td) = test_engine();
        assert!(engine.open_table("ghost").is_err());
        assert!(engine.drop_table("ghost").is_err());
    }

    #[test]
    fn test_engine_default_config() {
        let cfg = EngineConfig::default();
        assert_eq!(cfg.shared_buffers, 50000);
        assert!(cfg.wal_enabled);
    }
}
