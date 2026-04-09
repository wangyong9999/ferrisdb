//! Engine Facade — 存储引擎统一入口
//!
//! 将 BufferPool、WalWriter、TransactionManager、Catalog 等组件
//! 封装为单一入口，外部计算引擎只需与 `Engine` 交互。

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use ferrisdb_core::{Result, FerrisDBError};
use parking_lot::RwLock;
use ferrisdb_storage::buffer::{BufferPool, BufferPoolConfig, BackgroundWriter};
use ferrisdb_storage::control::ControlFile;
use ferrisdb_storage::catalog::SystemCatalog;
use ferrisdb_storage::smgr::StorageManager;
use ferrisdb_storage::wal::{WalWriter, WalFlusher, WalRecovery, RecoveryMode, CheckpointManager, CheckpointConfig, CheckpointType};

/// 引擎运行状态（供运维监控）
#[derive(Debug, Clone)]
pub struct EngineStats {
    /// Buffer pool 命中率 (0.0 ~ 1.0)
    pub buffer_pool_hit_rate: f64,
    /// Buffer pool 总 pin 次数
    pub buffer_pool_pins: u64,
    /// 当前脏页数
    pub buffer_pool_dirty_pages: u64,
    /// 活跃事务数
    pub active_transactions: u64,
    /// WAL 当前写入偏移
    pub wal_offset: u64,
    /// WAL RingBuffer 未 flush 数据量
    pub wal_ring_unflushed: u64,
    /// 最近 checkpoint LSN
    pub checkpoint_lsn: u64,
    /// 是否正在关闭
    pub is_shutdown: bool,
}

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
    /// 数据目录锁文件（持有期间独占数据目录，防止并发打开）
    _lockfile: std::fs::File,
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
    /// 表级 Metadata Lock（DDL 排他，DML 共享，防止 DROP+INSERT 并发）
    table_locks: RwLock<std::collections::HashMap<u32, Arc<parking_lot::RwLock<()>>>>,
    /// Control file
    control_file: Arc<ControlFile>,
    /// 后台写线程
    _bgwriter: Option<BackgroundWriter>,
    /// WAL flusher
    _wal_flusher: Option<WalFlusher>,
    /// Checkpoint manager
    checkpoint_manager: Option<Arc<CheckpointManager>>,
    /// Checkpoint 后台线程
    _checkpoint_handle: Option<JoinHandle<()>>,
    /// WAL RingBuffer（DML 无锁写入）
    wal_ring: Option<Arc<ferrisdb_storage::wal::ring_buffer::WalRingBuffer>>,
    /// WAL drain 线程
    _wal_drain_handle: Option<JoinHandle<()>>,
    /// AutoVacuum 后台线程
    _autovacuum_handle: Option<JoinHandle<()>>,
    /// AutoVacuum 停止信号
    autovacuum_shutdown: Arc<std::sync::atomic::AtomicBool>,
    /// 注册的 HeapTable 列表（供 AutoVacuum 扫描）
    registered_tables: Arc<RwLock<Vec<Arc<ferrisdb_transaction::HeapTable>>>>,
    /// 数据目录
    data_dir: PathBuf,
    /// 是否已关闭（阻止新事务）
    is_shutdown: std::sync::atomic::AtomicBool,
}

impl Engine {
    /// 打开或创建数据库引擎
    pub fn open(config: EngineConfig) -> Result<Self> {
        // 0. 数据目录锁——防止多进程同时打开同一数据库
        std::fs::create_dir_all(&config.data_dir)
            .map_err(|e| FerrisDBError::Internal(format!("Cannot create data dir: {}", e)))?;
        let lock_path = config.data_dir.join(".ferrisdb.lock");
        let lockfile = std::fs::OpenOptions::new()
            .write(true).create(true).open(&lock_path)
            .map_err(|e| FerrisDBError::Internal(format!("Cannot open lockfile: {}", e)))?;
        use std::os::unix::fs::FileExt;
        // try_lock_exclusive via fcntl
        let fd = std::os::unix::io::AsRawFd::as_raw_fd(&lockfile);
        let ret = unsafe {
            libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB)
        };
        if ret != 0 {
            return Err(FerrisDBError::Internal(
                "Database is already in use by another process".to_string()
            ));
        }

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

        // 4b. WAL RingBuffer + drain 线程
        let (wal_ring, wal_drain_handle) = if let Some(ref w) = wal_writer {
            let ring = Arc::new(ferrisdb_storage::wal::ring_buffer::WalRingBuffer::new(16 * 1024 * 1024));
            let handle = ferrisdb_storage::wal::ring_buffer::start_drain_thread(
                Arc::clone(&ring), Arc::clone(w), 5,
            );
            (Some(ring), Some(handle))
        } else {
            (None, None)
        };

        // 5. 事务管理器
        let mut tm = ferrisdb_transaction::TransactionManager::new(
            ferrisdb_core::config::config().max_connections.load(std::sync::atomic::Ordering::Acquire) as usize,
        );
        tm.set_buffer_pool(Arc::clone(&bp));
        if let Some(ref w) = wal_writer {
            tm.set_wal_writer(Arc::clone(w));
        }
        if let Some(ref ring) = wal_ring {
            tm.set_wal_ring(Arc::clone(ring));
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
                recovery.recover(RecoveryMode::CrashRecovery)?;
            }
        }

        // 9. 启动 Checkpoint 后台线程
        let (checkpoint_manager, checkpoint_handle) = if let Some(ref w) = wal_writer {
            let mut cm = CheckpointManager::new(
                CheckpointConfig {
                    interval_ms: 60_000,            // 每 60 秒检查
                    wal_size_threshold: 32 * 1024 * 1024, // 32MB WAL 增长触发
                    dirty_page_ratio_threshold: 0.5,
                    auto_checkpoint: true,
                },
                Arc::clone(w),
            );
            cm.set_flusher(Arc::clone(&bp) as Arc<dyn ferrisdb_storage::wal::DirtyPageFlusher>);
            cm.set_control_file(Arc::clone(&control_file));
            if let Some(ref ring) = wal_ring {
                cm.set_wal_ring(Arc::clone(ring));
            }
            let cm = Arc::new(cm);
            let handle = Arc::clone(&cm).start_auto_checkpoint();
            (Some(cm), Some(handle))
        } else {
            (None, None)
        };

        // 10. AutoVacuum 后台线程
        let autovacuum_shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let registered_tables: Arc<RwLock<Vec<Arc<ferrisdb_transaction::HeapTable>>>> =
            Arc::new(RwLock::new(Vec::new()));
        let autovacuum_handle = {
            let shutdown = Arc::clone(&autovacuum_shutdown);
            let tables = Arc::clone(&registered_tables);
            let txn_mgr_bg = Arc::clone(&tm);
            thread::Builder::new().name("autovacuum".into()).spawn(move || {
                loop {
                    if shutdown.load(std::sync::atomic::Ordering::Acquire) { break; }
                    thread::sleep(std::time::Duration::from_secs(10));
                    if shutdown.load(std::sync::atomic::Ordering::Acquire) { break; }
                    // 1. 超时事务强制 abort
                    let aborted = txn_mgr_bg.abort_timed_out_transactions();
                    if aborted > 0 {
                        eprintln!("[autovacuum] force-aborted {} timed-out transactions", aborted);
                    }
                    // 2. Vacuum dead tuples
                    let tables = tables.read().clone();
                    for table in &tables {
                        let _ = table.vacuum();
                    }
                }
            }).ok()
        };

        // 11. 标记为运行中
        control_file.set_state(1)?; // 1 = in production

        Ok(Self {
            _lockfile: lockfile,
            smgr, buffer_pool: bp, wal_writer, txn_manager: tm,
            catalog, control_file,
            table_locks: RwLock::new(std::collections::HashMap::new()),
            _bgwriter: bgwriter, _wal_flusher: wal_flusher,
            checkpoint_manager, _checkpoint_handle: checkpoint_handle,
            wal_ring: wal_ring.clone(), _wal_drain_handle: wal_drain_handle,
            _autovacuum_handle: autovacuum_handle,
            autovacuum_shutdown,
            registered_tables,
            data_dir: config.data_dir,
            is_shutdown: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// 干净关闭引擎
    pub fn shutdown(&self) -> Result<()> {
        // 1. 标记为关闭中 — 阻止新事务
        self.is_shutdown.store(true, std::sync::atomic::Ordering::Release);
        self.txn_manager.initiate_shutdown();
        // 2. 等待在途事务完成（最多 5 秒）
        let remaining = self.txn_manager.wait_for_all_transactions(5000);
        if remaining > 0 {
            eprintln!("Warning: {} active transactions still running at shutdown", remaining);
        }
        // 3. 停止 RingBuffer drain 线程（确保所有 WAL 数据落盘）
        if let Some(ref ring) = self.wal_ring {
            ring.shutdown(); // 信号 drain 线程退出（退出前会 final flush）
        }
        // 等待 drain 线程完成 final flush（最多 2 秒）
        std::thread::sleep(std::time::Duration::from_millis(100));

        // 4. 停止其他后台线程
        self.autovacuum_shutdown.store(true, std::sync::atomic::Ordering::Release);
        if let Some(ref cm) = self.checkpoint_manager {
            cm.stop();
        }
        // 5. 执行关闭 checkpoint（flush all dirty + write checkpoint record）
        if let Some(ref cm) = self.checkpoint_manager {
            let _ = cm.checkpoint(CheckpointType::Shutdown);
        } else {
            // 无 checkpoint manager，直接 flush
            self.buffer_pool.flush_all()?;
        }
        // 4. Sync WAL
        if let Some(ref w) = self.wal_writer {
            w.sync()?;
        }
        // 5. fsync 所有数据文件（确保脏页持久化到磁盘）
        self.smgr.sync_all()?;
        // 6. Update control file
        self.control_file.set_state(0)?; // 0 = clean shutdown
        Ok(())
    }

    /// 获取表的 metadata lock 底层 Arc
    fn get_table_lock(&self, oid: u32) -> Arc<parking_lot::RwLock<()>> {
        let locks = self.table_locks.read();
        if let Some(lock) = locks.get(&oid) {
            return Arc::clone(lock);
        }
        drop(locks);
        let mut locks = self.table_locks.write();
        locks.entry(oid).or_insert_with(|| Arc::new(parking_lot::RwLock::new(()))).clone()
    }

    /// 注册 HeapTable 供 AutoVacuum 管理
    pub fn register_table(&self, table: Arc<ferrisdb_transaction::HeapTable>) {
        self.registered_tables.write().push(table);
    }

    /// 检查引擎是否已关闭
    pub fn is_shutdown(&self) -> bool {
        self.is_shutdown.load(std::sync::atomic::Ordering::Acquire)
    }

    /// 获取引擎运行状态（供运维监控）
    pub fn stats(&self) -> EngineStats {
        EngineStats {
            buffer_pool_hit_rate: self.buffer_pool.hit_rate(),
            buffer_pool_pins: self.buffer_pool.stat_pins(),
            buffer_pool_dirty_pages: self.buffer_pool.dirty_page_count() as u64,
            active_transactions: self.txn_manager.active_transaction_count() as u64,
            wal_offset: self.wal_writer.as_ref().map_or(0, |w| w.offset()),
            wal_ring_unflushed: self.wal_ring.as_ref().map_or(0, |r| r.unflushed()),
            checkpoint_lsn: self.control_file.checkpoint_lsn().raw(),
            is_shutdown: self.is_shutdown(),
        }
    }

    // ==================== 表管理 ====================

    // ==================== 表空间管理 ====================

    /// 创建表空间
    pub fn create_tablespace(&self, name: &str, _path: &str) -> Result<u32> {
        let ts_name = format!("ts:{}", name);
        let oid = self.catalog.create_table(&ts_name, 0)?;
        self.write_ddl_wal(ferrisdb_storage::wal::WalRecordType::DdlCreate, oid, &ts_name);
        Ok(oid)
    }

    /// 创建表（指定表空间，0 = 默认）
    pub fn create_table_in(&self, name: &str, tablespace_id: u32) -> Result<u32> {
        let oid = self.catalog.create_table(name, tablespace_id)?;
        self.write_ddl_wal(ferrisdb_storage::wal::WalRecordType::DdlCreate, oid, name);
        Ok(oid)
    }

    /// 创建临时表（不写 WAL，进程退出时自动丢弃）
    pub fn create_temp_table(&self, name: &str) -> Result<u32> {
        // 临时表用 tablespace_id = u32::MAX 标识
        self.catalog.create_table(name, u32::MAX)
        // 不写 DDL WAL — 临时表不需要持久化
    }

    /// 打开临时表（不带 WAL writer）
    pub fn open_temp_table(&self, name: &str) -> Result<ferrisdb_transaction::HeapTable> {
        let meta = self.catalog.lookup_by_name(name)
            .ok_or_else(|| FerrisDBError::NotFound(format!("Table '{}' not found", name)))?;
        // 临时表不设置 WAL writer
        let table = ferrisdb_transaction::HeapTable::new(
            meta.oid, Arc::clone(&self.buffer_pool), Arc::clone(&self.txn_manager),
        );
        Ok(table)
    }

    /// 检查表是否是临时表
    pub fn is_temp_table(&self, name: &str) -> bool {
        self.catalog.lookup_by_name(name)
            .map(|m| m.tablespace_id == u32::MAX)
            .unwrap_or(false)
    }

    // ==================== 表管理 ====================

    /// 创建表（默认表空间），返回 table OID
    pub fn create_table(&self, name: &str) -> Result<u32> {
        let oid = self.catalog.create_table(name, 0)?;
        self.write_ddl_wal(ferrisdb_storage::wal::WalRecordType::DdlCreate, oid, name);
        Ok(oid)
    }

    /// 删除表
    pub fn drop_table(&self, name: &str) -> Result<()> {
        let meta = self.catalog.lookup_by_name(name)
            .ok_or_else(|| FerrisDBError::NotFound(format!("Table '{}' not found", name)))?;
        let oid = meta.oid;
        // 排他 MDL：阻塞并发 DML，确保无进行中的读写
        let mdl = self.get_table_lock(oid);
        let _mdl_guard = mdl.write();
        self.catalog.drop_relation(oid)?;
        self.write_ddl_wal(ferrisdb_storage::wal::WalRecordType::DdlDrop, oid, name);
        Ok(())
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
        if self.is_shutdown() {
            return Err(FerrisDBError::InvalidState("Engine is shutting down".to_string()));
        }
        self.txn_manager.begin()
    }

    // ==================== 索引 ====================

    /// 创建索引，返回 index OID
    pub fn create_index(&self, name: &str, table_name: &str) -> Result<u32> {
        let table_meta = self.catalog.lookup_by_name(table_name)
            .ok_or_else(|| FerrisDBError::NotFound(format!("Table '{}' not found", table_name)))?;
        let oid = self.catalog.create_index(name, table_meta.oid, 0)?;
        self.write_ddl_wal(ferrisdb_storage::wal::WalRecordType::DdlCreate, oid, name);
        Ok(oid)
    }

    /// 并发建索引 — 扫描表快照构建索引，不阻塞 DML
    ///
    /// 1. 创建索引元数据
    /// 2. 扫描当前表所有行构建索引
    /// 3. 保存索引状态
    pub fn create_index_concurrently(
        &self, name: &str, table_name: &str,
        key_extractor: impl Fn(&[u8]) -> ferrisdb_storage::BTreeKey,
    ) -> Result<u32> {
        let oid = self.create_index(name, table_name)?;
        let table = self.open_table(table_name)?;
        let btree = self.open_index(name)?;
        btree.init()?;

        // Scan table and populate index (non-blocking — table DML continues)
        let mut scan = ferrisdb_transaction::HeapScan::new(&table);
        while let Ok(Some((tid, _header, data))) = scan.next() {
            let key = key_extractor(&data);
            let value = ferrisdb_storage::BTreeValue::Tuple { block: tid.ip_blkid, offset: tid.ip_posid };
            btree.insert(key, value)?;
        }

        self.save_index_state(name, &btree)?;
        Ok(oid)
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
        // 恢复 free pages
        self.restore_index_free_pages(name, &btree);
        Ok(btree)
    }

    /// 保存索引状态到 catalog（root_page + next_page）
    pub fn save_index_state(&self, name: &str, btree: &ferrisdb_storage::BTree) -> Result<()> {
        let meta = self.catalog.lookup_by_name(name)
            .ok_or_else(|| FerrisDBError::NotFound(format!("Index '{}' not found", name)))?;
        self.catalog.update_pages(meta.oid, btree.next_page_count(), btree.root_page())?;
        // 持久化 free pages 列表
        let free_pages = btree.get_free_pages();
        if !free_pages.is_empty() {
            let path = self.data_dir.join(format!("idx_{}.freepages", name));
            let data: Vec<u8> = free_pages.iter().flat_map(|p| p.to_le_bytes()).collect();
            std::fs::write(&path, &data)
                .map_err(|e| FerrisDBError::Internal(format!("Failed to save free pages: {}", e)))?;
        }
        Ok(())
    }

    /// 恢复索引的 free pages 列表
    fn restore_index_free_pages(&self, name: &str, btree: &ferrisdb_storage::BTree) {
        let path = self.data_dir.join(format!("idx_{}.freepages", name));
        if let Ok(data) = std::fs::read(&path) {
            let pages: Vec<u32> = data.chunks_exact(4)
                .map(|c| u32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();
            if !pages.is_empty() {
                btree.set_free_pages(pages);
            }
        }
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

    // ==================== 内部方法 ====================

    /// 写入 DDL WAL 记录（create/drop table/index/tablespace）
    fn write_ddl_wal(&self, rtype: ferrisdb_storage::wal::WalRecordType, oid: u32, name: &str) {
        if let Some(ref w) = self.wal_writer {
            let header = ferrisdb_storage::wal::WalRecordHeader::new(rtype, (4 + name.len()) as u16);
            let hdr_size = std::mem::size_of::<ferrisdb_storage::wal::WalRecordHeader>();
            let mut buf = Vec::with_capacity(hdr_size + 4 + name.len());
            buf.extend_from_slice(unsafe {
                std::slice::from_raw_parts(&header as *const _ as *const u8, hdr_size)
            });
            buf.extend_from_slice(&oid.to_le_bytes());
            buf.extend_from_slice(name.as_bytes());
            let _ = w.write(&buf);
        }
    }
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

    #[test]
    fn test_engine_save_load_index_free_pages() {
        let td = tempfile::TempDir::new().unwrap();
        let engine = Engine::open(EngineConfig {
            data_dir: td.path().to_path_buf(),
            shared_buffers: 100,
            wal_enabled: false,
            ..Default::default()
        }).unwrap();
        engine.create_table("fp_table").unwrap();
        engine.create_index("fp_idx", "fp_table").unwrap();
        let idx = engine.open_index("fp_idx").unwrap();
        idx.set_free_pages(vec![10, 20, 30]);
        engine.save_index_state("fp_idx", &idx).unwrap();

        let idx2 = engine.open_index("fp_idx").unwrap();
        engine.restore_index_free_pages("fp_idx", &idx2);
        let pages = idx2.get_free_pages();
        assert_eq!(pages, vec![10, 20, 30]);
        engine.shutdown().unwrap();
    }
}
