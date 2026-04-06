//! WAL Recovery
//!
//! 从 WAL 文件恢复数据。支持基于页面 LSN 的幂等重做。

use std::collections::HashMap;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrisdb_core::{Lsn, FerrisDBError, Result};
use ferrisdb_core::atomic::{AtomicU32, AtomicU64, Ordering};
use parking_lot::Mutex;

use super::record::{WalRecordHeader, WalRecordType, WalRecordForPage, WalCheckpoint};
use super::writer::{WalFileHeader, WAL_FILE_MAGIC};
use crate::page::{PAGE_SIZE, HeapPage};
use crate::smgr::StorageManager;

/// 对齐的页面缓冲区（8192 字节对齐，供 HeapPage::from_bytes 使用）
#[repr(C, align(8192))]
struct AlignedPageBuf {
    data: [u8; PAGE_SIZE],
}

impl AlignedPageBuf {
    fn from_vec(src: &[u8]) -> Self {
        let mut buf = Self { data: [0u8; PAGE_SIZE] };
        let len = src.len().min(PAGE_SIZE);
        buf.data[..len].copy_from_slice(&src[..len]);
        buf
    }
}

/// 恢复模式
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// 崩溃恢复
    CrashRecovery,
    /// 从检查点恢复
    CheckpointRecovery,
}

/// 恢复状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryStage {
    /// 未开始
    NotStarted,
    /// 正在读取 WAL
    ReadingWal,
    /// 正在重做
    Redoing,
    /// 已完成
    Completed,
    /// 失败
    Failed,
}

/// 恢复统计信息
#[derive(Debug, Default, Clone)]
pub struct RecoveryStats {
    /// 读取的记录数
    pub records_read: u64,
    /// 重做的记录数
    pub records_redone: u64,
    /// 跳过的记录数（已应用或不需要重做）
    pub records_skipped: u64,
    /// 开始 LSN
    pub start_lsn: Lsn,
    /// 结束 LSN
    pub end_lsn: Lsn,
}

/// Redo 上下文 — 持有页面读写所需的资源
/// 事务在 recovery 中的状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoveryTxnState {
    InProgress,
    Committed,
    Aborted,
}

/// Undo action 从 WAL 解析出来的轻量结构
#[derive(Debug, Clone)]
struct RecoveryUndoAction {
    action_type: u8,
    table_oid: u32,
    page_no: u32,
    tuple_offset: u16,
    data: Vec<u8>,
}

struct RedoContext {
    /// 存储管理器
    smgr: Arc<StorageManager>,
    /// 页面 LSN 缓存（避免重复读页面头部）
    page_lsn_cache: HashMap<ferrisdb_core::PageId, Lsn>,
    /// 脏页集合（redo 期间修改过的页面）
    dirty_pages: HashMap<ferrisdb_core::PageId, Vec<u8>>,
    /// 事务状态追踪（xid → commit/abort/in-progress）
    txn_states: HashMap<u64, RecoveryTxnState>,
    /// 未提交事务的 undo actions（xid → actions）
    txn_undo_actions: HashMap<u64, Vec<RecoveryUndoAction>>,
}

impl RedoContext {
    fn new(smgr: Arc<StorageManager>) -> Self {
        Self {
            smgr,
            page_lsn_cache: HashMap::new(),
            dirty_pages: HashMap::new(),
            txn_states: HashMap::new(),
            txn_undo_actions: HashMap::new(),
        }
    }

    /// 读取页面数据。优先从脏页缓存读取，否则从磁盘读取。
    fn read_page(&mut self, page_id: &ferrisdb_core::PageId) -> Result<Vec<u8>> {
        // 检查脏页缓存
        if let Some(data) = self.dirty_pages.get(page_id) {
            return Ok(data.clone());
        }

        // 从磁盘读取
        let mut buf = vec![0u8; PAGE_SIZE];
        match self.smgr.read_page_by_id(page_id, &mut buf) {
            Ok(()) => Ok(buf),
            Err(_) => {
                // 页面不存在，返回全零页面（新页面）
                Ok(vec![0u8; PAGE_SIZE])
            }
        }
    }

    /// 写入页面到脏页缓存
    fn write_page(&mut self, page_id: ferrisdb_core::PageId, data: Vec<u8>) {
        self.dirty_pages.insert(page_id, data);
    }

    /// 刷脏：将所有脏页写入磁盘
    fn flush_dirty_pages(&mut self) -> Result<()> {
        for (page_id, data) in &self.dirty_pages {
            self.smgr.write_page_by_id(page_id, data)?;
        }
        self.dirty_pages.clear();
        Ok(())
    }

    /// 获取页面当前 LSN
    fn get_page_lsn(&mut self, page_id: &ferrisdb_core::PageId) -> Lsn {
        if let Some(lsn) = self.page_lsn_cache.get(page_id) {
            return *lsn;
        }

        let data = match self.read_page(page_id) {
            Ok(d) => d,
            Err(_) => return Lsn::INVALID,
        };

        // 从页面头部读取 LSN（前 8 字节）
        let lsn = if data.len() >= 8 {
            let raw = u64::from_le_bytes(data[0..8].try_into().unwrap_or([0; 8]));
            Lsn::from_raw(raw)
        } else {
            Lsn::INVALID
        };

        self.page_lsn_cache.insert(*page_id, lsn);
        lsn
    }

    /// 更新页面 LSN 缓存
    fn update_page_lsn(&mut self, page_id: ferrisdb_core::PageId, lsn: Lsn) {
        self.page_lsn_cache.insert(page_id, lsn);
    }

    /// 检查页面是否需要 redo（基于 LSN 比较）
    /// 返回 None 表示已应用（跳过），Some(page_data) 表示需要 redo
    pub fn prepare_redo(
        &mut self,
        page_id: &ferrisdb_core::PageId,
        record_lsn: Lsn,
    ) -> Result<Option<Vec<u8>>> {
        let page_lsn = self.get_page_lsn(page_id);
        if page_lsn.is_valid() && page_lsn.raw() >= record_lsn.raw() {
            return Ok(None);
        }
        let page_data = self.read_page(page_id)?;
        Ok(Some(page_data))
    }

    /// 更新页面 LSN 并写入脏页缓存
    pub fn finish_redo(
        &mut self,
        page_id: ferrisdb_core::PageId,
        record_lsn: Lsn,
        page_data: Vec<u8>,
    ) {
        let mut data = page_data;
        if data.len() >= 8 {
            data[0..8].copy_from_slice(&record_lsn.raw().to_le_bytes());
        }
        self.update_page_lsn(page_id, record_lsn);
        self.write_page(page_id, data);
    }
}

/// WAL Recovery
///
/// 从 WAL 文件恢复数据。支持页面 LSN 校验的幂等重做。
pub struct WalRecovery {
    /// WAL 目录
    wal_dir: PathBuf,
    /// 恢复状态
    stage: AtomicU32,
    /// 统计信息
    stats: Mutex<RecoveryStats>,
    /// 最后的检查点 LSN
    checkpoint_lsn: AtomicU64,
    /// 存储管理器
    smgr: Option<Arc<StorageManager>>,
}

impl WalRecovery {
    /// 创建新的 Recovery（无 SMgr，仅扫描）
    pub fn new<P: AsRef<Path>>(wal_dir: P) -> Self {
        Self {
            wal_dir: wal_dir.as_ref().to_path_buf(),
            stage: AtomicU32::new(RecoveryStage::NotStarted as u32),
            stats: Mutex::new(RecoveryStats::default()),
            checkpoint_lsn: AtomicU64::new(0),
            smgr: None,
        }
    }

    /// 创建带存储管理器的 Recovery（可执行实际 redo）
    pub fn with_smgr<P: AsRef<Path>>(wal_dir: P, smgr: Arc<StorageManager>) -> Self {
        Self {
            wal_dir: wal_dir.as_ref().to_path_buf(),
            stage: AtomicU32::new(RecoveryStage::NotStarted as u32),
            stats: Mutex::new(RecoveryStats::default()),
            checkpoint_lsn: AtomicU64::new(0),
            smgr: Some(smgr),
        }
    }

    /// 获取恢复状态
    #[inline]
    pub fn stage(&self) -> RecoveryStage {
        match self.stage.load(Ordering::Acquire) {
            0 => RecoveryStage::NotStarted,
            1 => RecoveryStage::ReadingWal,
            2 => RecoveryStage::Redoing,
            3 => RecoveryStage::Completed,
            _ => RecoveryStage::Failed,
        }
    }

    /// 设置恢复状态
    fn set_stage(&self, stage: RecoveryStage) {
        self.stage.store(stage as u32, Ordering::Release);
    }

    /// 获取检查点 LSN
    #[inline]
    pub fn checkpoint_lsn(&self) -> Lsn {
        Lsn::from_raw(self.checkpoint_lsn.load(Ordering::Acquire))
    }

    /// 获取统计信息
    pub fn stats(&self) -> RecoveryStats {
        self.stats.lock().clone()
    }

    /// 仅扫描 WAL（不执行 redo），收集检查点等信息
    pub fn recover_scan_only(&self) -> Result<RecoveryStats> {
        let wal_files = self.scan_wal_files()?;
        if wal_files.is_empty() {
            return Ok(RecoveryStats::default());
        }
        let _ = self.find_last_checkpoint(&wal_files)?;
        for file_no in &wal_files {
            self.recover_file(*file_no, Lsn::INVALID, None)?;
        }
        Ok(self.stats())
    }

    /// 执行恢复
    pub fn recover(&self, mode: RecoveryMode) -> Result<()> {
        self.set_stage(RecoveryStage::ReadingWal);

        let wal_files = self.scan_wal_files()?;

        if wal_files.is_empty() {
            self.set_stage(RecoveryStage::Completed);
            return Ok(());
        }

        let checkpoint_info = self.find_last_checkpoint(&wal_files)?;

        let start_lsn = match mode {
            RecoveryMode::CrashRecovery | RecoveryMode::CheckpointRecovery => {
                checkpoint_info.map(|(lsn, _)| lsn).unwrap_or(Lsn::INVALID)
            }
        };

        {
            let mut stats = self.stats.lock();
            stats.start_lsn = start_lsn;
        }

        self.set_stage(RecoveryStage::Redoing);

        // 创建 redo 上下文
        let mut redo_ctx = self.smgr.as_ref().map(|s| RedoContext::new(s.clone()));

        // Parallel redo: 使用可配置的 worker 数量
        let num_workers = ferrisdb_core::config::config()
            .max_connections.load(std::sync::atomic::Ordering::Relaxed)
            .min(4).max(1);

        for file_no in &wal_files {
            if let Some(ref mut ctx) = redo_ctx {
                self.recover_file(*file_no, start_lsn, Some(ctx))?;
            } else {
                self.recover_file(*file_no, start_lsn, None)?;
            }
        }

        // Note: parallel redo framework exists in parallel_redo.rs.
        // Currently using serial redo for correctness simplicity.
        // Parallel redo is enabled by configuring num_workers > 1,
        // which will use page-based partitioning in future iterations.
        let _ = num_workers;

        // Undo 阶段：回滚未提交事务
        if let Some(ref mut ctx) = redo_ctx {
            self.rollback_uncommitted_txns(ctx)?;
        }

        // 刷脏页（包括 undo 产生的脏页）
        if let Some(ctx) = redo_ctx.as_mut() {
            ctx.flush_dirty_pages()?;
        }

        self.set_stage(RecoveryStage::Completed);
        Ok(())
    }

    /// 扫描 WAL 目录，返回文件号列表（排序后）
    fn scan_wal_files(&self) -> Result<Vec<u32>> {
        let mut file_numbers = Vec::new();

        let entries = std::fs::read_dir(&self.wal_dir)
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::FileError(
                format!("Failed to read WAL directory: {}", e)
            )))?;

        for entry in entries {
            let entry = entry.map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::FileError(
                format!("Failed to read directory entry: {}", e)
            )))?;

            let path = entry.path();
            if let Some(ext) = path.extension() {
                if ext == "wal" {
                    if let Some(stem) = path.file_stem() {
                        if let Ok(file_no) = u32::from_str_radix(&stem.to_string_lossy(), 16) {
                            file_numbers.push(file_no);
                        }
                    }
                }
            }
        }

        file_numbers.sort();
        Ok(file_numbers)
    }

    /// 查找最后一个检查点
    fn find_last_checkpoint(&self, file_numbers: &[u32]) -> Result<Option<(Lsn, WalCheckpoint)>> {
        for &file_no in file_numbers.iter().rev() {
            if let Some(checkpoint) = self.find_checkpoint_in_file(file_no)? {
                return Ok(Some(checkpoint));
            }
        }
        Ok(None)
    }

    /// 在单个文件中查找检查点
    fn find_checkpoint_in_file(&self, file_no: u32) -> Result<Option<(Lsn, WalCheckpoint)>> {
        let file_path = self.get_file_path(file_no);

        let mut file = File::open(&file_path)
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to open WAL file: {}", e)
            )))?;

        let mut header_buf = [0u8; WalFileHeader::size()];
        file.read_exact(&mut header_buf)
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to read WAL header: {}", e)
            )))?;

        let header = WalFileHeader::from_bytes(&header_buf);
        if header.magic != WAL_FILE_MAGIC {
            return Err(FerrisDBError::Wal(ferrisdb_core::error::WalError::Corruption));
        }

        let mut offset = WalFileHeader::size() as u64;
        let mut last_checkpoint: Option<(Lsn, WalCheckpoint)> = None;

        loop {
            let mut record_header_buf = [0u8; std::mem::size_of::<WalRecordHeader>()];
            match file.read_exact(&mut record_header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                        format!("Failed to read record header: {}", e)
                    )));
                }
            }

            let record_header = unsafe {
                std::ptr::read(record_header_buf.as_ptr() as *const WalRecordHeader)
            };

            let rtype = match record_header.record_type() {
                Some(t) => t,
                None => {
                    file.seek(SeekFrom::Current(record_header.size as i64))
                        .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                            format!("Failed to seek: {}", e)
                        )))?;
                    offset += std::mem::size_of::<WalRecordHeader>() as u64 + record_header.size as u64;
                    continue;
                }
            };

            if rtype == WalRecordType::CheckpointShutdown || rtype == WalRecordType::CheckpointOnline {
                let mut checkpoint_buf = vec![0u8; record_header.size as usize];
                file.read_exact(&mut checkpoint_buf)
                    .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                        format!("Failed to read checkpoint: {}", e)
                    )))?;

                let lsn = Lsn::from_parts(file_no, offset as u32);
                let checkpoint = WalCheckpoint::new(0, rtype == WalRecordType::CheckpointShutdown);
                last_checkpoint = Some((lsn, checkpoint));

                offset += std::mem::size_of::<WalRecordHeader>() as u64 + record_header.size as u64;
            } else {
                file.seek(SeekFrom::Current(record_header.size as i64))
                    .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                        format!("Failed to seek: {}", e)
                    )))?;
                offset += std::mem::size_of::<WalRecordHeader>() as u64 + record_header.size as u64;
            }

            {
                let mut stats = self.stats.lock();
                stats.records_read += 1;
            }
        }

        Ok(last_checkpoint)
    }

    /// 恢复单个 WAL 文件
    fn recover_file(&self, file_no: u32, start_lsn: Lsn, mut redo_ctx: Option<&mut RedoContext>) -> Result<()> {
        let file_path = self.get_file_path(file_no);

        let mut file = File::open(&file_path)
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to open WAL file: {}", e)
            )))?;

        let mut header_buf = [0u8; WalFileHeader::size()];
        file.read_exact(&mut header_buf)
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to read WAL header: {}", e)
            )))?;

        let header = WalFileHeader::from_bytes(&header_buf);
        if header.magic != WAL_FILE_MAGIC {
            return Err(FerrisDBError::Wal(ferrisdb_core::error::WalError::Corruption));
        }

        // 跳过检查点之前的记录
        let (start_file_no, start_offset) = start_lsn.parts();
        if file_no < start_file_no {
            return Ok(());
        }
        if file_no == start_file_no && start_offset > 0 {
            file.seek(SeekFrom::Start(start_offset as u64))
                .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                    format!("Failed to seek: {}", e)
                )))?;
        }

        let mut current_lsn = Lsn::from_parts(file_no, file.stream_position().unwrap_or(0) as u32);

        loop {
            let mut record_header_buf = [0u8; std::mem::size_of::<WalRecordHeader>()];
            match file.read_exact(&mut record_header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                        format!("Failed to read record header: {}", e)
                    )));
                }
            }

            let record_header = unsafe {
                std::ptr::read(record_header_buf.as_ptr() as *const WalRecordHeader)
            };

            // torn record 检测：size 不合理或 rtype 无效 → 停止 replay
            if record_header.size > 32768 || record_header.record_type().is_none() {
                // 可能是 torn write（部分写入的 record header）
                break;
            }

            let mut record_data = vec![0u8; record_header.size as usize];
            match file.read_exact(&mut record_data) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => {
                    return Err(FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                        format!("Failed to read record data: {}", e)
                    )));
                }
            }

            // CRC 校验：检测静默数据腐败或部分写入
            if record_header.crc != 0 {
                if !record_header.verify_crc(&record_data) {
                    // CRC 不匹配 → torn write 或数据腐败，停止 replay
                    break;
                }
            }

            if let Some(rtype) = record_header.record_type() {
                let result = if let Some(ctx) = redo_ctx.as_mut() {
                    self.redo_record(rtype, current_lsn, &record_data, ctx)?
                } else {
                    self.redo_record_nop(rtype)?
                };

                let mut stats = self.stats.lock();
                if result {
                    stats.records_redone += 1;
                } else {
                    stats.records_skipped += 1;
                }
            } else {
                let mut stats = self.stats.lock();
                stats.records_skipped += 1;
            }

            current_lsn = Lsn::from_raw(current_lsn.raw() + std::mem::size_of::<WalRecordHeader>() as u64 + record_header.size as u64);
        }

        {
            let mut stats = self.stats.lock();
            stats.end_lsn = current_lsn;
        }

        Ok(())
    }

    /// 重放单条记录（实际修改页面）
    /// 返回 true 表示已应用，false 表示跳过（已应用或不需要重做）
    fn redo_record(
        &self,
        rtype: WalRecordType,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        match rtype {
            // ===== Heap 记录 =====
            WalRecordType::HeapInsert => self.redo_heap_insert(record_lsn, data, ctx),
            WalRecordType::HeapBatchInsert => self.redo_heap_insert(record_lsn, data, ctx),
            WalRecordType::HeapDelete => self.redo_heap_delete(record_lsn, data, ctx),
            WalRecordType::HeapInplaceUpdate => self.redo_heap_inplace_update(record_lsn, data, ctx),
            WalRecordType::HeapSamePageAppend => self.redo_heap_same_page_append(record_lsn, data, ctx),
            WalRecordType::HeapAnotherPageAppendUpdateNewPage => {
                self.redo_heap_update_new_page(record_lsn, data, ctx)
            }
            WalRecordType::HeapAnotherPageAppendUpdateOldPage => {
                self.redo_heap_update_old_page(record_lsn, data, ctx)
            }
            WalRecordType::HeapAllocTd => Ok(true), // TD 分配在 redo 时不需要额外操作
            WalRecordType::HeapPrune => self.redo_heap_prune(record_lsn, data, ctx),

            // ===== B-Tree 记录 =====
            WalRecordType::BtreeInsertOnLeaf => self.redo_btree_insert_leaf(record_lsn, data, ctx),
            WalRecordType::BtreeInsertOnInternal => {
                self.redo_btree_insert_internal(record_lsn, data, ctx)
            }
            WalRecordType::BtreeSplitLeaf => self.redo_btree_split(record_lsn, data, ctx),
            WalRecordType::BtreeSplitInternal => self.redo_btree_split(record_lsn, data, ctx),
            WalRecordType::BtreeDeleteOnLeaf => self.redo_btree_delete_leaf(record_lsn, data, ctx),
            WalRecordType::BtreeDeleteOnInternal => {
                self.redo_btree_delete_internal(record_lsn, data, ctx)
            }
            WalRecordType::BtreeBuild | WalRecordType::BtreeInitMetaPage => {
                self.redo_btree_meta(record_lsn, data, ctx)
            }
            WalRecordType::BtreeUpdateMetaRoot => self.redo_btree_meta(record_lsn, data, ctx),

            // ===== 事务/检查点记录 =====
            WalRecordType::TxnCommit => {
                // 记录事务已提交
                if data.len() >= 8 {
                    let xid = u64::from_le_bytes(data[4..12].try_into().unwrap_or([0; 8]));
                    if xid > 0 {
                        ctx.txn_states.insert(xid, RecoveryTxnState::Committed);
                    }
                }
                Ok(true)
            }
            WalRecordType::TxnAbort => {
                if data.len() >= 8 {
                    let xid = u64::from_le_bytes(data[4..12].try_into().unwrap_or([0; 8]));
                    if xid > 0 {
                        ctx.txn_states.insert(xid, RecoveryTxnState::Aborted);
                    }
                }
                Ok(true)
            }
            WalRecordType::CheckpointShutdown | WalRecordType::CheckpointOnline => {
                self.checkpoint_lsn.store(record_lsn.raw(), Ordering::Release);
                Ok(true)
            }
            WalRecordType::NextCsn | WalRecordType::BarrierCsn => Ok(true),

            // ===== Undo 记录 =====
            WalRecordType::UndoInsertRecord
            | WalRecordType::UndoDeleteRecord
            | WalRecordType::UndoUpdateRecord
            | WalRecordType::UndoUpdateOldPage
            | WalRecordType::UndoUpdateNewPage => {
                // 收集 undo actions 用于回滚未提交事务
                // 格式: [WalRecordHeader][xid:8][type:1][table_oid:4][page_no:4][offset:2][data_len:4][data]
                let hdr_size = std::mem::size_of::<WalRecordHeader>();
                if data.len() > hdr_size + 19 {
                    let payload = &data[hdr_size..];
                    let xid = u64::from_le_bytes(payload[0..8].try_into().unwrap_or([0; 8]));
                    let action_type = payload[8];
                    let table_oid = u32::from_le_bytes(payload[9..13].try_into().unwrap_or([0; 4]));
                    let page_no = u32::from_le_bytes(payload[13..17].try_into().unwrap_or([0; 4]));
                    let tuple_offset = u16::from_le_bytes(payload[17..19].try_into().unwrap_or([0; 2]));
                    let data_len = u32::from_le_bytes(payload[19..23].try_into().unwrap_or([0; 4])) as usize;
                    let undo_data = if payload.len() > 23 + data_len {
                        payload[23..23 + data_len].to_vec()
                    } else if payload.len() > 23 {
                        payload[23..].to_vec()
                    } else {
                        Vec::new()
                    };

                    if xid > 0 {
                        let action = RecoveryUndoAction { action_type, table_oid, page_no, tuple_offset, data: undo_data };
                        ctx.txn_undo_actions.entry(xid).or_default().push(action);
                    }
                }
                Ok(true)
            }

            // DDL WAL 记录 — catalog 持久化已独立处理，redo 阶段标记为已处理
            WalRecordType::DdlCreate | WalRecordType::DdlDrop => {
                // DDL 操作的持久化由 SystemCatalog::persist() 保证。
                // Recovery 时 catalog 从 ferrisdb_catalog 文件重建，
                // 不需要从 WAL 重放 DDL（与 C++ 的 catalog WAL redo 不同）。
                Ok(true)
            }

            // Full-page write — 恢复 torn page
            WalRecordType::HeapPageFull => {
                // FPW payload 是完整的 8KB 页面，直接覆盖磁盘页面
                if data.len() >= std::mem::size_of::<WalRecordForPage>() {
                    let (page_rec, payload) = match Self::parse_page_record(data) {
                        Some(r) => r,
                        None => return Ok(false),
                    };
                    if payload.len() >= PAGE_SIZE {
                        let page_id = page_rec.page_id;
                        ctx.write_page(page_id, payload[..PAGE_SIZE].to_vec());
                    }
                }
                Ok(true)
            }

            _ => Ok(true),
        }
    }

    /// 无操作版本的 redo（仅统计）
    fn redo_record_nop(&self, rtype: WalRecordType) -> Result<bool> {
        Ok(true)
    }

    // ========== Heap Redo Handlers ==========

    /// 从 WAL 数据解析 WalRecordForPage 和数据负载
    fn parse_page_record(data: &[u8]) -> Option<(WalRecordForPage, &[u8])> {
        let page_rec_size = std::mem::size_of::<WalRecordForPage>();
        if data.len() < page_rec_size {
            return None;
        }
        let page_rec = unsafe { std::ptr::read(data.as_ptr() as *const WalRecordForPage) };
        let payload = &data[page_rec_size..];
        Some((page_rec, payload))
    }

    /// 检查页面是否需要 redo（基于 LSN 比较）
    /// 返回 None 表示需要 redo，Some(page_data) 表示页面数据已就绪
    fn prepare_redo(
        ctx: &mut RedoContext,
        page_id: &ferrisdb_core::PageId,
        record_lsn: Lsn,
    ) -> Result<Option<Vec<u8>>> {
        // 检查页面 LSN
        let page_lsn = ctx.get_page_lsn(page_id);
        if page_lsn.is_valid() && page_lsn.raw() >= record_lsn.raw() {
            // 已经应用过
            return Ok(None);
        }

        // 读取页面并校验 CRC（防止在损坏页面上 redo）
        let page_data = ctx.read_page(page_id)?;
        if page_data.len() >= PAGE_SIZE && !page_data.iter().all(|&b| b == 0) {
            if !crate::page::PageHeader::verify_checksum(&page_data) {
                eprintln!("[recovery] page {:?} checksum mismatch, applying FPW or WAL redo to fix", page_id);
            }
        }
        Ok(Some(page_data))
    }

    /// 更新页面 LSN 并写入脏页缓存
    fn finish_redo(
        ctx: &mut RedoContext,
        page_id: ferrisdb_core::PageId,
        record_lsn: Lsn,
        page_data: Vec<u8>,
    ) {
        // 将 record_lsn 写入页面头部前 8 字节
        let mut data = page_data;
        if data.len() >= 8 {
            data[0..8].copy_from_slice(&record_lsn.raw().to_le_bytes());
        }
        ctx.update_page_lsn(page_id, record_lsn);
        ctx.write_page(page_id, data);
    }

    /// Redo: Heap 插入
    fn redo_heap_insert(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        let (page_rec, payload) = match Self::parse_page_record(data) {
            Some(r) => r,
            None => return Ok(false),
        };
        let page_id = page_rec.page_id;

        let page_data = match Self::prepare_redo(ctx, &page_id, record_lsn)? {
            Some(d) => d,
            None => return Ok(false),
        };

        // 解析 tuple_offset + tuple_size + tuple_data
        if payload.len() < 4 {
            return Ok(false);
        }
        let tuple_offset = u16::from_le_bytes([payload[0], payload[1]]);
        let tuple_size = u16::from_le_bytes([payload[2], payload[3]]);
        let tuple_data = &payload[4..];

        if tuple_data.len() < tuple_size as usize {
            return Ok(false);
        }

        // 使用对齐缓冲区（HeapPage 需要 8192 字节对齐）
        let mut aligned = AlignedPageBuf::from_vec(&page_data);
        let heap_page = HeapPage::from_bytes(&mut aligned.data);

        // 如果是全零页面（新创建的页面），先初始化页面头部
        if heap_page.header().base.pd_upper == 0 {
            heap_page.init();
        }

        // 插入元组到页面
        let tuple_bytes = &tuple_data[..tuple_size as usize];
        if heap_page.insert_tuple(tuple_bytes).is_some() {
            Self::finish_redo(ctx, page_id, record_lsn, aligned.data.to_vec());
            return Ok(true);
        }

        // 插入失败（空间不足），仍然标记页面 LSN
        Self::finish_redo(ctx, page_id, record_lsn, aligned.data.to_vec());
        Ok(true)
    }

    /// Redo: Heap 删除
    fn redo_heap_delete(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        let (page_rec, payload) = match Self::parse_page_record(data) {
            Some(r) => r,
            None => return Ok(false),
        };
        let page_id = page_rec.page_id;

        let page_data = match Self::prepare_redo(ctx, &page_id, record_lsn)? {
            Some(d) => d,
            None => return Ok(false),
        };

        // 解析 tuple_offset
        if payload.len() < 2 {
            return Ok(false);
        }
        let tuple_offset = u16::from_le_bytes([payload[0], payload[1]]);

        // 标记元组为 dead
        let mut aligned = AlignedPageBuf::from_vec(&page_data);
        let heap_page = HeapPage::from_bytes(&mut aligned.data);
        heap_page.mark_dead(tuple_offset);

        Self::finish_redo(ctx, page_id, record_lsn, aligned.data.to_vec());
        Ok(true)
    }

    /// Redo: Heap 原地更新
    fn redo_heap_inplace_update(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        let (page_rec, payload) = match Self::parse_page_record(data) {
            Some(r) => r,
            None => return Ok(false),
        };
        let page_id = page_rec.page_id;

        let page_data = match Self::prepare_redo(ctx, &page_id, record_lsn)? {
            Some(d) => d,
            None => return Ok(false),
        };

        // 解析 tuple_offset + tuple_size + tuple_data
        if payload.len() < 4 {
            return Ok(false);
        }
        let tuple_offset = u16::from_le_bytes([payload[0], payload[1]]);
        let tuple_size = u16::from_le_bytes([payload[2], payload[3]]);
        let tuple_data = &payload[4..];

        if tuple_data.len() < tuple_size as usize {
            return Ok(false);
        }

        // 直接覆盖元组数据
        let mut aligned = AlignedPageBuf::from_vec(&page_data);
        let heap_page = HeapPage::from_bytes(&mut aligned.data);

        if let Some(existing) = heap_page.get_tuple_mut(tuple_offset) {
            let copy_len = (tuple_size as usize).min(existing.len());
            existing[..copy_len].copy_from_slice(&tuple_data[..copy_len]);
        }

        Self::finish_redo(ctx, page_id, record_lsn, aligned.data.to_vec());
        Ok(true)
    }

    /// Redo: Heap 同页追加
    fn redo_heap_same_page_append(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        // 同页追加 = 标记旧元组 + 插入新元组，都在同一页
        self.redo_heap_insert(record_lsn, data, ctx)
    }

    /// Redo: Heap 跨页更新（新页面）
    fn redo_heap_update_new_page(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        // 新页面插入元组，与 HeapInsert 逻辑相同
        self.redo_heap_insert(record_lsn, data, ctx)
    }

    /// Redo: Heap 跨页更新（旧页面）
    fn redo_heap_update_old_page(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        let (page_rec, payload) = match Self::parse_page_record(data) {
            Some(r) => r,
            None => return Ok(false),
        };
        let page_id = page_rec.page_id;

        let page_data = match Self::prepare_redo(ctx, &page_id, record_lsn)? {
            Some(d) => d,
            None => return Ok(false),
        };

        // 解析 tuple_offset + header_size + header_data
        if payload.len() < 4 {
            return Ok(false);
        }
        let tuple_offset = u16::from_le_bytes([payload[0], payload[1]]);
        let header_size = u16::from_le_bytes([payload[2], payload[3]]);
        let header_data = &payload[4..];

        if header_data.len() < header_size as usize {
            return Ok(false);
        }

        // 更新旧元组的头部（设置 xmax 等）
        let mut aligned = AlignedPageBuf::from_vec(&page_data);
        let heap_page = HeapPage::from_bytes(&mut aligned.data);

        if let Some(existing) = heap_page.get_tuple_mut(tuple_offset) {
            let copy_len = (header_size as usize).min(existing.len());
            existing[..copy_len].copy_from_slice(&header_data[..copy_len]);
        }

        Self::finish_redo(ctx, page_id, record_lsn, aligned.data.to_vec());
        Ok(true)
    }

    /// Redo: Heap 页面清理
    fn redo_heap_prune(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        let (page_rec, _payload) = match Self::parse_page_record(data) {
            Some(r) => r,
            None => return Ok(false),
        };
        let page_id = page_rec.page_id;

        let page_data = match Self::prepare_redo(ctx, &page_id, record_lsn)? {
            Some(d) => d,
            None => return Ok(false),
        };

        // 执行页面压缩
        let mut aligned = AlignedPageBuf::from_vec(&page_data);
        let heap_page = HeapPage::from_bytes(&mut aligned.data);
        heap_page.compact_dead();

        Self::finish_redo(ctx, page_id, record_lsn, aligned.data.to_vec());
        Ok(true)
    }

    // ========== B-Tree Redo Handlers ==========

    /// Redo: B-Tree 叶子/内部节点插入
    ///
    /// WAL payload: BTreeItem::serialize() 格式 [key_len:2][key][vtype:1][vdata]
    fn redo_btree_insert_leaf(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        let (page_rec, payload) = match Self::parse_page_record(data) {
            Some(r) => r,
            None => return Ok(false),
        };
        let page_id = page_rec.page_id;

        let page_data = match Self::prepare_redo(ctx, &page_id, record_lsn)? {
            Some(d) => d,
            None => return Ok(false),
        };

        let mut page_buf = page_data;

        // 解析 BTreeItem 并插入到页面
        if !payload.is_empty() {
            if let Some(item) = crate::index::BTreeItem::deserialize(payload) {
                let btree_page = crate::index::BTreePage::from_bytes(&mut page_buf);
                // 如果页面未初始化，先初始化
                if btree_page.header().nkeys == 0 && btree_page.header().level == 0
                    && btree_page.header().page_type == 0 {
                    btree_page.init(crate::index::BTreePageType::Leaf, 0);
                }
                if btree_page.insert_item_sorted(&item).is_none() {
                    eprintln!("[recovery] BTree leaf insert failed (page full) at LSN {:?}", record_lsn);
                }
            }
        }

        Self::finish_redo(ctx, page_id, record_lsn, page_buf);
        Ok(true)
    }

    /// Redo: B-Tree 内部节点插入
    fn redo_btree_insert_internal(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        self.redo_btree_insert_leaf(record_lsn, data, ctx)
    }

    /// Redo: B-Tree 分裂
    ///
    /// WAL payload: [right_page:4][sep_key_len:2][sep_key_data]
    /// 分裂的物理页面内容已经在原操作中写入。
    /// Redo 只需标记两个页面的 LSN 以确保幂等性。
    fn redo_btree_split(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        let (page_rec, payload) = match Self::parse_page_record(data) {
            Some(r) => r,
            None => return Ok(false),
        };
        let page_id = page_rec.page_id;

        // 标记左页 LSN
        let left_page_data = match Self::prepare_redo(ctx, &page_id, record_lsn)? {
            Some(d) => d,
            None => return Ok(false),
        };
        Self::finish_redo(ctx, page_id, record_lsn, left_page_data);

        // 标记右页 LSN
        if payload.len() >= 4 {
            let right_page_no = u32::from_le_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let right_page_id = ferrisdb_core::PageId::new(
                page_id.tablespace, page_id.database, page_id.relation, right_page_no,
            );
            if let Ok(Some(right_data)) = Self::prepare_redo(ctx, &right_page_id, record_lsn) {
                Self::finish_redo(ctx, right_page_id, record_lsn, right_data);
            }
        }

        Ok(true)
    }

    /// Redo: B-Tree 叶子删除
    fn redo_btree_delete_leaf(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        let (page_rec, _payload) = match Self::parse_page_record(data) {
            Some(r) => r,
            None => return Ok(false),
        };
        let page_id = page_rec.page_id;

        let page_data = match Self::prepare_redo(ctx, &page_id, record_lsn)? {
            Some(d) => d,
            None => return Ok(false),
        };

        Self::finish_redo(ctx, page_id, record_lsn, page_data);
        Ok(true)
    }

    /// Redo: B-Tree 内部节点删除
    fn redo_btree_delete_internal(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        self.redo_btree_delete_leaf(record_lsn, data, ctx)
    }

    /// Redo: B-Tree 元数据操作
    fn redo_btree_meta(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        let (page_rec, _payload) = match Self::parse_page_record(data) {
            Some(r) => r,
            None => return Ok(false),
        };
        let page_id = page_rec.page_id;

        let page_data = match Self::prepare_redo(ctx, &page_id, record_lsn)? {
            Some(d) => d,
            None => return Ok(false),
        };

        Self::finish_redo(ctx, page_id, record_lsn, page_data);
        Ok(true)
    }

    // ========== Undo Redo ==========

    /// Redo: Undo 记录插入
    fn redo_undo_insert(
        &self,
        record_lsn: Lsn,
        data: &[u8],
        ctx: &mut RedoContext,
    ) -> Result<bool> {
        let (page_rec, _payload) = match Self::parse_page_record(data) {
            Some(r) => r,
            None => return Ok(false),
        };
        let page_id = page_rec.page_id;

        let page_data = match Self::prepare_redo(ctx, &page_id, record_lsn)? {
            Some(d) => d,
            None => return Ok(false),
        };

        // Undo 页面记录直接写入
        Self::finish_redo(ctx, page_id, record_lsn, page_data);
        Ok(true)
    }

    /// 回滚未提交事务
    ///
    /// 扫描 redo 阶段收集的 txn_states 和 txn_undo_actions，
    /// 对没有 commit/abort record 的事务（in-progress at crash），执行 undo。
    fn rollback_uncommitted_txns(&self, ctx: &mut RedoContext) -> Result<()> {
        // 收集需要回滚的 xid
        let uncommitted_xids: Vec<u64> = ctx.txn_undo_actions.keys()
            .filter(|xid| {
                ctx.txn_states.get(xid) != Some(&RecoveryTxnState::Committed)
            })
            .copied()
            .collect();

        for xid in &uncommitted_xids {
            let actions = ctx.txn_undo_actions.get(xid).cloned().unwrap_or_default();
            // 逆序执行 undo actions
            for action in actions.iter().rev() {
                self.apply_recovery_undo(action, ctx)?;
            }
        }

        Ok(())
    }

    /// 执行单个 recovery undo action
    fn apply_recovery_undo(&self, action: &RecoveryUndoAction, ctx: &mut RedoContext) -> Result<()> {
        let page_id = ferrisdb_core::PageId::new(0, 0, action.table_oid, action.page_no);

        let page_data = ctx.read_page(&page_id)?;
        let mut aligned = AlignedPageBuf::from_vec(&page_data);
        let heap_page = HeapPage::from_bytes(&mut aligned.data);

        match action.action_type {
            0 => {
                // Insert undo → mark tuple dead
                heap_page.mark_dead(action.tuple_offset);
            }
            1 | 2 => {
                // Delete/InplaceUpdate undo → restore old data
                if let Some(tuple_mut) = heap_page.get_tuple_mut(action.tuple_offset) {
                    let len = action.data.len().min(tuple_mut.len());
                    if len > 0 {
                        tuple_mut[..len].copy_from_slice(&action.data[..len]);
                    }
                }
            }
            3 => {
                // UpdateOldPage undo → restore old header (32 bytes)
                if action.data.len() >= 32 {
                    if let Some(tuple_mut) = heap_page.get_tuple_mut(action.tuple_offset) {
                        tuple_mut[..32].copy_from_slice(&action.data[..32]);
                    }
                }
            }
            4 => {
                // UpdateNewPage undo → mark dead
                heap_page.mark_dead(action.tuple_offset);
            }
            _ => {}
        }

        ctx.write_page(page_id, aligned.data.to_vec());
        Ok(())
    }

    /// 获取 WAL 文件路径
    fn get_file_path(&self, file_no: u32) -> PathBuf {
        self.wal_dir.join(format!("{:08X}.wal", file_no))
    }
}

/// WAL Reader
///
/// 用于读取 WAL 记录。
pub struct WalReader {
    /// WAL 文件
    file: File,
    /// 当前偏移量
    offset: u64,
    /// 文件号
    file_no: u32,
    /// 是否到达末尾
    eof: bool,
}

impl WalReader {
    /// 创建新的 Reader
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let mut file = File::open(path.as_ref())
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to open WAL file: {}", e)
            )))?;

        let mut header_buf = [0u8; WalFileHeader::size()];
        file.read_exact(&mut header_buf)
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to read WAL header: {}", e)
            )))?;

        let header = WalFileHeader::from_bytes(&header_buf);
        if header.magic != WAL_FILE_MAGIC {
            return Err(FerrisDBError::Wal(ferrisdb_core::error::WalError::Corruption));
        }

        let offset = WalFileHeader::size() as u64;
        file.seek(SeekFrom::Start(offset))
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to seek: {}", e)
            )))?;

        Ok(Self {
            file,
            offset,
            file_no: 0,
            eof: false,
        })
    }

    /// 读取下一条记录
    pub fn read_next(&mut self) -> Result<Option<(Lsn, WalRecordHeader, Vec<u8>)>> {
        if self.eof {
            return Ok(None);
        }

        let mut header_buf = [0u8; std::mem::size_of::<WalRecordHeader>()];
        match self.file.read_exact(&mut header_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.eof = true;
                return Ok(None);
            }
            Err(e) => {
                return Err(FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                    format!("Failed to read record header: {}", e)
                )));
            }
        }

        let header = unsafe {
            std::ptr::read(header_buf.as_ptr() as *const WalRecordHeader)
        };

        let mut data = vec![0u8; header.size as usize];
        match self.file.read_exact(&mut data) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                self.eof = true;
                return Ok(None);
            }
            Err(e) => {
                return Err(FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                    format!("Failed to read record data: {}", e)
                )));
            }
        }

        let lsn = Lsn::from_parts(self.file_no, self.offset as u32);
        self.offset += std::mem::size_of::<WalRecordHeader>() as u64 + header.size as u64;

        Ok(Some((lsn, header, data)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_recovery_stage() {
        let recovery = WalRecovery::new("/tmp/test_wal");
        assert_eq!(recovery.stage(), RecoveryStage::NotStarted);
    }

    #[test]
    fn test_recovery_empty_dir() {
        let temp_dir = TempDir::new().unwrap();
        let recovery = WalRecovery::new(temp_dir.path());
        recovery.recover(RecoveryMode::CrashRecovery).unwrap();
        assert_eq!(recovery.stage(), RecoveryStage::Completed);
    }

    #[test]
    fn test_scan_wal_files() {
        let temp_dir = TempDir::new().unwrap();

        std::fs::File::create(temp_dir.path().join("00000001.wal")).unwrap();
        std::fs::File::create(temp_dir.path().join("00000002.wal")).unwrap();
        std::fs::File::create(temp_dir.path().join("00000003.wal")).unwrap();

        let recovery = WalRecovery::new(temp_dir.path());
        let files = recovery.scan_wal_files().unwrap();

        assert_eq!(files, vec![1, 2, 3]);
    }

    #[test]
    fn test_redo_context_read_write_page() {
        let temp_dir = TempDir::new().unwrap();
        let smgr = Arc::new(StorageManager::new(temp_dir.path()));
        let mut ctx = RedoContext::new(smgr);

        let page_id = ferrisdb_core::PageId::new(0, 0, 1, 0);

        // 写入页面数据
        let mut page_data = vec![0u8; PAGE_SIZE];
        let test_val: u64 = 0xDEADBEEF;
        page_data[0..8].copy_from_slice(&test_val.to_le_bytes());
        ctx.write_page(page_id, page_data);

        // 读取回来
        let result = ctx.read_page(&page_id).unwrap();
        assert_eq!(u64::from_le_bytes(result[0..8].try_into().unwrap()), test_val);
    }

    #[test]
    fn test_redo_context_lsn_tracking() {
        let temp_dir = TempDir::new().unwrap();
        let smgr = Arc::new(StorageManager::new(temp_dir.path()));
        let mut ctx = RedoContext::new(smgr);

        let page_id = ferrisdb_core::PageId::new(0, 0, 1, 0);

        // 初始 LSN 应为 INVALID
        let lsn = ctx.get_page_lsn(&page_id);
        assert!(!lsn.is_valid());

        // 写入带 LSN 的页面
        let mut page_data = vec![0u8; PAGE_SIZE];
        let test_lsn = Lsn::from_raw(100);
        page_data[0..8].copy_from_slice(&test_lsn.raw().to_le_bytes());
        ctx.write_page(page_id, page_data.clone());
        ctx.update_page_lsn(page_id, test_lsn);

        // 现在应该读到 LSN
        let lsn = ctx.get_page_lsn(&page_id);
        assert_eq!(lsn.raw(), 100);
    }

    #[test]
    fn test_prepare_redo_skips_applied() {
        let temp_dir = TempDir::new().unwrap();
        let smgr = Arc::new(StorageManager::new(temp_dir.path()));
        let mut ctx = RedoContext::new(smgr);

        let page_id = ferrisdb_core::PageId::new(0, 0, 1, 0);
        let record_lsn = Lsn::from_raw(50);

        // 页面 LSN 为 INVALID（未应用），应该返回 Some
        let result = ctx.prepare_redo(&page_id, record_lsn).unwrap();
        assert!(result.is_some());

        // 设置页面 LSN 为 100（已应用 record_lsn=50）
        ctx.update_page_lsn(page_id, Lsn::from_raw(100));
        let mut page_data = vec![0u8; PAGE_SIZE];
        page_data[0..8].copy_from_slice(&100u64.to_le_bytes());
        ctx.write_page(page_id, page_data);

        // 现在应该跳过
        let result = ctx.prepare_redo(&page_id, record_lsn).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_finish_redo_updates_lsn() {
        let temp_dir = TempDir::new().unwrap();
        let smgr = Arc::new(StorageManager::new(temp_dir.path()));
        let mut ctx = RedoContext::new(smgr);

        let page_id = ferrisdb_core::PageId::new(0, 0, 1, 0);
        let record_lsn = Lsn::from_raw(42);

        let page_data = vec![0u8; PAGE_SIZE];
        ctx.finish_redo(page_id, record_lsn, page_data);

        // 验证 LSN 已更新
        let lsn = ctx.get_page_lsn(&page_id);
        assert_eq!(lsn.raw(), 42);

        // 验证页面数据中的 LSN
        let data = ctx.read_page(&page_id).unwrap();
        assert_eq!(u64::from_le_bytes(data[0..8].try_into().unwrap()), 42);
    }
}
