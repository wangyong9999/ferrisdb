//! Transaction 管理
//!
//! 事务生命周期、状态管理和 MVCC 可见性判断。

use ferrisdb_core::{BufferTag, Csn, CsnAllocator, PdbId, Snapshot, Xid};
use ferrisdb_core::atomic::{AtomicU64, AtomicU8, Ordering};
use ferrisdb_core::error::{FerrisDBError, TransactionError};
use ferrisdb_storage::{BufferPool, HeapPage, WalWriter, WalTxnCommit, WalRecordType};
use std::sync::Arc;
use std::time::Instant;

/// 撤销动作（内存中的轻量表示）
#[derive(Debug, Clone)]
#[allow(missing_docs)]
pub enum UndoAction {
    /// 插入撤销：标记元组为 dead
    Insert {
        /// 表 OID
        table_oid: u32,
        /// 页号
        page_no: u32,
        /// 元组偏移
        tuple_offset: u16,
    },
    /// 删除撤销：恢复旧数据
    Delete {
        /// 表 OID
        table_oid: u32,
        /// 页号
        page_no: u32,
        /// 元组偏移
        tuple_offset: u16,
        /// 旧数据
        old_data: Vec<u8>,
    },
    /// 原地更新撤销：恢复旧数据
    InplaceUpdate {
        /// 表 OID
        table_oid: u32,
        /// 页号
        page_no: u32,
        /// 元组偏移
        tuple_offset: u16,
        /// 旧数据
        old_data: Vec<u8>,
    },
    /// 跨页更新（旧页）：恢复旧的 tuple header
    UpdateOldPage {
        /// 表 OID
        table_oid: u32,
        /// 页号
        page_no: u32,
        /// 元组偏移
        tuple_offset: u16,
        /// 旧的头部数据
        old_header: [u8; 32],
    },
    /// 跨页更新（新页）：标记新元组为 dead
    UpdateNewPage {
        /// 表 OID
        table_oid: u32,
        /// 页号
        page_no: u32,
        /// 元组偏移
        tuple_offset: u16,
    },
}

impl UndoAction {
    /// 序列化为 WAL 记录负载
    ///
    /// 格式: [xid:8][type:1][table_oid:4][page_no:4][offset:2][data_len:4][data...]
    pub fn serialize_for_wal(&self, xid: Xid) -> Vec<u8> {
        let (action_type, table_oid, page_no, offset, data) = match self {
            UndoAction::Insert { table_oid, page_no, tuple_offset } =>
                (0u8, *table_oid, *page_no, *tuple_offset, &[][..]),
            UndoAction::Delete { table_oid, page_no, tuple_offset, old_data } =>
                (1u8, *table_oid, *page_no, *tuple_offset, old_data.as_slice()),
            UndoAction::InplaceUpdate { table_oid, page_no, tuple_offset, old_data } =>
                (2u8, *table_oid, *page_no, *tuple_offset, old_data.as_slice()),
            UndoAction::UpdateOldPage { table_oid, page_no, tuple_offset, old_header } =>
                (3u8, *table_oid, *page_no, *tuple_offset, &old_header[..]),
            UndoAction::UpdateNewPage { table_oid, page_no, tuple_offset } =>
                (4u8, *table_oid, *page_no, *tuple_offset, &[][..]),
        };

        // WalRecordHeader + xid(8) + type(1) + table_oid(4) + page_no(4) + offset(2) + data_len(4) + data
        let payload_len = 8 + 1 + 4 + 4 + 2 + 4 + data.len();
        let wal_type = match action_type {
            0 => ferrisdb_storage::WalRecordType::UndoInsertRecord,
            1 => ferrisdb_storage::WalRecordType::UndoDeleteRecord,
            2 => ferrisdb_storage::WalRecordType::UndoUpdateRecord,
            3 => ferrisdb_storage::WalRecordType::UndoUpdateOldPage,
            4 => ferrisdb_storage::WalRecordType::UndoUpdateNewPage,
            _ => ferrisdb_storage::WalRecordType::UndoInsertRecord,
        };
        let header = ferrisdb_storage::wal::WalRecordHeader::new(
            wal_type,
            payload_len as u16,
        );
        let header_size = std::mem::size_of::<ferrisdb_storage::wal::WalRecordHeader>();
        let mut buf = Vec::with_capacity(header_size + payload_len);

        // Header
        buf.extend_from_slice(unsafe {
            std::slice::from_raw_parts(&header as *const _ as *const u8, header_size)
        });
        // Payload
        buf.extend_from_slice(&xid.raw().to_le_bytes());
        buf.push(action_type);
        buf.extend_from_slice(&table_oid.to_le_bytes());
        buf.extend_from_slice(&page_no.to_le_bytes());
        buf.extend_from_slice(&offset.to_le_bytes());
        buf.extend_from_slice(&(data.len() as u32).to_le_bytes());
        buf.extend_from_slice(data);
        buf
    }
}

/// 事务状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum TransactionState {
    /// 进行中
    InProgress = 0,
    /// 已提交
    Committed = 1,
    /// 已回滚
    Aborted = 2,
    /// 准备中（2PC）
    Prepared = 3,
}

impl Default for TransactionState {
    fn default() -> Self {
        Self::InProgress
    }
}

/// 事务信息
#[derive(Debug)]
pub struct TransactionInfo {
    /// 事务 ID
    pub xid: Xid,
    /// 状态
    pub state: AtomicU8,
    /// 提交 CSN
    pub csn: AtomicU64,
    /// 开始时间
    pub start_time: Instant,
    /// 开始快照
    pub snapshot: Snapshot,
}

impl TransactionInfo {
    /// 创建新的事务信息
    pub fn new(xid: Xid, snapshot: Snapshot) -> Self {
        Self {
            xid,
            state: AtomicU8::new(TransactionState::InProgress as u8),
            csn: AtomicU64::new(Csn::INVALID.raw()),
            start_time: Instant::now(),
            snapshot,
        }
    }

    /// 获取状态
    pub fn state(&self) -> TransactionState {
        match self.state.load(Ordering::Acquire) {
            0 => TransactionState::InProgress,
            1 => TransactionState::Committed,
            2 => TransactionState::Aborted,
            3 => TransactionState::Prepared,
            _ => TransactionState::InProgress,
        }
    }

    /// 获取 CSN
    pub fn csn(&self) -> Csn {
        Csn::from_raw(self.csn.load(Ordering::Acquire))
    }

    /// 设置状态
    pub fn set_state(&self, state: TransactionState) {
        self.state.store(state as u8, Ordering::Release);
    }

    /// 设置 CSN
    pub fn set_csn(&self, csn: Csn) {
        self.csn.store(csn.raw(), Ordering::Release);
    }
}

/// 事务
///
/// 表示一个数据库事务，提供 MVCC 可见性判断。
pub struct Transaction {
    /// 事务信息
    info: Arc<TransactionInfo>,
    /// 事务管理器引用（Arc 保证生命周期安全）
    manager: Arc<TransactionManager>,
    /// 是否活跃
    active: bool,
    /// 撤销日志
    undo_log: Vec<UndoAction>,
    /// Buffer Pool 引用（用于回滚）
    buffer_pool: Option<Arc<BufferPool>>,
    /// WAL Writer 引用（用于写 commit/abort 记录）
    wal_writer: Option<Arc<WalWriter>>,
    /// 事务超时（毫秒，0=无限制）
    timeout_ms: u64,
}

impl Transaction {
    /// 创建新事务
    pub(crate) fn new(info: Arc<TransactionInfo>, manager: Arc<TransactionManager>) -> Self {
        Self {
            info,
            manager,
            active: true,
            undo_log: Vec::new(),
            buffer_pool: None,
            wal_writer: None,
            timeout_ms: 0,
        }
    }

    /// 创建带 Buffer Pool 的事务
    pub(crate) fn with_buffer_pool(
        info: Arc<TransactionInfo>,
        manager: Arc<TransactionManager>,
        buffer_pool: Arc<BufferPool>,
    ) -> Self {
        let wal_writer = manager.wal_writer.clone();
        let timeout_ms = manager.txn_timeout_ms;
        Self {
            info,
            manager,
            active: true,
            undo_log: Vec::new(),
            buffer_pool: Some(buffer_pool),
            wal_writer,
            timeout_ms,
        }
    }

    /// 获取事务 ID
    #[inline]
    pub fn xid(&self) -> Xid {
        self.info.xid
    }

    /// 获取快照
    #[inline]
    pub fn snapshot(&self) -> &Snapshot {
        &self.info.snapshot
    }

    /// 获取 CSN
    #[inline]
    pub fn csn(&self) -> Csn {
        self.info.csn()
    }

    /// 获取状态
    #[inline]
    pub fn state(&self) -> TransactionState {
        self.info.state()
    }

    /// 检查是否活跃
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active && self.state() == TransactionState::InProgress
    }

    /// 检查是否已超时
    pub fn is_timed_out(&self) -> bool {
        if self.timeout_ms == 0 {
            return false;
        }
        self.info.start_time.elapsed().as_millis() as u64 > self.timeout_ms
    }

    /// 检查事务是否超时，超时则自动 abort 并返回错误
    pub fn check_timeout(&mut self) -> ferrisdb_core::Result<()> {
        if self.is_timed_out() {
            let _ = self.abort();
            return Err(FerrisDBError::Transaction(TransactionError::Timeout));
        }
        Ok(())
    }

    /// 提交事务
    pub fn commit(&mut self) -> ferrisdb_core::Result<()> {
        if !self.active {
            return Err(ferrisdb_core::FerrisDBError::InvalidState(
                "Transaction is not active".to_string(),
            ));
        }
        self.check_timeout()?;

        // 分配 CSN
        let manager = &self.manager;
        let csn = manager.allocate_csn();

        // 正确的 commit 顺序（WAL-first）：
        // 1. 先写 WAL commit record（确保持久化）
        // 2. 标记 active=false（防止 Drop 触发 abort）
        // 3. 设 CSN 和状态（使其对其他事务可见）
        // Group commit: 写 WAL commit record 到 OS page cache，不等 fsync
        // 后台 flusher 线程 5ms 批量 fsync（多 commit 共享一次 fsync）
        if let Some(ref writer) = self.wal_writer {
            let commit_rec = WalTxnCommit::new(self.info.xid, csn.raw());
            writer.write(&commit_rec.to_bytes())?;
            writer.notify_sync();
        }

        // WAL 已持久化，先禁用 Drop 的 auto-abort（即使后续 panic 也不会回滚已提交事务）
        self.active = false;

        // 设置状态使其对其他事务可见
        self.info.set_csn(csn);
        self.info.set_state(TransactionState::Committed);

        // 写入无锁 CSN 查找表
        let slot_idx = self.info.xid.slot_id() as usize;
        manager.slot_csns[slot_idx].store(csn.raw(), Ordering::Release);

        // 标记槽位为可重用
        let xid = self.info.xid;
        self.manager.release_slot(xid);

        Ok(())
    }

    /// 回滚事务
    pub fn abort(&mut self) -> ferrisdb_core::Result<()> {
        if !self.active {
            return Err(ferrisdb_core::FerrisDBError::InvalidState(
                "Transaction is not active".to_string(),
            ));
        }

        self.info.set_state(TransactionState::Aborted);

        // 写 WAL abort record
        if let Some(ref writer) = self.wal_writer {
            let abort_rec = WalTxnCommit::new_abort(self.info.xid);
            let _ = writer.write(&abort_rec.to_bytes());
        }

        self.active = false;

        // 应用撤销日志（逆序）
        if let Some(ref bp) = self.buffer_pool {
            self.apply_undo(bp);
        }

        // 释放槽位
        let xid = self.info.xid;
        self.manager.release_slot(xid);

        Ok(())
    }

    /// 记录撤销动作
    ///
    /// 同时写入内存 undo_log 和 WAL（如果启用了 WalWriter）。
    /// 错误传播：Undo WAL 写入失败或 undo_log 过大都返回错误。
    pub fn push_undo(&mut self, action: UndoAction) -> ferrisdb_core::Result<()> {
        // 大小限制：防止长事务 OOM（默认 1M 条 undo action）
        const MAX_UNDO_ACTIONS: usize = 1_000_000;
        if self.undo_log.len() >= MAX_UNDO_ACTIONS {
            return Err(FerrisDBError::Transaction(TransactionError::InvalidState(
                format!("Transaction too large: undo_log exceeds {} actions", MAX_UNDO_ACTIONS)
            )));
        }
        if let Some(ref writer) = self.wal_writer {
            let wal_data = action.serialize_for_wal(self.info.xid);
            writer.write(&wal_data)?;
        }
        self.undo_log.push(action);
        Ok(())
    }

    /// 创建 Savepoint，返回 savepoint ID（即当前 undo_log 的位置）
    pub fn savepoint(&self) -> usize {
        self.undo_log.len()
    }

    /// 回滚到 Savepoint：撤销从 savepoint 位置之后的所有操作
    pub fn rollback_to_savepoint(&mut self, savepoint_id: usize) {
        if savepoint_id >= self.undo_log.len() {
            return; // 无需回滚
        }

        if let Some(ref bp) = self.buffer_pool {
            // 逆序应用从 savepoint_id 到末尾的 undo actions
            for action in self.undo_log[savepoint_id..].iter().rev() {
                Self::apply_single_undo(action, bp);
            }
        }

        // 截断 undo log 到 savepoint 位置
        self.undo_log.truncate(savepoint_id);
    }

    /// 应用撤销日志（逆序回放）
    fn apply_undo(&self, buffer_pool: &BufferPool) {
        for action in self.undo_log.iter().rev() {
            Self::apply_single_undo(action, buffer_pool);
        }
    }

    /// 应用单个撤销动作
    fn apply_single_undo(action: &UndoAction, buffer_pool: &BufferPool) {
        match action {
            UndoAction::Insert { table_oid, page_no, tuple_offset } => {
                let tag = BufferTag::new(PdbId::new(0), *table_oid as u16, *page_no);
                if let Ok(pinned) = buffer_pool.pin(&tag) {
                    let page_ptr = pinned.page_data();
                    let _lock = pinned.lock_exclusive();
                    let page = unsafe {
                        HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, ferrisdb_core::PAGE_SIZE))
                    };
                    page.mark_dead(*tuple_offset);
                    drop(_lock);
                    pinned.mark_dirty();
                }
            }
            UndoAction::Delete { table_oid, page_no, tuple_offset, old_data } => {
                let tag = BufferTag::new(PdbId::new(0), *table_oid as u16, *page_no);
                if let Ok(pinned) = buffer_pool.pin(&tag) {
                    let page_ptr = pinned.page_data();
                    let _lock = pinned.lock_exclusive();
                    let page = unsafe {
                        HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, ferrisdb_core::PAGE_SIZE))
                    };
                    if let Some(tuple_mut) = page.get_tuple_mut(*tuple_offset) {
                        let len = old_data.len().min(tuple_mut.len());
                        tuple_mut[..len].copy_from_slice(&old_data[..len]);
                    }
                    drop(_lock);
                    pinned.mark_dirty();
                }
            }
            UndoAction::InplaceUpdate { table_oid, page_no, tuple_offset, old_data } => {
                let tag = BufferTag::new(PdbId::new(0), *table_oid as u16, *page_no);
                if let Ok(pinned) = buffer_pool.pin(&tag) {
                    let page_ptr = pinned.page_data();
                    let _lock = pinned.lock_exclusive();
                    let page = unsafe {
                        HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, ferrisdb_core::PAGE_SIZE))
                    };
                    if let Some(tuple_mut) = page.get_tuple_mut(*tuple_offset) {
                        let len = old_data.len().min(tuple_mut.len());
                        tuple_mut[..len].copy_from_slice(&old_data[..len]);
                    }
                    drop(_lock);
                    pinned.mark_dirty();
                }
            }
            UndoAction::UpdateOldPage { table_oid, page_no, tuple_offset, old_header } => {
                let tag = BufferTag::new(PdbId::new(0), *table_oid as u16, *page_no);
                if let Ok(pinned) = buffer_pool.pin(&tag) {
                    let page_ptr = pinned.page_data();
                    let _lock = pinned.lock_exclusive();
                    let page = unsafe {
                        HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, ferrisdb_core::PAGE_SIZE))
                    };
                    if let Some(tuple_mut) = page.get_tuple_mut(*tuple_offset) {
                        tuple_mut[..32].copy_from_slice(old_header);
                    }
                    drop(_lock);
                    pinned.mark_dirty();
                }
            }
            UndoAction::UpdateNewPage { table_oid, page_no, tuple_offset } => {
                let tag = BufferTag::new(PdbId::new(0), *table_oid as u16, *page_no);
                if let Ok(pinned) = buffer_pool.pin(&tag) {
                    let page_ptr = pinned.page_data();
                    let _lock = pinned.lock_exclusive();
                    let page = unsafe {
                        HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, ferrisdb_core::PAGE_SIZE))
                    };
                    page.mark_dead(*tuple_offset);
                    drop(_lock);
                    pinned.mark_dirty();
                }
            }
        }
    }

    /// MVCC 可见性判断
    ///
    /// 判断由 (xmin, xmax, cmin, cmax) 标识的元组对当前事务是否可见。
    pub fn is_visible(
        &self,
        xmin: Xid,
        xmax: Xid,
        cmin: u32,
        cmax: u32,
    ) -> bool {
        // 1. 插入者是自己且未被删除 -> 可见
        if xmin == self.info.xid {
            // 自己插入的元组
            if xmax.is_valid() {
                // 已被更新/删除
                if xmax == self.info.xid {
                    // 自己删除的，不可见（除非是同一命令）
                    return cmin == cmax;
                }
                // 被其他事务删除
                return false;
            }
            // 未被删除，可见
            return true;
        }

        // 2. 插入者是其他事务
        // 检查插入事务是否已提交且 CSN 在快照之前
        let xmin_csn = self.get_transaction_csn(xmin);
        if xmin_csn.is_invalid() {
            // 插入事务未提交，不可见
            return false;
        }

        // 检查快照是否包含该事务
        if self.info.snapshot.is_active(xmin) {
            // 插入事务在快照中（当时未提交），不可见
            return false;
        }

        // 3. 检查删除事务
        if xmax.is_invalid() {
            // 未被删除，可见
            return true;
        }

        // 被删除
        if xmax == self.info.xid {
            // 自己删除的，不可见
            return false;
        }

        // 检查删除事务的 CSN
        let xmax_csn = self.get_transaction_csn(xmax);
        if xmax_csn.is_invalid() {
            // 删除事务未提交，可见
            return true;
        }

        // 检查删除事务是否在快照中
        if self.info.snapshot.is_active(xmax) {
            // 删除事务在快照中（当时未提交），可见
            return true;
        }

        // 删除事务已提交且不在快照中，不可见
        false
    }

    /// 获取事务的 CSN
    fn get_transaction_csn(&self, xid: Xid) -> Csn {
        // 简化实现：从管理器获取
        let manager = &self.manager;
        manager.get_csn(xid)
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if self.active {
            // 自动回滚未提交的事务
            let _ = self.abort();
        }
    }
}

/// 事务管理器
///
/// 管理所有事务的生命周期。
pub struct TransactionManager {
    /// CSN 分配器
    csn_allocator: CsnAllocator,
    /// 事务槽位
    slots: parking_lot::RwLock<Vec<Option<Arc<TransactionInfo>>>>,
    /// 是否正在关闭（拒绝新事务）
    is_shutdown: std::sync::atomic::AtomicBool,
    /// CSN 快速查找表（无锁）
    /// slot_csns[i] 存储事务 i 的 CSN，INVALID 表示未提交/未分配
    slot_csns: Vec<AtomicU64>,
    /// 最大槽位数
    max_slots: usize,
    /// Buffer Pool 引用（用于事务回滚）
    buffer_pool: Option<Arc<BufferPool>>,
    /// WAL Writer 引用（用于写 commit/abort 记录）
    wal_writer: Option<Arc<WalWriter>>,
    /// 事务超时（毫秒，0=无限制）
    txn_timeout_ms: u64,
}

impl TransactionManager {
    /// 创建新的事务管理器
    pub fn new(max_slots: usize) -> Self {
        let mut slots = Vec::with_capacity(max_slots);
        let mut slot_csns = Vec::with_capacity(max_slots);
        for _ in 0..max_slots {
            slots.push(None);
            slot_csns.push(AtomicU64::new(Csn::INVALID.raw()));
        }

        Self {
            csn_allocator: CsnAllocator::new(),
            slots: parking_lot::RwLock::new(slots),
            is_shutdown: std::sync::atomic::AtomicBool::new(false),
            slot_csns,
            max_slots,
            buffer_pool: None,
            wal_writer: None,
            txn_timeout_ms: 30_000, // 默认 30 秒
        }
    }

    /// 设置事务超时（毫秒，0=无限制）
    pub fn set_txn_timeout(&mut self, timeout_ms: u64) {
        self.txn_timeout_ms = timeout_ms;
    }

    /// 设置 Buffer Pool（用于事务回滚时访问页面）
    pub fn set_buffer_pool(&mut self, bp: Arc<BufferPool>) {
        self.buffer_pool = Some(bp);
    }

    /// 设置 WAL Writer（用于写 commit/abort 记录到磁盘）
    pub fn set_wal_writer(&mut self, writer: Arc<WalWriter>) {
        self.wal_writer = Some(writer);
    }

    /// 开始新事务（关闭中时拒绝）
    /// 接受 &Arc<Self> 以便 Transaction 安全持有 Arc 引用（无裸指针）
    pub fn begin(self: &Arc<Self>) -> ferrisdb_core::Result<Transaction> {
        if self.is_shutdown.load(std::sync::atomic::Ordering::Acquire) {
            return Err(FerrisDBError::InvalidState("Engine is shutting down".to_string()));
        }
        // 合并快照收集和槽位分配到一次写锁中
        let info = {
            let mut slots = self.slots.write();

            // 收集活跃事务（在写锁下，状态一致）
            let mut active_xids = Vec::new();
            let mut slot_idx = None;
            for (i, slot) in slots.iter().enumerate() {
                if i == 0 {
                    continue;
                }
                if let Some(ref info) = slot {
                    if info.state() == TransactionState::InProgress {
                        active_xids.push(info.xid);
                    }
                } else if slot_idx.is_none() {
                    slot_idx = Some(i);
                }
            }
            let idx = slot_idx.ok_or(ferrisdb_core::error::TransactionError::NoFreeSlot)?;

            let snapshot_csn = self.csn_allocator.current();
            let snapshot = Snapshot::new(snapshot_csn, active_xids);

            let xid = Xid::new(0, idx as u32);
            let info = Arc::new(TransactionInfo::new(xid, snapshot));
            slots[idx] = Some(Arc::clone(&info));
            info
        };

        if let Some(ref bp) = self.buffer_pool {
            Ok(Transaction::with_buffer_pool(info, Arc::clone(self), Arc::clone(bp)))
        } else {
            Ok(Transaction::new(info, Arc::clone(self)))
        }
    }

    /// 分配 CSN
    pub fn allocate_csn(&self) -> Csn {
        self.csn_allocator.allocate()
    }

    /// 获取事务的 CSN（无锁）
    pub fn get_csn(&self, xid: Xid) -> Csn {
        let slot_idx = xid.slot_id() as usize;
        if slot_idx < self.slot_csns.len() {
            let raw = self.slot_csns[slot_idx].load(Ordering::Acquire);
            return Csn::from_raw(raw);
        }
        Csn::INVALID
    }

    /// 收集当前快照（公开版本，供 prune 等操作使用）
    pub fn current_snapshot(&self) -> Snapshot {
        self.collect_snapshot()
    }

    /// 收集当前快照
    fn collect_snapshot(&self) -> Snapshot {
        let slots = self.slots.read();
        let mut active_xids = Vec::new();

        for slot in slots.iter() {
            if let Some(ref info) = slot {
                if info.state() == TransactionState::InProgress {
                    active_xids.push(info.xid);
                }
            }
        }

        let snapshot_csn = self.csn_allocator.current();
        Snapshot::new(snapshot_csn, active_xids)
    }

    /// 分配槽位
    fn allocate_slot(&self) -> ferrisdb_core::Result<Xid> {
        let slots = self.slots.read();
        for (i, slot) in slots.iter().enumerate() {
            if i == 0 {
                // 跳过槽位 0，因为 Xid(0) == INVALID
                continue;
            }
            if slot.is_none() {
                return Ok(Xid::new(0, i as u32));
            }
        }
        Err(FerrisDBError::Transaction(TransactionError::NoFreeSlot))
    }

    /// 启动关闭序列：拒绝新事务
    pub fn initiate_shutdown(&self) {
        self.is_shutdown.store(true, std::sync::atomic::Ordering::Release);
    }

    /// 等待所有活跃事务完成（最多 timeout_ms 毫秒）
    /// 返回剩余活跃事务数（0 = 全部完成）
    pub fn wait_for_all_transactions(&self, timeout_ms: u64) -> usize {
        let start = std::time::Instant::now();
        loop {
            let slots = self.slots.read();
            let active = slots.iter().filter(|s| {
                s.as_ref().map_or(false, |info| info.state() == TransactionState::InProgress)
            }).count();
            if active == 0 {
                return 0;
            }
            if start.elapsed().as_millis() as u64 >= timeout_ms {
                return active;
            }
            drop(slots);
            std::thread::sleep(std::time::Duration::from_millis(50));
        }
    }

    /// 扫描并标记超时事务为 Aborted（后台线程调用）
    /// 返回被强制 abort 的事务数
    pub fn abort_timed_out_transactions(&self) -> usize {
        if self.txn_timeout_ms == 0 { return 0; }
        let slots = self.slots.read();
        let mut aborted = 0;
        for slot in slots.iter() {
            if let Some(ref info) = slot {
                if info.state() == TransactionState::InProgress
                    && info.start_time.elapsed().as_millis() as u64 > self.txn_timeout_ms
                {
                    // 标记为 Aborted（Transaction::Drop 会跳过已非 active 的事务）
                    info.set_state(TransactionState::Aborted);
                    aborted += 1;
                }
            }
        }
        aborted
    }

    /// 获取当前活跃事务数
    pub fn active_transaction_count(&self) -> usize {
        let slots = self.slots.read();
        slots.iter().filter(|s| {
            s.as_ref().map_or(false, |info| info.state() == TransactionState::InProgress)
        }).count()
    }

    /// 释放槽位
    fn release_slot(&self, xid: Xid) {
        let slot_idx = xid.slot_id() as usize;
        // 清除无锁 CSN 表
        self.slot_csns[slot_idx].store(Csn::INVALID.raw(), Ordering::Release);
        // 清除槽位
        let mut slots = self.slots.write();
        if slot_idx < slots.len() {
            slots[slot_idx] = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_manager_begin() {
        let mgr = Arc::new(TransactionManager::new(100));
        let tx = mgr.begin().unwrap();
        assert!(tx.is_active());
        assert_eq!(tx.state(), TransactionState::InProgress);
    }

    #[test]
    fn test_transaction_commit() {
        let mgr = Arc::new(TransactionManager::new(100));
        let mut tx = mgr.begin().unwrap();
        tx.commit().unwrap();
        assert!(!tx.is_active());
        assert_eq!(tx.state(), TransactionState::Committed);
        assert!(tx.csn().is_valid());
    }

    #[test]
    fn test_transaction_abort() {
        let mgr = Arc::new(TransactionManager::new(100));
        let mut tx = mgr.begin().unwrap();
        tx.abort().unwrap();
        assert!(!tx.is_active());
        assert_eq!(tx.state(), TransactionState::Aborted);
    }

    #[test]
    fn test_transaction_visibility_own_tuple() {
        let mgr = Arc::new(TransactionManager::new(100));
        let tx = mgr.begin().unwrap();

        // 自己插入的元组，未删除
        assert!(tx.is_visible(tx.xid(), Xid::INVALID, 0, 0));

        // 自己插入的元组，自己删除
        assert!(!tx.is_visible(tx.xid(), tx.xid(), 0, 1));
    }
}
