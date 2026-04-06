//! Heap Table
//!
//! 表的堆存储实现。

use ferrisdb_core::{BufferTag, FerrisDBError, PdbId, Snapshot, Xid};
use ferrisdb_storage::{BufferPool, HeapPage, ItemPointerData, ItemIdFlags};
use ferrisdb_storage::wal::{WalBuffer, WalWriter, WalHeapInsert, WalHeapDelete, WalHeapInplaceUpdate, WalHeapUpdateNewPage, WalHeapUpdateOldPage};
use std::sync::Arc;

use crate::transaction::{Transaction, TransactionManager, UndoAction};

/// 元组 ID
pub type TupleId = ItemPointerData;

/// 元组头部信息
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct TupleHeader {
    /// 插入事务 ID
    pub xmin: Xid,
    /// 删除事务 ID
    pub xmax: Xid,
    /// 插入命令 ID
    pub cmin: u32,
    /// 删除命令 ID
    pub cmax: u32,
    /// 元组指针（指向新版本）
    pub ctid: TupleId,
    /// 信息标志
    pub infomask: u16,
}

impl TupleHeader {
    /// 创建新的元组头部
    pub fn new(xmin: Xid, cmin: u32) -> Self {
        Self {
            xmin,
            xmax: Xid::INVALID,
            cmin,
            cmax: 0,
            ctid: TupleId::INVALID,
            infomask: 0,
        }
    }

    /// 序列化大小
    pub const fn serialized_size() -> usize {
        8 + 8 + 4 + 4 + 6 + 2 // 32 bytes
    }

    /// 序列化
    pub fn serialize(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..8].copy_from_slice(&self.xmin.raw().to_le_bytes());
        buf[8..16].copy_from_slice(&self.xmax.raw().to_le_bytes());
        buf[16..20].copy_from_slice(&self.cmin.to_le_bytes());
        buf[20..24].copy_from_slice(&self.cmax.to_le_bytes());
        // ctid (6 bytes)
        buf[24..28].copy_from_slice(&self.ctid.ip_blkid.to_le_bytes());
        buf[28..30].copy_from_slice(&self.ctid.ip_posid.to_le_bytes());
        buf[30..32].copy_from_slice(&self.infomask.to_le_bytes());
        buf
    }

    /// 从字节数组反序列化
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 32 {
            return None;
        }

        let xmin_raw = u64::from_le_bytes(data[0..8].try_into().ok()?);
        let xmax_raw = u64::from_le_bytes(data[8..16].try_into().ok()?);
        let cmin = u32::from_le_bytes(data[16..20].try_into().ok()?);
        let cmax = u32::from_le_bytes(data[20..24].try_into().ok()?);
        let ip_blkid = u32::from_le_bytes(data[24..28].try_into().ok()?);
        let ip_posid = u16::from_le_bytes(data[28..30].try_into().ok()?);
        let infomask = u16::from_le_bytes(data[30..32].try_into().ok()?);

        Some(Self {
            xmin: Xid::from_raw(xmin_raw),
            xmax: Xid::from_raw(xmax_raw),
            cmin,
            cmax,
            ctid: TupleId::new(ip_blkid, ip_posid),
            infomask,
        })
    }
}

/// Heap Table
pub struct HeapTable {
    /// 表 OID
    table_oid: u32,
    /// Buffer Pool
    buffer_pool: Arc<BufferPool>,
    /// Transaction Manager
    txn_mgr: Arc<TransactionManager>,
    /// WAL Buffer (optional, 内存 staging)
    wal_buffer: Option<Arc<WalBuffer>>,
    /// WAL Writer (optional, 磁盘持久化)
    wal_writer: Option<Arc<WalWriter>>,
    /// 当前页面数量
    current_page: std::sync::atomic::AtomicU32,
    /// Free Space Map: 每页 1 byte 空闲等级 (0=满, 255=空)
    /// 使用 AtomicU8 数组避免 RwLock 争用
    fsm: Vec<std::sync::atomic::AtomicU8>,
    /// FSM 最大已分配页数
    fsm_max_page: std::sync::atomic::AtomicU32,
}

impl HeapTable {
    /// 创建新的 Heap Table（无 WAL）
    pub fn new(table_oid: u32, buffer_pool: Arc<BufferPool>, txn_mgr: Arc<TransactionManager>) -> Self {
        Self {
            table_oid,
            buffer_pool,
            txn_mgr,
            wal_buffer: None,
            wal_writer: None,
            current_page: std::sync::atomic::AtomicU32::new(0),
            fsm: (0..65536).map(|_| std::sync::atomic::AtomicU8::new(255)).collect(),
            fsm_max_page: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// 创建带内存 WAL Buffer 的 Heap Table
    pub fn with_wal(
        table_oid: u32,
        buffer_pool: Arc<BufferPool>,
        txn_mgr: Arc<TransactionManager>,
        wal_buffer: Arc<WalBuffer>,
    ) -> Self {
        Self {
            table_oid,
            buffer_pool,
            txn_mgr,
            wal_buffer: Some(wal_buffer),
            wal_writer: None,
            current_page: std::sync::atomic::AtomicU32::new(0),
            fsm: (0..65536).map(|_| std::sync::atomic::AtomicU8::new(255)).collect(),
            fsm_max_page: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// 创建带磁盘 WAL Writer 的 Heap Table（生产模式）
    pub fn with_wal_writer(
        table_oid: u32,
        buffer_pool: Arc<BufferPool>,
        txn_mgr: Arc<TransactionManager>,
        wal_writer: Arc<WalWriter>,
    ) -> Self {
        Self {
            table_oid,
            buffer_pool,
            txn_mgr,
            wal_buffer: None,
            wal_writer: Some(wal_writer),
            current_page: std::sync::atomic::AtomicU32::new(0),
            fsm: (0..65536).map(|_| std::sync::atomic::AtomicU8::new(255)).collect(),
            fsm_max_page: std::sync::atomic::AtomicU32::new(0),
        }
    }

    /// 设置 WAL Writer（可在创建后追加）
    pub fn set_wal_writer(&mut self, writer: Arc<WalWriter>) {
        self.wal_writer = Some(writer);
    }

    /// 设置当前页面数（从 catalog 恢复时使用）
    pub fn set_current_page(&self, page_count: u32) {
        self.current_page.store(page_count, std::sync::atomic::Ordering::Release);
    }

    /// 获取当前页面数（用于 catalog 持久化）
    pub fn get_current_page(&self) -> u32 {
        self.current_page.load(std::sync::atomic::Ordering::Acquire)
    }

    /// 持久化 FSM 到文件
    pub fn save_fsm<P: AsRef<std::path::Path>>(&self, path: P) -> ferrisdb_core::Result<()> {
        let max = self.fsm_max_page.load(std::sync::atomic::Ordering::Acquire) as usize;
        let max = max.min(self.fsm.len());
        let mut buf = Vec::with_capacity(4 + max);
        buf.extend_from_slice(&(max as u32).to_le_bytes());
        for i in 0..max {
            buf.push(self.fsm[i].load(std::sync::atomic::Ordering::Relaxed));
        }
        std::fs::write(path.as_ref(), &buf)
            .map_err(|e| ferrisdb_core::FerrisDBError::Internal(format!("Failed to save FSM: {}", e)))?;
        Ok(())
    }

    /// 从文件加载 FSM
    pub fn load_fsm<P: AsRef<std::path::Path>>(&self, path: P) -> ferrisdb_core::Result<()> {
        let data = match std::fs::read(path.as_ref()) {
            Ok(d) => d,
            Err(_) => return Ok(()), // 文件不存在 = 空 FSM
        };
        if data.len() < 4 { return Ok(()); }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0;4])) as usize;
        let count = count.min(self.fsm.len()).min(data.len() - 4);
        for i in 0..count {
            self.fsm[i].store(data[4 + i], std::sync::atomic::Ordering::Relaxed);
        }
        self.fsm_max_page.store(count as u32, std::sync::atomic::Ordering::Release);
        Ok(())
    }

    /// FSM: 查找有足够空闲空间的页（无锁，roving pointer 避免线性扫描）
    fn fsm_find_page(&self, needed: u16) -> Option<u32> {
        let threshold = if needed >= 8192 { 0 } else { 255 - (needed as u32 * 255 / 8192) as u8 };
        let max = self.fsm_max_page.load(std::sync::atomic::Ordering::Acquire) as usize;
        if max == 0 { return None; }
        let len = max.min(self.fsm.len());
        // 从上次成功位置附近开始搜索（last_page - 1），最多搜索 8 页
        let current = self.current_page.load(std::sync::atomic::Ordering::Acquire) as usize;
        let start = if current > 0 { current - 1 } else { 0 };
        let search_limit = 8.min(len);
        for offset in 0..search_limit {
            let i = (start + offset) % len;
            if self.fsm[i].load(std::sync::atomic::Ordering::Relaxed) >= threshold {
                return Some(i as u32);
            }
        }
        None // 8 页内没找到就放弃，走 allocate new page 路径
    }

    /// FSM: 更新页面空闲空间等级（无锁）
    fn fsm_update(&self, page_no: u32, free_space: u16) {
        let level = if free_space >= 8192 { 255 } else { (free_space as u32 * 255 / 8192) as u8 };
        let idx = page_no as usize;
        if idx < self.fsm.len() {
            self.fsm[idx].store(level, std::sync::atomic::Ordering::Relaxed);
            // 更新 max_page
            let mut current_max = self.fsm_max_page.load(std::sync::atomic::Ordering::Acquire);
            while page_no >= current_max {
                match self.fsm_max_page.compare_exchange(current_max, page_no + 1, std::sync::atomic::Ordering::AcqRel, std::sync::atomic::Ordering::Acquire) {
                    Ok(_) => break,
                    Err(v) => current_max = v,
                }
            }
        }
    }

    /// 创建 BufferTag
    fn make_tag(&self, page_no: u32) -> BufferTag {
        BufferTag::new(PdbId::new(0), self.table_oid as u16, page_no)
    }

    /// 创建 PageId (用于 WAL 记录)
    fn make_page_id(&self, page_no: u32) -> ferrisdb_storage::PageId {
        ferrisdb_storage::PageId::new(0, 0, self.table_oid, page_no)
    }

    /// 写入 WAL 记录
    ///
    /// 优先写 WalWriter（磁盘持久化），回退到 WalBuffer（内存）。
    /// 错误必须传播——静默丢弃会导致已提交数据在 crash 后丢失。
    #[inline]
    fn wal_write(&self, record_data: &[u8]) -> ferrisdb_core::Result<()> {
        if let Some(ref writer) = self.wal_writer {
            writer.write(record_data)?;
        } else if let Some(ref buf) = self.wal_buffer {
            buf.write(record_data)?;
        }
        Ok(())
    }

    /// 插入元组
    ///
    /// 返回元组 ID。如果提供了 txn，自动记录 undo action 用于回滚。
    pub fn insert(
        &self,
        data: &[u8],
        xmin: Xid,
        cmin: u32,
    ) -> ferrisdb_core::Result<TupleId> {
        self.insert_with_undo(data, xmin, cmin, None)
    }

    /// 插入元组（带 undo 支持）
    pub fn insert_with_undo(
        &self,
        data: &[u8],
        xmin: Xid,
        cmin: u32,
        txn: Option<&mut Transaction>,
    ) -> ferrisdb_core::Result<TupleId> {
        let header = TupleHeader::new(xmin, cmin);
        let header_bytes = header.serialize();

        let total_size = TupleHeader::serialized_size() + data.len();
        let needed = (total_size + 5) as u16;

        // Fast path: FSM 查找有空间的页，回退到 last page
        let target_page = self.fsm_find_page(needed);
        let max_page = self.current_page.load(std::sync::atomic::Ordering::Acquire);
        let try_page = target_page.unwrap_or_else(|| if max_page > 0 { max_page - 1 } else { u32::MAX });
        if try_page < max_page {
            let last_page = try_page;
            let tag = self.make_tag(last_page);
            if let Ok(pinned) = self.buffer_pool.pin(&tag) {
                let page_ptr = pinned.page_data();
                let _lock = pinned.lock_exclusive();
                let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, 8192)) };

                if page.has_free_space(needed) {
                    if let Some(offset) = page.insert_tuple_from_parts(&header_bytes, data) {
                        // WAL
                        let mut tuple_buf = Vec::with_capacity(total_size);
                        tuple_buf.extend_from_slice(&header_bytes);
                        tuple_buf.extend_from_slice(data);
                        let page_id = self.make_page_id(last_page);
                        let wal_rec = WalHeapInsert::new(page_id, offset, &tuple_buf);
                        self.wal_write(&wal_rec.serialize_with_data(&tuple_buf))?;

                        // Undo: 记录插入，回滚时标记为 dead
                        if let Some(txn) = txn {
                            txn.push_undo(UndoAction::Insert {
                                table_oid: self.table_oid,
                                page_no: last_page,
                                tuple_offset: offset,
                            });
                        }

                        // 更新 FSM
                        let free = page.free_space();
                        drop(_lock);
                        pinned.mark_dirty();
                        self.fsm_update(last_page, free);
                        return Ok(TupleId::new(last_page, offset));
                    }
                }
            }
        }

        // Allocate a new page
        let new_page_no = self.current_page.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let tag = self.make_tag(new_page_no);

        let pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();
        let _lock = pinned.lock_exclusive();

        let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, 8192)) };
        page.init();

        let offset = page.insert_tuple_from_parts(&header_bytes, data)
            .ok_or_else(|| FerrisDBError::Internal("Failed to insert tuple in new page".to_string()))?;

        // WAL
        let mut tuple_buf = Vec::with_capacity(total_size);
        tuple_buf.extend_from_slice(&header_bytes);
        tuple_buf.extend_from_slice(data);
        let page_id = self.make_page_id(new_page_no);
        let wal_rec = WalHeapInsert::new(page_id, offset, &tuple_buf);
        self.wal_write(&wal_rec.serialize_with_data(&tuple_buf))?;

        // Undo
        if let Some(txn) = txn {
            txn.push_undo(UndoAction::Insert {
                table_oid: self.table_oid,
                page_no: new_page_no,
                tuple_offset: offset,
            });
        }

        drop(_lock);
        pinned.mark_dirty();

        Ok(TupleId::new(new_page_no, offset))
    }

    /// 更新元组
    pub fn update(
        &self,
        tid: TupleId,
        new_data: &[u8],
        xmax: Xid,
        cmax: u32,
    ) -> ferrisdb_core::Result<TupleId> {
        self.update_with_undo(tid, new_data, xmax, cmax, None)
    }

    /// 更新元组（带 undo 支持）
    pub fn update_with_undo(
        &self,
        tid: TupleId,
        new_data: &[u8],
        xmax: Xid,
        cmax: u32,
        mut txn: Option<&mut Transaction>,
    ) -> ferrisdb_core::Result<TupleId> {
        let tag = self.make_tag(tid.ip_blkid);
        let pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();

        // Collect undo info while holding the lock, apply after releasing
        let mut undo_actions: Vec<UndoAction> = Vec::new();

        let header_bytes;
        let total_size;
        let tuple_data;
        {
            let _lock = pinned.lock_exclusive();
            let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, 8192)) };

            let old_data = page.get_tuple(tid.ip_posid)
                .ok_or_else(|| FerrisDBError::Internal("Tuple not found".to_string()))?;

            let old_tuple_len = old_data.len();
            let new_tuple_len = TupleHeader::serialized_size() + new_data.len();

            // HOT update: same size → in-place
            if new_tuple_len == old_tuple_len {
                let mut old_header = TupleHeader::deserialize(old_data)
                    .ok_or_else(|| FerrisDBError::Internal("Invalid tuple header".to_string()))?;

                // Save old data for undo before modification
                if txn.is_some() {
                    undo_actions.push(UndoAction::InplaceUpdate {
                        table_oid: self.table_oid,
                        page_no: tid.ip_blkid,
                        tuple_offset: tid.ip_posid,
                        old_data: old_data.to_vec(),
                    });
                }

                old_header.xmax = xmax;
                old_header.cmax = cmax;

                let old_data_mut = page.get_tuple_mut(tid.ip_posid).unwrap();
                old_data_mut[0..32].copy_from_slice(&old_header.serialize());
                old_data_mut[32..].copy_from_slice(new_data);

                // WAL
                let updated_data: Vec<u8> = {
                    let mut v = Vec::with_capacity(old_tuple_len);
                    v.extend_from_slice(&old_data_mut[0..32]);
                    v.extend_from_slice(&old_data_mut[32..]);
                    v
                };
                let page_id = self.make_page_id(tid.ip_blkid);
                let wal_rec = WalHeapInplaceUpdate::new(page_id, tid.ip_posid, &updated_data);
                self.wal_write(&wal_rec.serialize_with_data(&updated_data))?;

                drop(_lock);
                pinned.mark_dirty();

                // Push undo actions to transaction
                if let Some(ref mut txn) = txn {
                    for action in undo_actions {
                        txn.push_undo(action);
                    }
                }

                return Ok(tid);
            }

            // Non-HOT path: create new version
            let mut old_header = TupleHeader::deserialize(old_data)
                .ok_or_else(|| FerrisDBError::Internal("Invalid tuple header".to_string()))?;

            // Save old header for undo before modification
            let old_header_for_undo = if txn.is_some() {
                Some(old_header.serialize())
            } else {
                None
            };

            let old_header_xmin = old_header.xmin;
            let old_header_cmin = old_header.cmin;

            old_header.xmax = xmax;
            old_header.cmax = cmax;
            let updated_old_header = old_header.serialize();

            let old_data_mut = page.get_tuple_mut(tid.ip_posid).unwrap();
            old_data_mut[0..32].copy_from_slice(&updated_old_header);

            let new_header = TupleHeader::new(old_header_xmin, old_header_cmin);
            // new tuple's ctid initially points to self (will be set when we know its location)
            header_bytes = new_header.serialize();
            total_size = TupleHeader::serialized_size() + new_data.len();
            tuple_data = {
                let mut td = vec![0u8; total_size];
                td[0..32].copy_from_slice(&header_bytes);
                td[32..].copy_from_slice(new_data);
                td
            };

            // Try same-page insert
            if page.has_free_space((total_size + 5) as u16) {
                if let Some(offset) = page.insert_tuple(&tuple_data) {
                    // Set old tuple's ctid to point to new version (forward chain)
                    let old_data_mut2 = page.get_tuple_mut(tid.ip_posid).unwrap();
                    let new_ctid = TupleId::new(tid.ip_blkid, offset);
                    old_data_mut2[24..28].copy_from_slice(&new_ctid.ip_blkid.to_le_bytes());
                    old_data_mut2[28..30].copy_from_slice(&new_ctid.ip_posid.to_le_bytes());

                    let old_page_id = self.make_page_id(tid.ip_blkid);
                    let wal_old = WalHeapUpdateOldPage::new(old_page_id, tid.ip_posid);
                    self.wal_write(&wal_old.serialize_with_header(&updated_old_header))?;
                    let wal_new = WalHeapInsert::new(old_page_id, offset, &tuple_data);
                    self.wal_write(&wal_new.serialize_with_data(&tuple_data))?;

                    drop(_lock);
                    pinned.mark_dirty();

                    // Undo: new page dead + old page header restore (reverse order)
                    if let Some(ref mut txn) = txn {
                        if let Some(old_hdr) = old_header_for_undo {
                            txn.push_undo(UndoAction::UpdateNewPage {
                                table_oid: self.table_oid,
                                page_no: tid.ip_blkid,
                                tuple_offset: offset,
                            });
                            txn.push_undo(UndoAction::UpdateOldPage {
                                table_oid: self.table_oid,
                                page_no: tid.ip_blkid,
                                tuple_offset: tid.ip_posid,
                                old_header: old_hdr,
                            });
                        }
                    }

                    let _ = self.prune_page(tid.ip_blkid);
                    return Ok(TupleId::new(tid.ip_blkid, offset));
                }
            }

            // WAL: old page xmax set
            let old_page_id = self.make_page_id(tid.ip_blkid);
            let wal_old = WalHeapUpdateOldPage::new(old_page_id, tid.ip_posid);
            self.wal_write(&wal_old.serialize_with_header(&updated_old_header))?;

            // Undo for cross-page: old page header restore
            if txn.is_some() {
                if let Some(old_hdr) = old_header_for_undo {
                    undo_actions.push(UndoAction::UpdateOldPage {
                        table_oid: self.table_oid,
                        page_no: tid.ip_blkid,
                        tuple_offset: tid.ip_posid,
                        old_header: old_hdr,
                    });
                }
            }

            drop(_lock);
        }
        pinned.mark_dirty();

        // New page insert
        let new_page_no = self.current_page.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let new_tag = self.make_tag(new_page_no);

        let new_pinned = self.buffer_pool.pin(&new_tag)?;
        let new_page_ptr = new_pinned.page_data();
        let new_offset;
        {
            let _new_lock = new_pinned.lock_exclusive();
            let new_page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(new_page_ptr, 8192)) };
            new_page.init();

            new_offset = new_page.insert_tuple(&tuple_data)
                .ok_or_else(|| FerrisDBError::Internal("Failed to insert tuple in new page".to_string()))?;

            let new_page_id = self.make_page_id(new_page_no);
            let wal_new = WalHeapUpdateNewPage::new(new_page_id, new_offset, &tuple_data);
            self.wal_write(&wal_new.serialize_with_data(&tuple_data))?;

            drop(_new_lock);
        }
        new_pinned.mark_dirty();

        // Update old tuple's ctid to point to new version (cross-page forward chain)
        // + WAL record for ctid update (防止 crash 后 ctid 链断裂)
        {
            let old_pinned = self.buffer_pool.pin(&self.make_tag(tid.ip_blkid))?;
            let _old_lock = old_pinned.lock_exclusive();
            let old_page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(old_pinned.page_data(), 8192)) };
            if let Some(old_tuple) = old_page.get_tuple_mut(tid.ip_posid) {
                let new_ctid = TupleId::new(new_page_no, new_offset);
                old_tuple[24..28].copy_from_slice(&new_ctid.ip_blkid.to_le_bytes());
                old_tuple[28..30].copy_from_slice(&new_ctid.ip_posid.to_le_bytes());

                // WAL: 记录 old page ctid 更新（header 的 32 字节包含 ctid）
                let old_page_id = self.make_page_id(tid.ip_blkid);
                let wal_ctid = WalHeapUpdateOldPage::new(old_page_id, tid.ip_posid);
                self.wal_write(&wal_ctid.serialize_with_header(&old_tuple[..32]))?;
            }
            drop(_old_lock);
            old_pinned.mark_dirty();
        }

        // Undo: mark new page tuple for dead on rollback
        if let Some(ref mut txn) = txn {
            undo_actions.push(UndoAction::UpdateNewPage {
                table_oid: self.table_oid,
                page_no: new_page_no,
                tuple_offset: new_offset,
            });
            for action in undo_actions {
                txn.push_undo(action);
            }
        }

        let _ = self.prune_page(tid.ip_blkid);

        Ok(TupleId::new(new_page_no, new_offset))
    }

    /// 删除元组
    pub fn delete(&self, tid: TupleId, xmax: Xid, cmax: u32) -> ferrisdb_core::Result<()> {
        self.delete_with_undo(tid, xmax, cmax, None)
    }

    /// 删除元组（带 undo 支持）
    pub fn delete_with_undo(
        &self,
        tid: TupleId,
        xmax: Xid,
        cmax: u32,
        txn: Option<&mut Transaction>,
    ) -> ferrisdb_core::Result<()> {
        let tag = self.make_tag(tid.ip_blkid);
        let pinned = self.buffer_pool.pin(&tag)?;
        let page_ptr = pinned.page_data();
        let _lock = pinned.lock_exclusive();

        let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, 8192)) };

        let old_data = page.get_tuple(tid.ip_posid)
            .ok_or_else(|| FerrisDBError::Internal("Tuple not found".to_string()))?;

        // Save old data for undo before modification
        let old_data_for_undo: Option<Vec<u8>> = if txn.is_some() {
            Some(old_data.to_vec())
        } else {
            None
        };

        let mut header = TupleHeader::deserialize(old_data)
            .ok_or_else(|| FerrisDBError::Internal("Invalid tuple header".to_string()))?;

        header.xmax = xmax;
        header.cmax = cmax;
        let updated_header = header.serialize();

        let old_data_mut = page.get_tuple_mut(tid.ip_posid).unwrap();
        old_data_mut[0..32].copy_from_slice(&updated_header);

        // WAL
        let page_id = self.make_page_id(tid.ip_blkid);
        let wal_rec = WalHeapDelete::new(page_id, tid.ip_posid);
        self.wal_write(&wal_rec.to_bytes())?;

        drop(_lock);
        pinned.mark_dirty();

        // Undo: 记录删除，回滚时恢复旧数据
        if let Some(txn) = txn {
            if let Some(old_bytes) = old_data_for_undo {
                txn.push_undo(UndoAction::Delete {
                    table_oid: self.table_oid,
                    page_no: tid.ip_blkid,
                    tuple_offset: tid.ip_posid,
                    old_data: old_bytes,
                });
            }
        }

        Ok(())
    }

    /// 获取元组（带可见性判断）
    ///
    /// 如果提供了 transaction，只返回对该事务可见的 tuple。
    pub fn fetch_visible(&self, tid: TupleId, txn: &crate::transaction::Transaction) -> ferrisdb_core::Result<Option<(TupleHeader, Vec<u8>)>> {
        match self.fetch(tid)? {
            Some((header, data)) => {
                if txn.is_visible(header.xmin, header.xmax, header.cmin, header.cmax) {
                    Ok(Some((header, data)))
                } else {
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    /// 获取元组（跟踪 ctid 前向链到最新版本）
    ///
    /// 如果 tuple 有 xmax（已被更新）且 ctid 指向其他位置，自动跟踪到最新版本。
    /// HOT update 不创建链（TupleId 不变）；non-HOT update 创建前向链（old→new）。
    pub fn fetch(&self, tid: TupleId) -> ferrisdb_core::Result<Option<(TupleHeader, Vec<u8>)>> {
        let mut current_tid = tid;

        for _ in 0..10 {
            if !current_tid.is_valid() {
                return Ok(None);
            }

            let tag = self.make_tag(current_tid.ip_blkid);
            let pinned = match self.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => return Ok(None),
            };
            let page_ptr = pinned.page_data();
            let _lock = pinned.lock_shared();

            let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, 8192)) };

            let data = match page.get_tuple(current_tid.ip_posid) {
                Some(d) => d,
                None => return Ok(None),
            };

            let header = match TupleHeader::deserialize(data) {
                Some(h) => h,
                None => return Ok(None),
            };

            let user_data = data[32..].to_vec();
            drop(_lock);

            // Follow forward ctid chain: if xmax is set and ctid points elsewhere
            if header.xmax.is_valid() && header.ctid.is_valid()
                && header.ctid != current_tid
            {
                current_tid = header.ctid;
                continue;
            }

            return Ok(Some((header, user_data)));
        }

        Ok(None)
    }

    /// 获取表 OID
    #[inline]
    pub fn table_oid(&self) -> u32 {
        self.table_oid
    }

    /// 机会式页面清理
    ///
    /// 扫描页面上所有元组，标记对所有活跃事务不可见的死元组为 Dead，然后压缩。
    /// 频率控制：仅在适当时机执行，避免每次 DML 都 prune。
    fn prune_page(&self, page_no: u32) {
        // 简单频率控制：利用 page_no 的低位做概率采样（~1/16 概率执行）
        if page_no & 0xF != 0 {
            return;
        }
        let snapshot = self.txn_mgr.current_snapshot();
        let tag = self.make_tag(page_no);
        let pinned = match self.buffer_pool.pin(&tag) {
            Ok(p) => p,
            Err(_) => return,
        };
        let page_ptr = pinned.page_data();
        let _lock = pinned.lock_exclusive();
        let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, 8192)) };

        let linp_count = page.linp_count() as usize;
        if linp_count == 0 { return; }

        let mut any_dead = false;
        for i in 0..linp_count {
            let item = match page.item_id(i as u16) {
                Some(id) => id,
                None => continue,
            };
            if item.lp_flags != ItemIdFlags::Normal as u8 { continue; }
            let off = item.lp_off as usize;
            let len = item.lp_len as usize;
            if off + 32 > 8192 || len < 32 { continue; }
            let data = &page.as_bytes()[off..off + len];
            let xmax = Xid::from_raw(u64::from_le_bytes(data[8..16].try_into().unwrap()));
            if !xmax.is_valid() { continue; }
            let xmin = Xid::from_raw(u64::from_le_bytes(data[0..8].try_into().unwrap()));
            if Self::is_tuple_dead_to_all(&xmin, &xmax, &snapshot, &self.txn_mgr) {
                page.mark_dead((i + 1) as u16);
                any_dead = true;
            }
        }

        if any_dead {
            let reclaimed = page.compact_dead();
            drop(_lock);
            pinned.mark_dirty();
            // 更新 FSM
            let pinned2 = match self.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => return,
            };
            let pg = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(pinned2.page_data(), 8192)) };
            self.fsm_update(page_no, pg.free_space());
            let _ = reclaimed;
        }
    }

    /// 显式 Vacuum 单个页面：清理对所有活跃事务不可见的死元组
    ///
    /// 调用者负责确保页面上没有活跃的索引引用指向将被清理的元组，
    /// 或者调用者知道所有清理的元组都是 HOT 链上被取代的旧版本。
    pub fn vacuum_page(&self, page_no: u32) -> usize {
        let snapshot = self.txn_mgr.current_snapshot();
        self.do_vacuum_page(page_no, &snapshot)
    }

    /// 执行页面 vacuum
    fn do_vacuum_page(&self, page_no: u32, snapshot: &Snapshot) -> usize {
        let tag = self.make_tag(page_no);
        let pinned = match self.buffer_pool.pin(&tag) {
            Ok(p) => p,
            Err(_) => return 0,
        };
        let page_ptr = pinned.page_data();
        let _lock = pinned.lock_exclusive();

        let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, 8192)) };

        let linp_count = page.linp_count() as usize;
        if linp_count == 0 {
            return 0;
        }

        let mut any_dead = false;

        for i in 0..linp_count {
            let item = match page.item_id(i as u16) {
                Some(id) => id,
                None => continue,
            };

            if item.lp_flags != ItemIdFlags::Normal as u8 {
                continue;
            }

            let off = item.lp_off as usize;
            let len = item.lp_len as usize;

            if off + 32 > 8192 || len < 32 {
                continue;
            }

            let data = &page.as_bytes()[off..off + len];
            let xmin = Xid::from_raw(u64::from_le_bytes(data[0..8].try_into().unwrap()));
            let xmax = Xid::from_raw(u64::from_le_bytes(data[8..16].try_into().unwrap()));

            if !xmax.is_valid() {
                continue;
            }

            if Self::is_tuple_dead_to_all(&xmin, &xmax, snapshot, &self.txn_mgr) {
                page.mark_dead((i + 1) as u16);
                any_dead = true;
            }
        }

        if any_dead {
            let reclaimed = page.compact_dead();
            drop(_lock);
            pinned.mark_dirty();
            reclaimed
        } else {
            0
        }
    }

    /// Vacuum 所有页面，返回总共回收的字节数
    pub fn vacuum(&self) -> usize {
        let snapshot = self.txn_mgr.current_snapshot();
        let max_page = self.current_page.load(std::sync::atomic::Ordering::Acquire);
        let mut total_reclaimed = 0;
        for page_no in 0..max_page {
            total_reclaimed += self.do_vacuum_page(page_no, &snapshot);
        }
        total_reclaimed
    }

    #[allow(dead_code)]
    fn _prune_page_impl(&self, page_no: u32) {
        // 保留旧实现供参考
        let snapshot = self.txn_mgr.current_snapshot();

        let tag = self.make_tag(page_no);
        let pinned = match self.buffer_pool.pin(&tag) {
            Ok(p) => p,
            Err(_) => return,
        };
        let page_ptr = pinned.page_data();
        let _lock = pinned.lock_exclusive();

        let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, 8192)) };

        let linp_count = page.linp_count() as usize;
        if linp_count == 0 {
            return;
        }

        let mut any_dead = false;

        for i in 0..linp_count {
            let item = match page.item_id(i as u16) {
                Some(id) => id,
                None => continue,
            };

            if item.lp_flags != ItemIdFlags::Normal as u8 {
                continue;
            }

            let off = item.lp_off as usize;
            let len = item.lp_len as usize;

            if off + 32 > 8192 || len < 32 {
                continue;
            }

            let data = &page.as_bytes()[off..off + len];
            let xmin = Xid::from_raw(u64::from_le_bytes(data[0..8].try_into().unwrap()));
            let xmax = Xid::from_raw(u64::from_le_bytes(data[8..16].try_into().unwrap()));

            if !xmax.is_valid() {
                continue;
            }

            if Self::is_tuple_dead_to_all(&xmin, &xmax, &snapshot, &self.txn_mgr) {
                page.mark_dead((i + 1) as u16);
                any_dead = true;
            }
        }

        if any_dead {
            let _reclaimed = page.compact_dead();
            drop(_lock);
            pinned.mark_dirty();
        }
    }

    /// 判断元组是否对所有活跃事务都不可见
    #[allow(dead_code)]
    fn is_tuple_dead_to_all(xmin: &Xid, xmax: &Xid, snapshot: &Snapshot, txn_mgr: &TransactionManager) -> bool {
        let xmax_csn = txn_mgr.get_csn(*xmax);
        let xmin_csn = txn_mgr.get_csn(*xmin);

        if !xmax_csn.is_valid() && !xmin_csn.is_valid()
            && !snapshot.is_active(*xmax) && !snapshot.is_active(*xmin) {
            return false;
        }

        if snapshot.is_active(*xmax) {
            return false;
        }

        if !xmax_csn.is_valid() && !xmin_csn.is_valid() {
            return false;
        }

        if snapshot.is_active(*xmin) {
            return false;
        }

        true
    }
}

/// 简化的表扫描器
pub struct HeapScan<'a> {
    table: &'a HeapTable,
    current_page: u32,
    current_offset: u16,
    max_page: u32,
}

impl<'a> HeapScan<'a> {
    /// 创建新的扫描器
    pub fn new(table: &'a HeapTable) -> Self {
        let max_page = table.current_page.load(std::sync::atomic::Ordering::Acquire);
        Self {
            table,
            current_page: 0,
            current_offset: 0,
            max_page,
        }
    }

    /// 获取下一个可见元组（带事务可见性过滤）
    pub fn next_visible(&mut self, txn: &crate::transaction::Transaction) -> ferrisdb_core::Result<Option<(TupleId, TupleHeader, Vec<u8>)>> {
        loop {
            match self.next()? {
                Some((tid, header, data)) => {
                    if txn.is_visible(header.xmin, header.xmax, header.cmin, header.cmax) {
                        return Ok(Some((tid, header, data)));
                    }
                    // 不可见，继续扫描下一个
                }
                None => return Ok(None),
            }
        }
    }

    /// 获取下一个元组（不检查可见性）
    pub fn next(&mut self) -> ferrisdb_core::Result<Option<(TupleId, TupleHeader, Vec<u8>)>> {
        while self.current_page < self.max_page {
            let tag = self.table.make_tag(self.current_page);

            let pinned = match self.table.buffer_pool.pin(&tag) {
                Ok(p) => p,
                Err(_) => {
                    self.current_page += 1;
                    self.current_offset = 0;
                    continue;
                }
            };

            let page_ptr = pinned.page_data();
            let _lock = pinned.lock_shared();

            let page = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(page_ptr, 8192)) };

            let linp_count = page.linp_count();

            while self.current_offset < linp_count {
                let offset = self.current_offset;
                self.current_offset += 1;

                if let Some(data) = page.get_tuple(offset + 1) {
                    if let Some(header) = TupleHeader::deserialize(data) {
                        let user_data = data[32..].to_vec();
                        let tid = TupleId::new(self.current_page, offset + 1);
                        drop(_lock);
                        return Ok(Some((tid, header, user_data)));
                    }
                }
            }

            drop(_lock);
            self.current_page += 1;
            self.current_offset = 0;
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_header_size() {
        assert_eq!(TupleHeader::serialized_size(), 32);
    }

    #[test]
    fn test_tuple_header_serialize() {
        let header = TupleHeader::new(Xid::new(0, 1), 0);
        let bytes = header.serialize();
        assert_eq!(bytes.len(), 32);

        let deserialized = TupleHeader::deserialize(&bytes).unwrap();
        assert_eq!(deserialized.xmin.raw(), header.xmin.raw());
        assert_eq!(deserialized.cmin, header.cmin);
    }

    #[test]
    fn test_tuple_header_deserialize() {
        let header = TupleHeader {
            xmin: Xid::new(0, 5),
            xmax: Xid::new(0, 10),
            cmin: 1,
            cmax: 2,
            ctid: TupleId::new(100, 5),
            infomask: 0x1234,
        };

        let bytes = header.serialize();
        let restored = TupleHeader::deserialize(&bytes).unwrap();

        assert_eq!(restored.xmin.raw(), header.xmin.raw());
        assert_eq!(restored.xmax.raw(), header.xmax.raw());
        assert_eq!(restored.cmin, header.cmin);
        assert_eq!(restored.cmax, header.cmax);
        assert_eq!(u32::from_le_bytes(bytes[24..28].try_into().unwrap()), 100);
        assert_eq!(u16::from_le_bytes(bytes[28..30].try_into().unwrap()), 5);
        assert_eq!(restored.infomask, header.infomask);
    }
}
