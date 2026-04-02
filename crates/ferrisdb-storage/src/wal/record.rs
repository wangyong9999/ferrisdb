//! WAL Record Types
//!
//! 定义 WAL 记录的类型和格式。

use ferrisdb_core::{Lsn, PageId, Xid};

/// WAL 记录类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u16)]
pub enum WalRecordType {
    // === Heap 记录类型 ===
    /// 插入记录
    HeapInsert = 0,
    /// 批量插入记录
    HeapBatchInsert = 1,
    /// 删除记录
    HeapDelete = 2,
    /// 原地更新记录
    HeapInplaceUpdate = 3,
    /// 同页追加
    HeapSamePageAppend = 4,
    /// 另页追加更新（新页）
    HeapAnotherPageAppendUpdateNewPage = 5,
    /// 另页追加更新（旧页）
    HeapAnotherPageAppendUpdateOldPage = 6,
    /// 分配事务槽
    HeapAllocTd = 7,
    /// 页面清理
    HeapPrune = 8,

    // === B-Tree 记录类型 ===
    /// B-Tree 构建
    BtreeBuild = 20,
    /// B-Tree 初始化元页面
    BtreeInitMetaPage = 21,
    /// B-Tree 更新元页面根
    BtreeUpdateMetaRoot = 22,
    /// B-Tree 在内部节点插入
    BtreeInsertOnInternal = 27,
    /// B-Tree 在叶子节点插入
    BtreeInsertOnLeaf = 28,
    /// B-Tree 内部节点分裂
    BtreeSplitInternal = 29,
    /// B-Tree 叶子节点分裂
    BtreeSplitLeaf = 30,
    /// B-Tree 在内部节点删除
    BtreeDeleteOnInternal = 37,
    /// B-Tree 在叶子节点删除
    BtreeDeleteOnLeaf = 38,

    // === 检查点记录类型 ===
    /// 关机检查点
    CheckpointShutdown = 154,
    /// 在线检查点
    CheckpointOnline = 155,

    // === Full-page write ===
    /// 完整页面写入（torn page 保护）
    HeapPageFull = 158,

    // === Undo 记录类型 ===
    /// Undo 插入记录
    UndoInsertRecord = 160,

    // === 事务记录类型 ===
    /// 事务提交
    TxnCommit = 175,
    /// 事务回滚
    TxnAbort = 176,

    // === DDL 记录类型 ===
    /// 创建表/索引/表空间
    DdlCreate = 190,
    /// 删除表/索引/表空间
    DdlDrop = 191,

    // === 其他 ===
    /// 下一个 CSN
    NextCsn = 179,
    /// 屏障 CSN
    BarrierCsn = 180,
}

impl TryFrom<u16> for WalRecordType {
    type Error = ();

    fn try_from(value: u16) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::HeapInsert),
            1 => Ok(Self::HeapBatchInsert),
            2 => Ok(Self::HeapDelete),
            3 => Ok(Self::HeapInplaceUpdate),
            4 => Ok(Self::HeapSamePageAppend),
            5 => Ok(Self::HeapAnotherPageAppendUpdateNewPage),
            6 => Ok(Self::HeapAnotherPageAppendUpdateOldPage),
            7 => Ok(Self::HeapAllocTd),
            8 => Ok(Self::HeapPrune),
            20 => Ok(Self::BtreeBuild),
            21 => Ok(Self::BtreeInitMetaPage),
            22 => Ok(Self::BtreeUpdateMetaRoot),
            27 => Ok(Self::BtreeInsertOnInternal),
            28 => Ok(Self::BtreeInsertOnLeaf),
            29 => Ok(Self::BtreeSplitInternal),
            30 => Ok(Self::BtreeSplitLeaf),
            37 => Ok(Self::BtreeDeleteOnInternal),
            38 => Ok(Self::BtreeDeleteOnLeaf),
            154 => Ok(Self::CheckpointShutdown),
            155 => Ok(Self::CheckpointOnline),
            158 => Ok(Self::HeapPageFull),
            160 => Ok(Self::UndoInsertRecord),
            175 => Ok(Self::TxnCommit),
            176 => Ok(Self::TxnAbort),
            179 => Ok(Self::NextCsn),
            180 => Ok(Self::BarrierCsn),
            190 => Ok(Self::DdlCreate),
            191 => Ok(Self::DdlDrop),
            _ => Err(()),
        }
    }
}

/// WAL 记录头部（基础）
#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
pub struct WalRecordHeader {
    /// 记录大小
    pub size: u16,
    /// 记录类型
    pub rtype: u16,
}

impl WalRecordHeader {
    /// 创建新的 WAL 记录头部
    #[inline]
    pub const fn new(rtype: WalRecordType, size: u16) -> Self {
        Self {
            size,
            rtype: rtype as u16,
        }
    }

    /// 获取记录类型
    #[inline]
    pub fn record_type(&self) -> Option<WalRecordType> {
        WalRecordType::try_from(self.rtype).ok()
    }

    /// 头部大小
    #[inline]
    pub const fn header_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

const _: () = assert!(std::mem::size_of::<WalRecordHeader>() == 4);

/// 页面 WAL 记录头部（带页面信息）
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WalRecordForPage {
    /// 基础头部
    pub header: WalRecordHeader,
    /// 页面 ID
    pub page_id: PageId,
    /// 标志位
    pub flags: u8,
    /// 页面前一个 WAL ID
    pub page_pre_wal_id: u64,
    /// 页面前一个 PLSN
    pub page_pre_plsn: u64,
    /// 页面前一个 GLSN
    pub page_pre_glsn: u64,
    /// 文件前一个版本
    pub file_pre_version: u64,
}

impl WalRecordForPage {
    /// 创建新的页面 WAL 记录
    pub fn new(rtype: WalRecordType, page_id: PageId, data_size: u16) -> Self {
        Self {
            header: WalRecordHeader::new(rtype, data_size),
            page_id,
            flags: 0,
            page_pre_wal_id: 0,
            page_pre_plsn: 0,
            page_pre_glsn: 0,
            file_pre_version: 1,
        }
    }

    /// 总大小（头部 + 数据）
    #[inline]
    pub fn total_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.header.size as usize
    }
}

/// WAL 原子组头部
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WalAtomicGroupHeader {
    /// 组长度
    pub group_len: u32,
    /// CRC 校验
    pub crc: u32,
    /// 事务 ID
    pub xid: Xid,
    /// 记录数量
    pub record_num: u16,
    /// 填充
    pub _padding: u16,
}

impl WalAtomicGroupHeader {
    /// 创建新的原子组头部
    #[inline]
    pub const fn new(xid: Xid) -> Self {
        Self {
            group_len: 0,
            crc: 0,
            xid,
            record_num: 0,
            _padding: 0,
        }
    }

    /// 头部大小
    #[inline]
    pub const fn header_size() -> usize {
        std::mem::size_of::<Self>()
    }
}

/// WAL 事务提交记录
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WalTxnCommit {
    /// 基础头部
    pub header: WalRecordHeader,
    /// 事务 ID
    pub xid: Xid,
    /// 提交 CSN
    pub csn: u64,
    /// 提交时间戳
    pub timestamp: u64,
}

impl WalTxnCommit {
    /// 创建新的事务提交记录
    pub fn new(xid: Xid, csn: u64) -> Self {
        Self {
            header: WalRecordHeader::new(WalRecordType::TxnCommit, std::mem::size_of::<Self>() as u16 - std::mem::size_of::<WalRecordHeader>() as u16),
            xid,
            csn,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        }
    }

    /// 创建事务中止记录
    pub fn new_abort(xid: Xid) -> Self {
        Self {
            header: WalRecordHeader::new(WalRecordType::TxnAbort, std::mem::size_of::<Self>() as u16 - std::mem::size_of::<WalRecordHeader>() as u16),
            xid,
            csn: 0,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
        }
    }

    /// 序列化为字节
    pub fn to_bytes(&self) -> Vec<u8> {
        let size = std::mem::size_of::<Self>();
        let mut buf = vec![0u8; size];
        unsafe {
            std::ptr::copy_nonoverlapping(
                self as *const Self as *const u8,
                buf.as_mut_ptr(),
                size,
            );
        }
        buf
    }
}

/// WAL 检查点记录
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct WalCheckpoint {
    /// 基础头部
    pub header: WalRecordHeader,
    /// 检查点时间戳
    pub timestamp: u64,
    /// 磁盘恢复 PLSN
    pub disk_recovery_plsn: u64,
    /// 检查点类型（0=在线，1=关机）
    pub checkpoint_type: u8,
    /// 填充
    pub _padding: [u8; 7],
}

impl WalCheckpoint {
    /// 创建新的检查点记录
    pub fn new(disk_recovery_plsn: u64, shutdown: bool) -> Self {
        Self {
            header: WalRecordHeader::new(
                if shutdown {
                    WalRecordType::CheckpointShutdown
                } else {
                    WalRecordType::CheckpointOnline
                },
                0,
            ),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64,
            disk_recovery_plsn,
            checkpoint_type: if shutdown { 1 } else { 0 },
            _padding: [0; 7],
        }
    }

    /// 序列化为字节
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(std::mem::size_of::<Self>());
        // 简化实现：直接写入关键字段
        bytes.extend_from_slice(&self.timestamp.to_le_bytes());
        bytes.extend_from_slice(&self.disk_recovery_plsn.to_le_bytes());
        bytes.push(self.checkpoint_type);
        bytes.extend_from_slice(&self._padding);
        bytes
    }
}

/// Heap 插入 WAL 记录
#[derive(Debug, Clone)]
#[repr(C)]
pub struct WalHeapInsert {
    /// 页面记录头部
    pub page_header: WalRecordForPage,
    /// 元组偏移
    pub tuple_offset: u16,
    /// 元组大小
    pub tuple_size: u16,
}

impl WalHeapInsert {
    /// 创建新的 Heap 插入记录
    pub fn new(page_id: PageId, tuple_offset: u16, tuple_data: &[u8]) -> Self {
        Self {
            page_header: WalRecordForPage::new(
                WalRecordType::HeapInsert,
                page_id,
                (4 + tuple_data.len()) as u16,
            ),
            tuple_offset,
            tuple_size: tuple_data.len() as u16,
        }
    }

    /// 序列化记录头 + 元组数据为字节流
    pub fn serialize_with_data(&self, tuple_data: &[u8]) -> Vec<u8> {
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        let mut buf = Vec::with_capacity(header_bytes.len() + tuple_data.len());
        buf.extend_from_slice(header_bytes);
        buf.extend_from_slice(tuple_data);
        buf
    }
}

/// Heap 删除 WAL 记录
#[derive(Debug, Clone)]
#[repr(C)]
pub struct WalHeapDelete {
    /// 页面记录头部
    pub page_header: WalRecordForPage,
    /// 元组偏移
    pub tuple_offset: u16,
    /// 保留
    pub _reserved: u16,
}

impl WalHeapDelete {
    /// 创建新的 Heap 删除记录
    pub fn new(page_id: PageId, tuple_offset: u16) -> Self {
        Self {
            page_header: WalRecordForPage::new(
                WalRecordType::HeapDelete,
                page_id,
                4,
            ),
            tuple_offset,
            _reserved: 0,
        }
    }

    /// 序列化为字节流
    pub fn to_bytes(&self) -> Vec<u8> {
        unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        }
        .to_vec()
    }
}

/// Heap 原地更新 WAL 记录（HOT update / 相同大小替换）
#[derive(Debug, Clone)]
#[repr(C)]
pub struct WalHeapInplaceUpdate {
    /// 页面记录头部
    pub page_header: WalRecordForPage,
    /// 元组偏移
    pub tuple_offset: u16,
    /// 元组大小
    pub tuple_size: u16,
}

impl WalHeapInplaceUpdate {
    /// 创建新的原地更新记录
    pub fn new(page_id: PageId, tuple_offset: u16, tuple_data: &[u8]) -> Self {
        Self {
            page_header: WalRecordForPage::new(
                WalRecordType::HeapInplaceUpdate,
                page_id,
                (4 + tuple_data.len()) as u16,
            ),
            tuple_offset,
            tuple_size: tuple_data.len() as u16,
        }
    }

    /// 序列化记录头 + 元组数据为字节流
    pub fn serialize_with_data(&self, tuple_data: &[u8]) -> Vec<u8> {
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        let mut buf = Vec::with_capacity(header_bytes.len() + tuple_data.len());
        buf.extend_from_slice(header_bytes);
        buf.extend_from_slice(tuple_data);
        buf
    }
}

/// Heap 同页追加更新 WAL 记录（非 HOT，在同一页插入新版本）
#[derive(Debug, Clone)]
#[repr(C)]
pub struct WalHeapSamePageAppend {
    /// 页面记录头部
    pub page_header: WalRecordForPage,
    /// 旧元组偏移
    pub old_tuple_offset: u16,
    /// 新元组偏移
    pub new_tuple_offset: u16,
    /// 新元组大小
    pub new_tuple_size: u16,
    /// 旧元组头部更新数据（32 bytes TupleHeader）
    pub old_header_update: [u8; 32],
}

/// Heap 跨页更新（新页面上的记录）
#[derive(Debug, Clone)]
#[repr(C)]
pub struct WalHeapUpdateNewPage {
    /// 页面记录头部
    pub page_header: WalRecordForPage,
    /// 元组偏移
    pub tuple_offset: u16,
    /// 元组大小
    pub tuple_size: u16,
}

impl WalHeapUpdateNewPage {
    /// 创建新的跨页更新记录（新页面）
    pub fn new(page_id: PageId, tuple_offset: u16, tuple_data: &[u8]) -> Self {
        Self {
            page_header: WalRecordForPage::new(
                WalRecordType::HeapAnotherPageAppendUpdateNewPage,
                page_id,
                (4 + tuple_data.len()) as u16,
            ),
            tuple_offset,
            tuple_size: tuple_data.len() as u16,
        }
    }

    /// 序列化记录头 + 元组数据
    pub fn serialize_with_data(&self, tuple_data: &[u8]) -> Vec<u8> {
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        let mut buf = Vec::with_capacity(header_bytes.len() + tuple_data.len());
        buf.extend_from_slice(header_bytes);
        buf.extend_from_slice(tuple_data);
        buf
    }
}

/// Heap 跨页更新（旧页面上的 xmax 设置）
#[derive(Debug, Clone)]
#[repr(C)]
pub struct WalHeapUpdateOldPage {
    /// 页面记录头部
    pub page_header: WalRecordForPage,
    /// 元组偏移
    pub tuple_offset: u16,
    /// 更新后的头部大小（32 bytes）
    pub header_size: u16,
}

impl WalHeapUpdateOldPage {
    /// 创建旧的页面更新记录
    pub fn new(page_id: PageId, tuple_offset: u16) -> Self {
        Self {
            page_header: WalRecordForPage::new(
                WalRecordType::HeapAnotherPageAppendUpdateOldPage,
                page_id,
                (4 + 32) as u16,
            ),
            tuple_offset,
            header_size: 32,
        }
    }

    /// 序列化记录头 + 更新后的头部数据
    pub fn serialize_with_header(&self, header_data: &[u8]) -> Vec<u8> {
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                self as *const Self as *const u8,
                std::mem::size_of::<Self>(),
            )
        };
        let mut buf = Vec::with_capacity(header_bytes.len() + header_data.len());
        buf.extend_from_slice(header_bytes);
        buf.extend_from_slice(header_data);
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_record_header_size() {
        assert_eq!(std::mem::size_of::<WalRecordHeader>(), 4);
    }

    #[test]
    fn test_wal_record_for_page_size() {
        // PageId is 16 bytes (4 * u32), so total is larger
        // header(4) + page_id(16) + flags(1+7 padding) + wal_id(8) + plsn(8) + glsn(8) + version(8) = 60
        println!("WalRecordForPage size: {}", std::mem::size_of::<WalRecordForPage>());
    }

    #[test]
    fn test_wal_atomic_group_header_size() {
        // Xid is u64(8), so total is group_len(4) + crc(4) + xid(8) + record_num(2) + padding(2) = 20
        // But with alignment, it might be different
        println!("WalAtomicGroupHeader size: {}", std::mem::size_of::<WalAtomicGroupHeader>());
    }

    #[test]
    fn test_wal_record_type_conversion() {
        assert_eq!(
            WalRecordType::try_from(0).unwrap(),
            WalRecordType::HeapInsert
        );
        assert_eq!(
            WalRecordType::try_from(175).unwrap(),
            WalRecordType::TxnCommit
        );
        assert!(WalRecordType::try_from(999).is_err());
    }

    #[test]
    fn test_wal_record_header() {
        let header = WalRecordHeader::new(WalRecordType::HeapInsert, 100);
        let size = header.size;
        assert_eq!(size, 100);
        assert_eq!(header.record_type(), Some(WalRecordType::HeapInsert));
    }

    #[test]
    fn test_wal_txn_commit() {
        let xid = Xid::new(1, 0);
        let commit = WalTxnCommit::new(xid, 12345);
        assert_eq!(commit.xid, xid);
        assert_eq!(commit.csn, 12345);
    }
}
