//! Undo Record
//!
//! 记录用于回滚操作的数据。

use ferrisdb_core::{Csn, Xid};

/// Undo Record 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum UndoRecordType {
    /// 插入
    Insert = 0,
    /// 更新
    Update = 1,
    /// 删除
    Delete = 2,
    /// 2PC 准备
    Prepare = 3,
    /// 2PC 提交
    Commit = 4,
    /// 2PC 回滚
    Rollback = 5,
}

impl Default for UndoRecordType {
    fn default() -> Self {
        Self::Insert
    }
}

/// Undo Record 头部
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct UndoRecordHeader {
    /// 记录类型
    pub record_type: u8,
    /// 标志
    pub flags: u8,
    /// 元数据长度
    pub meta_len: u16,
    /// 数据长度
    pub data_len: u32,
    /// 事务 ID
    pub xid: u64,
    /// CSN
    pub csn: u64,
    /// 保留
    pub _reserved: [u8; 4],
}

impl UndoRecordHeader {
    /// 创建新的头部
    pub const fn new() -> Self {
        Self {
            record_type: UndoRecordType::Insert as u8,
            flags: 0,
            meta_len: 0,
            data_len: 0,
            xid: 0,
            csn: 0,
            _reserved: [0; 4],
        }
    }

    /// 获取记录类型
    pub fn record_type(&self) -> UndoRecordType {
        match self.record_type {
            0 => UndoRecordType::Insert,
            1 => UndoRecordType::Update,
            2 => UndoRecordType::Delete,
            3 => UndoRecordType::Prepare,
            4 => UndoRecordType::Commit,
            5 => UndoRecordType::Rollback,
            _ => UndoRecordType::Insert,
        }
    }

    /// 获取总大小
    pub const fn total_size(&self) -> usize {
        std::mem::size_of::<Self>() + self.meta_len as usize + self.data_len as usize
    }
}

impl Default for UndoRecordHeader {
    fn default() -> Self {
        Self::new()
    }
}

// 编译时验证
const _: () = assert!(std::mem::size_of::<UndoRecordHeader>() == 32);

/// Undo Record
#[derive(Debug, Clone)]
pub struct UndoRecord {
    /// 头部
    pub header: UndoRecordHeader,
    /// 元数据
    pub meta: Vec<u8>,
    /// 数据
    pub data: Vec<u8>,
}

impl UndoRecord {
    /// 创建新的 Insert Undo Record
    pub fn new_insert(xid: Xid, table_oid: u32, tid: (u32, u16)) -> Self {
        let mut record = Self::new(UndoRecordType::Insert);
        record.header.xid = xid.raw();

        // 元数据: table_oid(4) + tid(6)
        record.meta = Vec::with_capacity(10);
        record.meta.extend_from_slice(&table_oid.to_le_bytes());
        record.meta.extend_from_slice(&tid.0.to_le_bytes());
        record.meta.extend_from_slice(&tid.1.to_le_bytes());
        record.header.meta_len = record.meta.len() as u16;

        record
    }

    /// 创建新的 Update Undo Record
    pub fn new_update(xid: Xid, table_oid: u32, old_tid: (u32, u16), new_tid: (u32, u16), old_data: &[u8]) -> Self {
        let mut record = Self::new(UndoRecordType::Update);
        record.header.xid = xid.raw();

        // 元数据: table_oid(4) + old_tid(6) + new_tid(6)
        record.meta = Vec::with_capacity(16);
        record.meta.extend_from_slice(&table_oid.to_le_bytes());
        record.meta.extend_from_slice(&old_tid.0.to_le_bytes());
        record.meta.extend_from_slice(&old_tid.1.to_le_bytes());
        record.meta.extend_from_slice(&new_tid.0.to_le_bytes());
        record.meta.extend_from_slice(&new_tid.1.to_le_bytes());
        record.header.meta_len = record.meta.len() as u16;

        // 旧数据
        record.data = old_data.to_vec();
        record.header.data_len = record.data.len() as u32;

        record
    }

    /// 创建新的 Delete Undo Record
    pub fn new_delete(xid: Xid, table_oid: u32, tid: (u32, u16), old_data: &[u8]) -> Self {
        let mut record = Self::new(UndoRecordType::Delete);
        record.header.xid = xid.raw();

        // 元数据: table_oid(4) + tid(6)
        record.meta = Vec::with_capacity(10);
        record.meta.extend_from_slice(&table_oid.to_le_bytes());
        record.meta.extend_from_slice(&tid.0.to_le_bytes());
        record.meta.extend_from_slice(&tid.1.to_le_bytes());
        record.header.meta_len = record.meta.len() as u16;

        // 旧数据
        record.data = old_data.to_vec();
        record.header.data_len = record.data.len() as u32;

        record
    }

    /// 创建新的事务提交记录
    pub fn new_commit(xid: Xid, csn: Csn) -> Self {
        let mut record = Self::new(UndoRecordType::Commit);
        record.header.xid = xid.raw();
        record.header.csn = csn.raw();
        record
    }

    /// 创建新的记录
    fn new(record_type: UndoRecordType) -> Self {
        Self {
            header: UndoRecordHeader {
                record_type: record_type as u8,
                ..Default::default()
            },
            meta: Vec::new(),
            data: Vec::new(),
        }
    }

    /// 获取记录类型
    pub fn record_type(&self) -> UndoRecordType {
        self.header.record_type()
    }

    /// 获取事务 ID
    pub fn xid(&self) -> Xid {
        Xid::from_raw(self.header.xid)
    }

    /// 获取 CSN
    pub fn csn(&self) -> Csn {
        Csn::from_raw(self.header.csn)
    }

    /// 设置 CSN
    pub fn set_csn(&mut self, csn: Csn) {
        self.header.csn = csn.raw();
    }

    /// 序列化
    pub fn serialize(&self) -> Vec<u8> {
        let total_size = self.header.total_size();
        let mut buf = Vec::with_capacity(total_size);

        // 头部
        buf.extend_from_slice(unsafe {
            std::slice::from_raw_parts(
                &self.header as *const UndoRecordHeader as *const u8,
                std::mem::size_of::<UndoRecordHeader>(),
            )
        });

        // 元数据
        buf.extend_from_slice(&self.meta);

        // 数据
        buf.extend_from_slice(&self.data);

        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_undo_record_header_size() {
        assert_eq!(std::mem::size_of::<UndoRecordHeader>(), 32);
    }

    #[test]
    fn test_undo_record_new_insert() {
        let xid = Xid::new(0, 1);
        let record = UndoRecord::new_insert(xid, 100, (1, 0));

        assert_eq!(record.record_type(), UndoRecordType::Insert);
        assert_eq!(record.xid(), xid);
    }

    #[test]
    fn test_undo_record_new_delete() {
        let xid = Xid::new(0, 1);
        let old_data = vec![1, 2, 3, 4];
        let record = UndoRecord::new_delete(xid, 100, (1, 0), &old_data);

        assert_eq!(record.record_type(), UndoRecordType::Delete);
        assert_eq!(record.data, old_data);
    }

    #[test]
    fn test_undo_record_serialize() {
        let xid = Xid::new(0, 1);
        let record = UndoRecord::new_insert(xid, 100, (1, 0));
        let buf = record.serialize();

        assert!(!buf.is_empty());
        assert!(buf.len() >= 32);  // At least header size
    }
}
