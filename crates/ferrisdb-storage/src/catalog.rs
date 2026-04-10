//! System Catalog — 持久化的系统表元数据管理
//!
//! 管理表、索引等数据库对象的元数据，支持持久化到磁盘文件。
//!
//! 等价于 PostgreSQL/openGauss 的 pg_class + pg_attribute：
//! - RelationMeta = pg_class（关系级：表名、OID、页面数）
//! - ColumnDef = pg_attribute（列级：列名、类型、可空、位置）

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use parking_lot::RwLock;
use ferrisdb_core::{Result, FerrisDBError};

// ============================================================
// 数据类型定义（等价于 pg_type 的核心子集）
// ============================================================

/// 列数据类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum DataType {
    /// 32 位有符号整数
    Int32 = 0,
    /// 64 位有符号整数
    Int64 = 1,
    /// 64 位浮点数
    Float64 = 2,
    /// 变长 UTF-8 文本
    Text = 3,
    /// 布尔值
    Boolean = 4,
    /// 变长字节数组
    Bytes = 5,
    /// 时间戳（微秒精度，UTC epoch）
    Timestamp = 6,
    /// 日期（天数，epoch 1970-01-01 起算）
    Date = 7,
    /// 32 位浮点数
    Float32 = 8,
    /// 16 位有符号整数
    Int16 = 9,
}

impl DataType {
    /// 从 u8 反序列化
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::Int32),
            1 => Some(Self::Int64),
            2 => Some(Self::Float64),
            3 => Some(Self::Text),
            4 => Some(Self::Boolean),
            5 => Some(Self::Bytes),
            6 => Some(Self::Timestamp),
            7 => Some(Self::Date),
            8 => Some(Self::Float32),
            9 => Some(Self::Int16),
            _ => None,
        }
    }

    /// 固定长度类型的字节大小（变长类型返回 None）
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::Int32 => Some(4),
            Self::Int64 => Some(8),
            Self::Float64 => Some(8),
            Self::Boolean => Some(1),
            Self::Timestamp => Some(8),
            Self::Date => Some(4),
            Self::Float32 => Some(4),
            Self::Int16 => Some(2),
            Self::Text | Self::Bytes => None,
        }
    }

    /// 类型名称（SQL 显示用）
    pub fn sql_name(&self) -> &'static str {
        match self {
            Self::Int32 => "INT",
            Self::Int64 => "BIGINT",
            Self::Float64 => "DOUBLE",
            Self::Text => "TEXT",
            Self::Boolean => "BOOLEAN",
            Self::Bytes => "BYTEA",
            Self::Timestamp => "TIMESTAMP",
            Self::Date => "DATE",
            Self::Float32 => "REAL",
            Self::Int16 => "SMALLINT",
        }
    }
}

// ============================================================
// 列定义（等价于 pg_attribute）
// ============================================================

/// 列定义
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    /// 列名
    pub name: String,
    /// 数据类型
    pub data_type: DataType,
    /// 是否允许 NULL
    pub nullable: bool,
    /// 列在表中的位置（0-based）
    pub position: u16,
}

impl ColumnDef {
    /// 创建列定义
    pub fn new(name: &str, data_type: DataType, nullable: bool, position: u16) -> Self {
        Self {
            name: name.to_string(),
            data_type,
            nullable,
            position,
        }
    }

    /// 序列化为字节
    fn to_bytes(&self) -> Vec<u8> {
        let name_bytes = self.name.as_bytes();
        let mut buf = Vec::with_capacity(6 + name_bytes.len());
        buf.push(self.data_type as u8);
        buf.push(if self.nullable { 1 } else { 0 });
        buf.extend_from_slice(&self.position.to_le_bytes());
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        buf
    }

    /// 从字节反序列化
    fn from_bytes(data: &[u8]) -> Option<(Self, usize)> {
        if data.len() < 6 { return None; }
        let data_type = DataType::from_u8(data[0])?;
        let nullable = data[1] != 0;
        let position = u16::from_le_bytes(data[2..4].try_into().ok()?);
        let name_len = u16::from_le_bytes(data[4..6].try_into().ok()?) as usize;
        if data.len() < 6 + name_len { return None; }
        let name = String::from_utf8(data[6..6 + name_len].to_vec()).ok()?;
        Some((Self { name, data_type, nullable, position }, 6 + name_len))
    }
}

/// Relation 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum RelationType {
    /// 堆表
    Table = 0,
    /// 索引
    Index = 1,
    /// 系统表
    System = 2,
}

/// Relation 元数据（可持久化）
///
/// 等价于 pg_class 的核心字段 + 内联 pg_attribute 列定义。
#[derive(Debug, Clone)]
pub struct RelationMeta {
    /// Relation OID
    pub oid: u32,
    /// 名称
    pub name: String,
    /// 类型
    pub rel_type: RelationType,
    /// 表空间 ID
    pub tablespace_id: u32,
    /// 所属表 OID（索引用）
    pub owner_table: u32,
    /// 当前页面数（Heap: 已分配页数, Index: root page 号）
    pub current_pages: u32,
    /// 索引 root page（仅 Index 类型使用）
    pub root_page: u32,
    /// 列定义（等价于 pg_attribute）— 仅 Table 类型有意义
    pub columns: Vec<ColumnDef>,
}

impl RelationMeta {
    /// 序列化为字节
    ///
    /// 格式: [oid:4][type:1][tablespace:4][owner:4][pages:4][root:4]
    ///        [name_len:2][name][col_count:2][col1_bytes][col2_bytes]...
    ///
    /// 向后兼容：旧格式无 col_count 字段，from_bytes 检测剩余长度判断。
    fn to_bytes(&self) -> Vec<u8> {
        let name_bytes = self.name.as_bytes();
        let mut buf = Vec::with_capacity(32 + name_bytes.len());
        buf.extend_from_slice(&self.oid.to_le_bytes());
        buf.push(self.rel_type as u8);
        buf.extend_from_slice(&self.tablespace_id.to_le_bytes());
        buf.extend_from_slice(&self.owner_table.to_le_bytes());
        buf.extend_from_slice(&self.current_pages.to_le_bytes());
        buf.extend_from_slice(&self.root_page.to_le_bytes());
        buf.extend_from_slice(&(name_bytes.len() as u16).to_le_bytes());
        buf.extend_from_slice(name_bytes);
        // 列定义（新增，向后兼容）
        buf.extend_from_slice(&(self.columns.len() as u16).to_le_bytes());
        for col in &self.columns {
            buf.extend_from_slice(&col.to_bytes());
        }
        buf
    }

    /// 从字节反序列化（向后兼容：无列定义的旧格式也能读）
    fn from_bytes(data: &[u8]) -> Option<(Self, usize)> {
        if data.len() < 23 { return None; }
        let oid = u32::from_le_bytes(data[0..4].try_into().ok()?);
        let rel_type = match data[4] {
            0 => RelationType::Table,
            1 => RelationType::Index,
            2 => RelationType::System,
            _ => return None,
        };
        let tablespace_id = u32::from_le_bytes(data[5..9].try_into().ok()?);
        let owner_table = u32::from_le_bytes(data[9..13].try_into().ok()?);
        let current_pages = u32::from_le_bytes(data[13..17].try_into().ok()?);
        let root_page = u32::from_le_bytes(data[17..21].try_into().ok()?);
        let name_len = u16::from_le_bytes(data[21..23].try_into().ok()?) as usize;
        if data.len() < 23 + name_len { return None; }
        let name = String::from_utf8(data[23..23 + name_len].to_vec()).ok()?;
        let mut offset = 23 + name_len;

        // 列定义（新格式才有）
        let mut columns = Vec::new();
        if offset + 2 <= data.len() {
            let col_count = u16::from_le_bytes(data[offset..offset+2].try_into().ok()?) as usize;
            offset += 2;
            for _ in 0..col_count {
                if offset >= data.len() { break; }
                if let Some((col, consumed)) = ColumnDef::from_bytes(&data[offset..]) {
                    columns.push(col);
                    offset += consumed;
                } else {
                    break;
                }
            }
        }

        Some((Self { oid, name, rel_type, tablespace_id, owner_table, current_pages, root_page, columns }, offset))
    }
}

/// 系统目录（可持久化）
pub struct SystemCatalog {
    /// relation OID → 元数据
    relations: RwLock<HashMap<u32, RelationMeta>>,
    /// 名称 → OID 索引
    name_index: RwLock<HashMap<String, u32>>,
    /// 下一个 OID
    next_oid: std::sync::atomic::AtomicU32,
    /// 持久化文件路径（None = 纯内存模式）
    file_path: Option<PathBuf>,
}

impl SystemCatalog {
    /// 创建纯内存目录（测试用）
    pub fn new() -> Self {
        Self {
            relations: RwLock::new(HashMap::new()),
            name_index: RwLock::new(HashMap::new()),
            next_oid: std::sync::atomic::AtomicU32::new(10000),
            file_path: None,
        }
    }

    /// 打开持久化目录（从文件加载，不存在则创建空目录）
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let path = data_dir.as_ref().join("ferrisdb_catalog");
        let mut catalog = Self {
            relations: RwLock::new(HashMap::new()),
            name_index: RwLock::new(HashMap::new()),
            next_oid: std::sync::atomic::AtomicU32::new(10000),
            file_path: Some(path.clone()),
        };
        if path.exists() {
            let data = std::fs::read(&path)
                .map_err(|e| FerrisDBError::Internal(format!("Failed to read catalog: {}", e)))?;
            catalog.load_from_bytes(&data)?;
        }
        Ok(catalog)
    }

    /// 从字节加载
    fn load_from_bytes(&mut self, data: &[u8]) -> Result<()> {
        if data.len() < 4 { return Ok(()); }
        let count = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0; 4])) as usize;
        let max_oid_bytes = if data.len() >= 8 { u32::from_le_bytes(data[4..8].try_into().unwrap_or([0;4])) } else { 10000 };
        self.next_oid.store(max_oid_bytes, std::sync::atomic::Ordering::Release);

        let mut offset = 8;
        let mut rels = self.relations.write();
        let mut names = self.name_index.write();
        for _ in 0..count {
            if offset >= data.len() { break; }
            if let Some((meta, consumed)) = RelationMeta::from_bytes(&data[offset..]) {
                names.insert(meta.name.clone(), meta.oid);
                rels.insert(meta.oid, meta);
                offset += consumed;
            } else {
                break;
            }
        }
        Ok(())
    }

    /// 持久化到磁盘
    pub fn persist(&self) -> Result<()> {
        let path = match &self.file_path {
            Some(p) => p,
            None => return Ok(()), // 纯内存模式
        };
        let rels = self.relations.read();
        let count = rels.len() as u32;
        let next_oid = self.next_oid.load(std::sync::atomic::Ordering::Acquire);

        let mut buf = Vec::new();
        buf.extend_from_slice(&count.to_le_bytes());
        buf.extend_from_slice(&next_oid.to_le_bytes());
        for meta in rels.values() {
            buf.extend_from_slice(&meta.to_bytes());
        }

        let tmp = path.with_extension("tmp");
        std::fs::write(&tmp, &buf)
            .map_err(|e| FerrisDBError::Internal(format!("Failed to write catalog: {}", e)))?;
        std::fs::rename(&tmp, path)
            .map_err(|e| FerrisDBError::Internal(format!("Failed to rename catalog: {}", e)))?;
        Ok(())
    }

    /// 创建表（无 Schema — 向后兼容）
    pub fn create_table(&self, name: &str, tablespace_id: u32) -> Result<u32> {
        self.create_table_with_schema(name, tablespace_id, vec![])
    }

    /// 创建表（带列定义 Schema）
    pub fn create_table_with_schema(&self, name: &str, tablespace_id: u32, columns: Vec<ColumnDef>) -> Result<u32> {
        // 检查重名
        if self.name_index.read().contains_key(name) {
            return Err(FerrisDBError::Internal(format!("Table '{}' already exists", name)));
        }
        let oid = self.next_oid.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let meta = RelationMeta {
            oid, name: name.to_string(), rel_type: RelationType::Table,
            tablespace_id, owner_table: 0, current_pages: 0, root_page: u32::MAX,
            columns,
        };
        self.relations.write().insert(oid, meta);
        self.name_index.write().insert(name.to_string(), oid);
        self.persist()?;
        Ok(oid)
    }

    /// 创建索引
    pub fn create_index(&self, name: &str, table_oid: u32, tablespace_id: u32) -> Result<u32> {
        if !self.relations.read().contains_key(&table_oid) {
            return Err(FerrisDBError::Internal("Table not found".to_string()));
        }
        let oid = self.next_oid.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let meta = RelationMeta {
            oid, name: name.to_string(), rel_type: RelationType::Index,
            tablespace_id, owner_table: table_oid, current_pages: 0, root_page: u32::MAX,
            columns: vec![],
        };
        self.relations.write().insert(oid, meta);
        self.name_index.write().insert(name.to_string(), oid);
        self.persist()?;
        Ok(oid)
    }

    /// 更新 relation 的页面计数（Heap current_page / Index root_page）
    pub fn update_pages(&self, oid: u32, current_pages: u32, root_page: u32) -> Result<()> {
        let mut rels = self.relations.write();
        if let Some(meta) = rels.get_mut(&oid) {
            meta.current_pages = current_pages;
            meta.root_page = root_page;
        }
        drop(rels);
        self.persist()?;
        Ok(())
    }

    /// 按名称查找
    pub fn lookup_by_name(&self, name: &str) -> Option<RelationMeta> {
        let oid = *self.name_index.read().get(name)?;
        self.relations.read().get(&oid).cloned()
    }

    /// 按 OID 查找
    pub fn lookup_by_oid(&self, oid: u32) -> Option<RelationMeta> {
        self.relations.read().get(&oid).cloned()
    }

    /// 删除
    pub fn drop_relation(&self, oid: u32) -> Result<()> {
        let meta = self.relations.write().remove(&oid)
            .ok_or_else(|| FerrisDBError::Internal("Relation not found".to_string()))?;
        self.name_index.write().remove(&meta.name);
        self.persist()?;
        Ok(())
    }

    /// 列出所有表
    pub fn list_tables(&self) -> Vec<RelationMeta> {
        self.relations.read().values()
            .filter(|m| m.rel_type == RelationType::Table)
            .cloned().collect()
    }

    /// 列出表的索引
    pub fn list_indexes(&self, table_oid: u32) -> Vec<RelationMeta> {
        self.relations.read().values()
            .filter(|m| m.rel_type == RelationType::Index && m.owner_table == table_oid)
            .cloned().collect()
    }

    /// 总 relation 数
    pub fn count(&self) -> usize {
        self.relations.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test] fn test_create_table() { let c = SystemCatalog::new(); let oid = c.create_table("users", 1).unwrap(); assert!(oid >= 10000); }
    #[test] fn test_lookup_by_name() { let c = SystemCatalog::new(); c.create_table("orders", 1).unwrap(); let m = c.lookup_by_name("orders").unwrap(); assert_eq!(m.name, "orders"); }
    #[test] fn test_lookup_nonexistent() { let c = SystemCatalog::new(); assert!(c.lookup_by_name("ghost").is_none()); }
    #[test] fn test_create_index() { let c = SystemCatalog::new(); let t = c.create_table("t", 1).unwrap(); let i = c.create_index("t_idx", t, 1).unwrap(); assert_eq!(c.lookup_by_oid(i).unwrap().rel_type, RelationType::Index); }
    #[test] fn test_drop_relation() { let c = SystemCatalog::new(); let oid = c.create_table("drop_me", 1).unwrap(); c.drop_relation(oid).unwrap(); assert!(c.lookup_by_name("drop_me").is_none()); }
    #[test] fn test_list_tables() { let c = SystemCatalog::new(); c.create_table("a", 1).unwrap(); c.create_table("b", 1).unwrap(); assert_eq!(c.list_tables().len(), 2); }
    #[test] fn test_count() { let c = SystemCatalog::new(); c.create_table("a", 1).unwrap(); assert_eq!(c.count(), 1); }

    // ===== 持久化测试 =====
    #[test]
    fn test_persist_and_reload() {
        let td = TempDir::new().unwrap();
        {
            let c = SystemCatalog::open(td.path()).unwrap();
            c.create_table("users", 0).unwrap();
            c.create_table("orders", 0).unwrap();
            c.update_pages(c.lookup_by_name("users").unwrap().oid, 42, u32::MAX).unwrap();
        }
        {
            let c = SystemCatalog::open(td.path()).unwrap();
            assert_eq!(c.count(), 2);
            let users = c.lookup_by_name("users").unwrap();
            assert_eq!(users.current_pages, 42);
            assert!(c.lookup_by_name("orders").is_some());
        }
    }

    #[test]
    fn test_persist_index() {
        let td = TempDir::new().unwrap();
        {
            let c = SystemCatalog::open(td.path()).unwrap();
            let t = c.create_table("t", 0).unwrap();
            let i = c.create_index("t_pk", t, 0).unwrap();
            c.update_pages(i, 0, 5).unwrap(); // root_page = 5
        }
        {
            let c = SystemCatalog::open(td.path()).unwrap();
            let idx = c.lookup_by_name("t_pk").unwrap();
            assert_eq!(idx.rel_type, RelationType::Index);
            assert_eq!(idx.root_page, 5);
        }
    }

    #[test]
    fn test_persist_drop() {
        let td = TempDir::new().unwrap();
        {
            let c = SystemCatalog::open(td.path()).unwrap();
            c.create_table("temp", 0).unwrap();
            c.drop_relation(c.lookup_by_name("temp").unwrap().oid).unwrap();
        }
        {
            let c = SystemCatalog::open(td.path()).unwrap();
            assert_eq!(c.count(), 0);
        }
    }

    #[test]
    fn test_meta_roundtrip() {
        let meta = RelationMeta {
            oid: 42, name: "test_table".to_string(), rel_type: RelationType::Table,
            tablespace_id: 1, owner_table: 0, current_pages: 100, root_page: u32::MAX,
            columns: vec![],
        };
        let bytes = meta.to_bytes();
        let (restored, _) = RelationMeta::from_bytes(&bytes).unwrap();
        assert_eq!(restored.oid, 42);
        assert_eq!(restored.name, "test_table");
        assert_eq!(restored.current_pages, 100);
    }

    #[test] fn test_concurrent_create() { let c = std::sync::Arc::new(SystemCatalog::new()); let mut h = vec![]; for t in 0..4 { let c = c.clone(); h.push(std::thread::spawn(move || { for i in 0..25 { c.create_table(&format!("t_{}_{}", t, i), 1).unwrap(); } })); } for j in h { j.join().unwrap(); } assert_eq!(c.count(), 100); }
    #[test] fn test_update_pages() { let c = SystemCatalog::new(); let oid = c.create_table("t", 0).unwrap(); c.update_pages(oid, 50, 3).unwrap(); let m = c.lookup_by_oid(oid).unwrap(); assert_eq!(m.current_pages, 50); assert_eq!(m.root_page, 3); }
}
