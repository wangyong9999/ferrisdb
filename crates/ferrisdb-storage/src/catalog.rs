//! System Catalog — 系统表元数据管理
//!
//! 管理表、索引、表空间等数据库对象的元数据。

use std::collections::HashMap;
use parking_lot::RwLock;
use ferrisdb_core::{Result, FerrisDBError};

/// Relation 类型
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RelationType {
    /// 堆表
    Table,
    /// 索引
    Index,
    /// 系统表
    System,
}

/// Relation 元数据
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
}

/// 系统目录
pub struct SystemCatalog {
    /// relation OID → 元数据
    relations: RwLock<HashMap<u32, RelationMeta>>,
    /// 名称 → OID 索引
    name_index: RwLock<HashMap<String, u32>>,
    /// 下一个 OID
    next_oid: std::sync::atomic::AtomicU32,
}

impl SystemCatalog {
    /// 创建空目录
    pub fn new() -> Self {
        Self {
            relations: RwLock::new(HashMap::new()),
            name_index: RwLock::new(HashMap::new()),
            next_oid: std::sync::atomic::AtomicU32::new(10000),
        }
    }

    /// 创建表
    pub fn create_table(&self, name: &str, tablespace_id: u32) -> Result<u32> {
        let oid = self.next_oid.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let meta = RelationMeta {
            oid, name: name.to_string(), rel_type: RelationType::Table,
            tablespace_id, owner_table: 0,
        };
        self.relations.write().insert(oid, meta);
        self.name_index.write().insert(name.to_string(), oid);
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
            tablespace_id, owner_table: table_oid,
        };
        self.relations.write().insert(oid, meta);
        self.name_index.write().insert(name.to_string(), oid);
        Ok(oid)
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

    #[test] fn test_create_table() { let c = SystemCatalog::new(); let oid = c.create_table("users", 1).unwrap(); assert!(oid >= 10000); }
    #[test] fn test_lookup_by_name() { let c = SystemCatalog::new(); c.create_table("orders", 1).unwrap(); let m = c.lookup_by_name("orders").unwrap(); assert_eq!(m.name, "orders"); assert_eq!(m.rel_type, RelationType::Table); }
    #[test] fn test_lookup_by_oid() { let c = SystemCatalog::new(); let oid = c.create_table("t1", 1).unwrap(); assert!(c.lookup_by_oid(oid).is_some()); }
    #[test] fn test_lookup_nonexistent() { let c = SystemCatalog::new(); assert!(c.lookup_by_name("ghost").is_none()); }
    #[test] fn test_create_index() { let c = SystemCatalog::new(); let t = c.create_table("t", 1).unwrap(); let i = c.create_index("t_idx", t, 1).unwrap(); let m = c.lookup_by_oid(i).unwrap(); assert_eq!(m.rel_type, RelationType::Index); assert_eq!(m.owner_table, t); }
    #[test] fn test_create_index_no_table() { let c = SystemCatalog::new(); assert!(c.create_index("bad_idx", 999, 1).is_err()); }
    #[test] fn test_drop_relation() { let c = SystemCatalog::new(); let oid = c.create_table("drop_me", 1).unwrap(); c.drop_relation(oid).unwrap(); assert!(c.lookup_by_name("drop_me").is_none()); }
    #[test] fn test_list_tables() { let c = SystemCatalog::new(); c.create_table("a", 1).unwrap(); c.create_table("b", 1).unwrap(); assert_eq!(c.list_tables().len(), 2); }
    #[test] fn test_list_indexes() { let c = SystemCatalog::new(); let t = c.create_table("t", 1).unwrap(); c.create_index("i1", t, 1).unwrap(); c.create_index("i2", t, 1).unwrap(); assert_eq!(c.list_indexes(t).len(), 2); }
    #[test] fn test_count() { let c = SystemCatalog::new(); c.create_table("a", 1).unwrap(); c.create_table("b", 1).unwrap(); assert_eq!(c.count(), 2); }
}
