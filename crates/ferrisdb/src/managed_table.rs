//! ManagedTable — 带自动索引维护的表
//!
//! 封装 HeapTable + 关联的 BTree 索引，INSERT/DELETE 时自动更新索引。

use std::sync::Arc;
use ferrisdb_core::{Result, Xid};
use ferrisdb_storage::{BTree, BTreeKey, BTreeValue, BufferPool, WalWriter};
use ferrisdb_transaction::heap::{HeapTable, TupleId};
use ferrisdb_transaction::transaction::{Transaction, TransactionManager};

/// 索引定义
///
/// 支持表达式索引：`key_extractor` 可以是任意函数，如 `|data| lower(col)` 等。
pub struct IndexDef {
    /// 索引名称
    pub name: String,
    /// BTree 实例
    pub btree: BTree,
    /// 从 tuple 数据提取 key 的函数（支持表达式索引）
    pub key_extractor: Box<dyn Fn(&[u8]) -> BTreeKey + Send + Sync>,
    /// 是否唯一约束
    pub unique: bool,
    /// 是否表达式索引（true = key 由函数计算，非直接列值）
    pub is_expression: bool,
}

/// 带自动索引维护的表
pub struct ManagedTable {
    /// 底层堆表
    heap: HeapTable,
    /// 关联的索引
    indexes: Vec<IndexDef>,
}

impl ManagedTable {
    /// 创建 ManagedTable
    pub fn new(heap: HeapTable) -> Self {
        Self { heap, indexes: Vec::new() }
    }

    /// 添加索引
    pub fn add_index(&mut self, def: IndexDef) {
        self.indexes.push(def);
    }

    /// 获取底层堆表
    pub fn heap(&self) -> &HeapTable { &self.heap }

    /// 插入行 — 自动更新所有索引
    pub fn insert(&self, data: &[u8], xid: Xid, cmin: u32) -> Result<TupleId> {
        let tid = self.heap.insert(data, xid, cmin)?;

        // 更新所有索引
        for idx in &self.indexes {
            let key = (idx.key_extractor)(data);
            let value = BTreeValue::Tuple { block: tid.ip_blkid, offset: tid.ip_posid };
            if idx.unique {
                idx.btree.insert_unique(key, value)?;
            } else {
                idx.btree.insert(key, value)?;
            }
        }

        Ok(tid)
    }

    /// 插入行（带 undo 支持）— 自动更新索引
    pub fn insert_with_undo(&self, data: &[u8], xid: Xid, cmin: u32, txn: Option<&mut Transaction>) -> Result<TupleId> {
        let tid = self.heap.insert_with_undo(data, xid, cmin, txn)?;

        for idx in &self.indexes {
            let key = (idx.key_extractor)(data);
            let value = BTreeValue::Tuple { block: tid.ip_blkid, offset: tid.ip_posid };
            if idx.unique {
                idx.btree.insert_unique(key, value)?;
            } else {
                idx.btree.insert(key, value)?;
            }
        }

        Ok(tid)
    }

    /// 删除行 — 自动从索引中移除
    pub fn delete(&self, tid: TupleId, data: &[u8], xid: Xid, cmax: u32) -> Result<()> {
        self.heap.delete(tid, xid, cmax)?;

        // 从所有索引中删除
        for idx in &self.indexes {
            let key = (idx.key_extractor)(data);
            let _ = idx.btree.delete(&key);
        }

        Ok(())
    }

    /// 通过索引查找 — 返回 (TupleId, data)
    pub fn lookup_by_index(&self, index_idx: usize, key: &BTreeKey) -> Result<Option<(TupleId, Vec<u8>)>> {
        if index_idx >= self.indexes.len() {
            return Ok(None);
        }
        let idx = &self.indexes[index_idx];
        match idx.btree.lookup(key)? {
            Some(BTreeValue::Tuple { block, offset }) => {
                let tid = TupleId::new(block, offset);
                match self.heap.fetch(tid)? {
                    Some((_, data)) => Ok(Some((tid, data))),
                    None => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    /// 索引数量
    pub fn index_count(&self) -> usize { self.indexes.len() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrisdb_storage::BufferPoolConfig;

    fn setup() -> (Arc<BufferPool>, Arc<TransactionManager>) {
        let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(500)).unwrap());
        let mut tm = TransactionManager::new(64);
        tm.set_buffer_pool(Arc::clone(&bp));
        (bp, Arc::new(tm))
    }

    #[test]
    fn test_managed_insert_with_index() {
        let (bp, tm) = setup();
        let heap = HeapTable::new(1, Arc::clone(&bp), Arc::clone(&tm));
        let mut mt = ManagedTable::new(heap);

        let btree = BTree::new(100, Arc::clone(&bp));
        btree.init().unwrap();
        mt.add_index(IndexDef {
            name: "pk".to_string(),
            btree,
            key_extractor: Box::new(|data| BTreeKey::new(data[0..4].to_vec())),
            unique: true,
            is_expression: false,
        });

        let data = b"key1_rest_of_data";
        let tid = mt.insert(data, Xid::new(0, 1), 0).unwrap();

        // Verify via index lookup
        let key = BTreeKey::new(b"key1".to_vec());
        let result = mt.lookup_by_index(0, &key).unwrap();
        assert!(result.is_some());
    }

    #[test]
    fn test_managed_delete_removes_from_index() {
        let (bp, tm) = setup();
        let heap = HeapTable::new(2, Arc::clone(&bp), Arc::clone(&tm));
        let mut mt = ManagedTable::new(heap);

        let btree = BTree::new(101, Arc::clone(&bp));
        btree.init().unwrap();
        mt.add_index(IndexDef {
            name: "pk".to_string(),
            btree,
            key_extractor: Box::new(|data| BTreeKey::new(data[0..4].to_vec())),
            unique: false,
            is_expression: false,
        });

        let data = b"abcd_rest";
        let tid = mt.insert(data, Xid::new(0, 1), 0).unwrap();
        mt.delete(tid, data, Xid::new(0, 2), 0).unwrap();

        let key = BTreeKey::new(b"abcd".to_vec());
        let result = mt.lookup_by_index(0, &key).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_managed_unique_constraint() {
        let (bp, tm) = setup();
        let heap = HeapTable::new(3, Arc::clone(&bp), Arc::clone(&tm));
        let mut mt = ManagedTable::new(heap);

        let btree = BTree::new(102, Arc::clone(&bp));
        btree.init().unwrap();
        mt.add_index(IndexDef {
            name: "uk".to_string(),
            btree,
            key_extractor: Box::new(|data| BTreeKey::new(data[0..4].to_vec())),
            unique: true,
            is_expression: false,
        });

        mt.insert(b"key1_data", Xid::new(0, 1), 0).unwrap();
        // Duplicate should fail
        assert!(mt.insert(b"key1_other", Xid::new(0, 2), 0).is_err());
    }

    #[test]
    fn test_managed_multiple_indexes() {
        let (bp, tm) = setup();
        let heap = HeapTable::new(4, Arc::clone(&bp), Arc::clone(&tm));
        let mut mt = ManagedTable::new(heap);

        let bt1 = BTree::new(103, Arc::clone(&bp)); bt1.init().unwrap();
        let bt2 = BTree::new(104, Arc::clone(&bp)); bt2.init().unwrap();
        mt.add_index(IndexDef { name: "idx1".to_string(), btree: bt1, key_extractor: Box::new(|d| BTreeKey::new(d[0..2].to_vec())), unique: false, is_expression: false });
        mt.add_index(IndexDef { name: "idx2".to_string(), btree: bt2, key_extractor: Box::new(|d| BTreeKey::new(d[2..4].to_vec())), unique: false, is_expression: false });
        assert_eq!(mt.index_count(), 2);

        mt.insert(b"aabb_data", Xid::new(0, 1), 0).unwrap();
        assert!(mt.lookup_by_index(0, &BTreeKey::new(b"aa".to_vec())).unwrap().is_some());
        assert!(mt.lookup_by_index(1, &BTreeKey::new(b"bb".to_vec())).unwrap().is_some());
    }

    #[test]
    fn test_managed_no_indexes() {
        let (bp, tm) = setup();
        let heap = HeapTable::new(5, bp, tm);
        let mt = ManagedTable::new(heap);
        mt.insert(b"no_index", Xid::new(0, 1), 0).unwrap();
        assert_eq!(mt.index_count(), 0);
    }
}
