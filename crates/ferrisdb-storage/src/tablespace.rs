//! Tablespace 管理 — 表空间目录和 extent 分配
//!
//! 每个表空间是一个磁盘目录，内含多个 relation 的数据文件。
//! Extent 是连续页面的分配单位（默认 8 pages = 64KB）。

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use parking_lot::RwLock;
use ferrisdb_core::{Result, FerrisDBError};

/// 默认 extent 大小（页面数）
pub const DEFAULT_EXTENT_SIZE: u32 = 8;

/// 表空间
pub struct Tablespace {
    /// 表空间 ID
    id: u32,
    /// 目录路径
    path: PathBuf,
    /// 每个 relation 的当前 extent 数
    relation_extents: RwLock<HashMap<u32, u32>>,
    /// Extent 大小（页面数）
    extent_size: u32,
}

impl Tablespace {
    /// 创建或打开表空间
    pub fn open(id: u32, path: &Path, extent_size: u32) -> Result<Self> {
        std::fs::create_dir_all(path)
            .map_err(|e| FerrisDBError::Internal(format!("Failed to create tablespace dir: {}", e)))?;
        Ok(Self {
            id,
            path: path.to_path_buf(),
            relation_extents: RwLock::new(HashMap::new()),
            extent_size: if extent_size == 0 { DEFAULT_EXTENT_SIZE } else { extent_size },
        })
    }

    /// 表空间 ID
    pub fn id(&self) -> u32 { self.id }

    /// 表空间路径
    pub fn path(&self) -> &Path { &self.path }

    /// Extent 大小
    pub fn extent_size(&self) -> u32 { self.extent_size }

    /// 为 relation 分配一个新 extent，返回起始页号
    pub fn allocate_extent(&self, relation_id: u32) -> u32 {
        let mut extents = self.relation_extents.write();
        let current = extents.entry(relation_id).or_insert(0);
        let start_page = *current * self.extent_size;
        *current += 1;
        start_page
    }

    /// 获取 relation 的 extent 数
    pub fn extent_count(&self, relation_id: u32) -> u32 {
        self.relation_extents.read().get(&relation_id).copied().unwrap_or(0)
    }

    /// 获取 relation 的总页面数
    pub fn total_pages(&self, relation_id: u32) -> u32 {
        self.extent_count(relation_id) * self.extent_size
    }

    /// 列出所有 relation
    pub fn relations(&self) -> Vec<u32> {
        self.relation_extents.read().keys().copied().collect()
    }

    /// relation 数据文件路径
    pub fn relation_path(&self, relation_id: u32) -> PathBuf {
        self.path.join(format!("rel_{}", relation_id))
    }
}

/// 表空间管理器 — 管理多个表空间
pub struct TablespaceManager {
    tablespaces: RwLock<HashMap<u32, Tablespace>>,
    base_dir: PathBuf,
}

impl TablespaceManager {
    /// 创建管理器
    pub fn new(base_dir: &Path) -> Self {
        Self {
            tablespaces: RwLock::new(HashMap::new()),
            base_dir: base_dir.to_path_buf(),
        }
    }

    /// 创建表空间
    pub fn create_tablespace(&self, id: u32) -> Result<()> {
        let path = self.base_dir.join(format!("ts_{}", id));
        let ts = Tablespace::open(id, &path, DEFAULT_EXTENT_SIZE)?;
        self.tablespaces.write().insert(id, ts);
        Ok(())
    }

    /// 获取表空间
    pub fn get_tablespace(&self, id: u32) -> Option<u32> {
        if self.tablespaces.read().contains_key(&id) { Some(id) } else { None }
    }

    /// 分配 extent
    pub fn allocate_extent(&self, tablespace_id: u32, relation_id: u32) -> Result<u32> {
        let ts = self.tablespaces.read();
        let t = ts.get(&tablespace_id)
            .ok_or_else(|| FerrisDBError::Internal("Tablespace not found".to_string()))?;
        Ok(t.allocate_extent(relation_id))
    }

    /// 列出所有表空间 ID
    pub fn list(&self) -> Vec<u32> {
        self.tablespaces.read().keys().copied().collect()
    }
}

/// Data Segment — relation 级别的存储管理
///
/// 管理一个 relation 的 extent chain + 空闲页面追踪。
pub struct DataSegment {
    /// Relation OID
    relation_id: u32,
    /// Extent chain: 按分配顺序的 extent 起始页号
    extent_chain: RwLock<Vec<u32>>,
    /// 每个 extent 的大小
    extent_size: u32,
    /// 空闲页面 bitmap（每 extent 一个 Vec<bool>）
    free_bitmap: RwLock<Vec<Vec<bool>>>,
}

impl DataSegment {
    /// 创建 segment
    pub fn new(relation_id: u32, extent_size: u32) -> Self {
        Self {
            relation_id,
            extent_chain: RwLock::new(Vec::new()),
            extent_size: if extent_size == 0 { DEFAULT_EXTENT_SIZE } else { extent_size },
            free_bitmap: RwLock::new(Vec::new()),
        }
    }

    /// Relation OID
    pub fn relation_id(&self) -> u32 { self.relation_id }

    /// 添加 extent
    pub fn add_extent(&self, start_page: u32) {
        self.extent_chain.write().push(start_page);
        self.free_bitmap.write().push(vec![true; self.extent_size as usize]);
    }

    /// Extent 数量
    pub fn extent_count(&self) -> usize { self.extent_chain.read().len() }

    /// 总页面数
    pub fn total_pages(&self) -> u32 { self.extent_count() as u32 * self.extent_size }

    /// 分配一个空闲页面，返回绝对页号
    pub fn allocate_page(&self) -> Option<u32> {
        let chain = self.extent_chain.read();
        let mut bitmap = self.free_bitmap.write();
        for (ext_idx, bm) in bitmap.iter_mut().enumerate() {
            for (page_idx, free) in bm.iter_mut().enumerate() {
                if *free {
                    *free = false;
                    return Some(chain[ext_idx] + page_idx as u32);
                }
            }
        }
        None
    }

    /// 释放页面
    pub fn free_page(&self, abs_page: u32) {
        let chain = self.extent_chain.read();
        let mut bitmap = self.free_bitmap.write();
        for (ext_idx, &start) in chain.iter().enumerate() {
            if abs_page >= start && abs_page < start + self.extent_size {
                let page_idx = (abs_page - start) as usize;
                if ext_idx < bitmap.len() && page_idx < bitmap[ext_idx].len() {
                    bitmap[ext_idx][page_idx] = true;
                }
                return;
            }
        }
    }

    /// 空闲页面数
    pub fn free_page_count(&self) -> u32 {
        self.free_bitmap.read().iter()
            .flat_map(|bm| bm.iter())
            .filter(|&&f| f)
            .count() as u32
    }

    /// Extent chain（起始页号列表）
    pub fn extent_starts(&self) -> Vec<u32> { self.extent_chain.read().clone() }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test] fn test_tablespace_create() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 8).unwrap(); assert_eq!(ts.id(), 1); }
    #[test] fn test_tablespace_extent_size() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 16).unwrap(); assert_eq!(ts.extent_size(), 16); }
    #[test] fn test_tablespace_default_extent() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 0).unwrap(); assert_eq!(ts.extent_size(), DEFAULT_EXTENT_SIZE); }
    #[test] fn test_allocate_extent() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 8).unwrap(); let p1 = ts.allocate_extent(100); let p2 = ts.allocate_extent(100); assert_eq!(p1, 0); assert_eq!(p2, 8); }
    #[test] fn test_extent_count() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 8).unwrap(); ts.allocate_extent(100); ts.allocate_extent(100); assert_eq!(ts.extent_count(100), 2); }
    #[test] fn test_total_pages() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 8).unwrap(); ts.allocate_extent(100); ts.allocate_extent(100); assert_eq!(ts.total_pages(100), 16); }
    #[test] fn test_multiple_relations() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 8).unwrap(); ts.allocate_extent(100); ts.allocate_extent(200); ts.allocate_extent(300); assert_eq!(ts.relations().len(), 3); }
    #[test] fn test_relation_path() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 8).unwrap(); let p = ts.relation_path(42); assert!(p.to_string_lossy().contains("rel_42")); }
    #[test] fn test_manager_create() { let td = TempDir::new().unwrap(); let mgr = TablespaceManager::new(td.path()); mgr.create_tablespace(1).unwrap(); assert!(mgr.get_tablespace(1).is_some()); }
    #[test] fn test_manager_list() { let td = TempDir::new().unwrap(); let mgr = TablespaceManager::new(td.path()); mgr.create_tablespace(1).unwrap(); mgr.create_tablespace(2).unwrap(); assert_eq!(mgr.list().len(), 2); }
    #[test] fn test_manager_allocate() { let td = TempDir::new().unwrap(); let mgr = TablespaceManager::new(td.path()); mgr.create_tablespace(1).unwrap(); let p = mgr.allocate_extent(1, 100).unwrap(); assert_eq!(p, 0); }
    #[test] fn test_manager_nonexistent() { let td = TempDir::new().unwrap(); let mgr = TablespaceManager::new(td.path()); assert!(mgr.get_tablespace(999).is_none()); }
    #[test] fn test_concurrent_allocate() { let td = TempDir::new().unwrap(); let ts = std::sync::Arc::new(Tablespace::open(1, td.path(), 8).unwrap()); let mut h = vec![]; for _ in 0..4 { let ts = ts.clone(); h.push(std::thread::spawn(move || { for _ in 0..50 { ts.allocate_extent(100); } })); } for j in h { j.join().unwrap(); } assert_eq!(ts.extent_count(100), 200); }
    #[test] fn test_large_extent() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 1024).unwrap(); ts.allocate_extent(1); assert_eq!(ts.total_pages(1), 1024); }
    #[test] fn test_many_extents() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 8).unwrap(); for _ in 0..100 { ts.allocate_extent(1); } assert_eq!(ts.extent_count(1), 100); }
    #[test] fn test_empty_relations() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 8).unwrap(); assert_eq!(ts.extent_count(999), 0); assert_eq!(ts.total_pages(999), 0); }

    // ===== DataSegment tests =====
    #[test] fn test_seg_new() { let s = DataSegment::new(1, 8); assert_eq!(s.relation_id(), 1); assert_eq!(s.extent_count(), 0); }
    #[test] fn test_seg_add_extent() { let s = DataSegment::new(1, 8); s.add_extent(0); assert_eq!(s.extent_count(), 1); assert_eq!(s.total_pages(), 8); }
    #[test] fn test_seg_allocate_page() { let s = DataSegment::new(1, 8); s.add_extent(0); let p = s.allocate_page().unwrap(); assert_eq!(p, 0); let p2 = s.allocate_page().unwrap(); assert_eq!(p2, 1); }
    #[test] fn test_seg_allocate_fills_extent() { let s = DataSegment::new(1, 4); s.add_extent(0); for _ in 0..4 { s.allocate_page().unwrap(); } assert!(s.allocate_page().is_none()); }
    #[test] fn test_seg_free_page() { let s = DataSegment::new(1, 4); s.add_extent(0); let p = s.allocate_page().unwrap(); s.free_page(p); let p2 = s.allocate_page().unwrap(); assert_eq!(p2, p); }
    #[test] fn test_seg_free_count() { let s = DataSegment::new(1, 8); s.add_extent(0); assert_eq!(s.free_page_count(), 8); s.allocate_page(); assert_eq!(s.free_page_count(), 7); }
    #[test] fn test_seg_multiple_extents() { let s = DataSegment::new(1, 4); s.add_extent(0); s.add_extent(100); for _ in 0..4 { s.allocate_page().unwrap(); } let p = s.allocate_page().unwrap(); assert_eq!(p, 100); }
    #[test] fn test_seg_extent_chain() { let s = DataSegment::new(1, 8); s.add_extent(0); s.add_extent(100); s.add_extent(200); assert_eq!(s.extent_starts(), vec![0, 100, 200]); }
    #[test] fn test_seg_alloc_free_cycle() { let s = DataSegment::new(1, 8); s.add_extent(0); let pages: Vec<u32> = (0..8).map(|_| s.allocate_page().unwrap()).collect(); for p in &pages { s.free_page(*p); } assert_eq!(s.free_page_count(), 8); }
    #[test] fn test_seg_concurrent() { let s = std::sync::Arc::new(DataSegment::new(1, 100)); s.add_extent(0); let mut h = vec![]; for _ in 0..4 { let s = s.clone(); h.push(std::thread::spawn(move || { for _ in 0..20 { if let Some(p) = s.allocate_page() { s.free_page(p); } } })); } for j in h { j.join().unwrap(); } }
    #[test] fn test_seg_large_extent() { let s = DataSegment::new(1, 1024); s.add_extent(0); for _ in 0..100 { s.allocate_page().unwrap(); } assert_eq!(s.free_page_count(), 924); }
    #[test] fn test_seg_free_invalid() { let s = DataSegment::new(1, 8); s.add_extent(0); s.free_page(999); }
    #[test] fn test_seg_empty_allocate() { let s = DataSegment::new(1, 8); assert!(s.allocate_page().is_none()); }
    #[test] fn test_seg_total_pages_multi() { let s = DataSegment::new(1, 8); s.add_extent(0); s.add_extent(100); s.add_extent(200); assert_eq!(s.total_pages(), 24); }
    #[test] fn test_seg_default_extent_size() { let s = DataSegment::new(1, 0); s.add_extent(0); assert_eq!(s.total_pages(), DEFAULT_EXTENT_SIZE); }
    #[test] fn test_seg_alloc_across_extents() { let s = DataSegment::new(1, 2); s.add_extent(0); s.add_extent(10); let p1 = s.allocate_page().unwrap(); assert_eq!(p1, 0); let p2 = s.allocate_page().unwrap(); assert_eq!(p2, 1); let p3 = s.allocate_page().unwrap(); assert_eq!(p3, 10); }
    #[test] fn test_seg_free_reuse() { let s = DataSegment::new(1, 4); s.add_extent(0); for _ in 0..4 { s.allocate_page().unwrap(); } s.free_page(2); let p = s.allocate_page().unwrap(); assert_eq!(p, 2); }
    #[test] fn test_seg_many_alloc_free() { let s = DataSegment::new(1, 100); s.add_extent(0); for _ in 0..50 { let p = s.allocate_page().unwrap(); s.free_page(p); } assert_eq!(s.free_page_count(), 100); }
}
