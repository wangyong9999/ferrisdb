//! Segment deep coverage — matching C++ tablespace/segment test depth

use ferrisdb_storage::tablespace::*;
use ferrisdb_storage::*;
use tempfile::TempDir;
use std::sync::Arc;

// ===== DataSegment deep tests =====
#[test] fn test_seg_alloc_100_pages() { let s = DataSegment::new(1, 100); s.add_extent(0); for i in 0..100 { assert_eq!(s.allocate_page().unwrap(), i); } assert!(s.allocate_page().is_none()); }
#[test] fn test_seg_multi_extent_alloc() { let s = DataSegment::new(1, 4); for i in 0..5 { s.add_extent(i * 100); } for _ in 0..20 { s.allocate_page().unwrap(); } assert_eq!(s.free_page_count(), 0); }
#[test] fn test_seg_free_all_realloc() { let s = DataSegment::new(1, 8); s.add_extent(0); let pages: Vec<u32> = (0..8).map(|_| s.allocate_page().unwrap()).collect(); for p in pages.iter().rev() { s.free_page(*p); } assert_eq!(s.free_page_count(), 8); for _ in 0..8 { s.allocate_page().unwrap(); } }
#[test] fn test_seg_extent_chain_order() { let s = DataSegment::new(1, 8); s.add_extent(0); s.add_extent(1000); s.add_extent(2000); assert_eq!(s.extent_starts(), vec![0, 1000, 2000]); }
#[test] fn test_seg_interleaved_alloc_free() { let s = DataSegment::new(1, 16); s.add_extent(0); s.add_extent(100); s.add_extent(200); for _ in 0..20 { if let Some(p) = s.allocate_page() { s.free_page(p); } } }
#[test] fn test_seg_total_pages_empty() { let s = DataSegment::new(1, 8); assert_eq!(s.total_pages(), 0); }
#[test] fn test_seg_total_pages_one() { let s = DataSegment::new(1, 8); s.add_extent(0); assert_eq!(s.total_pages(), 8); }
#[test] fn test_seg_total_pages_ten() { let s = DataSegment::new(1, 8); for i in 0..10 { s.add_extent(i * 100); } assert_eq!(s.total_pages(), 80); }
#[test] fn test_seg_free_page_idempotent() { let s = DataSegment::new(1, 8); s.add_extent(0); s.allocate_page().unwrap(); s.free_page(0); s.free_page(0); assert_eq!(s.free_page_count(), 8); }

// ===== Tablespace + Segment integration =====
#[test] fn test_ts_segment_lifecycle() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 8).unwrap(); let seg = DataSegment::new(100, ts.extent_size()); let start = ts.allocate_extent(100); seg.add_extent(start); for _ in 0..8 { seg.allocate_page().unwrap(); } assert_eq!(seg.free_page_count(), 0); let start2 = ts.allocate_extent(100); seg.add_extent(start2); assert_eq!(seg.free_page_count(), 8); }
#[test] fn test_ts_multi_relation_segments() { let td = TempDir::new().unwrap(); let ts = Tablespace::open(1, td.path(), 8).unwrap(); let s1 = DataSegment::new(100, 8); let s2 = DataSegment::new(200, 8); s1.add_extent(ts.allocate_extent(100)); s2.add_extent(ts.allocate_extent(200)); s1.allocate_page().unwrap(); s2.allocate_page().unwrap(); }

// ===== Manager deep =====
#[test] fn test_mgr_create_many() { let td = TempDir::new().unwrap(); let mgr = TablespaceManager::new(td.path()); for i in 1..20 { mgr.create_tablespace(i).unwrap(); } assert_eq!(mgr.list().len(), 19); }
#[test]
fn test_mgr_allocate_many() {
    let td = TempDir::new().unwrap();
    let mgr = TablespaceManager::new(td.path());
    mgr.create_tablespace(1).unwrap();
    for i in 0..100u32 {
        let p = mgr.allocate_extent(1, i).unwrap();
        assert_eq!(p, 0);
    }
}

// ===== Catalog deep =====
#[test] fn test_cat_create_many_tables() { let c = SystemCatalog::new(); for i in 0..100 { c.create_table(&format!("t_{}", i), 1).unwrap(); } assert_eq!(c.list_tables().len(), 100); }
#[test] fn test_cat_create_many_indexes() { let c = SystemCatalog::new(); let t = c.create_table("base", 1).unwrap(); for i in 0..20 { c.create_index(&format!("idx_{}", i), t, 1).unwrap(); } assert_eq!(c.list_indexes(t).len(), 20); }
#[test] fn test_cat_drop_and_recreate() { let c = SystemCatalog::new(); let oid = c.create_table("temp", 1).unwrap(); c.drop_relation(oid).unwrap(); c.create_table("temp", 1).unwrap(); assert!(c.lookup_by_name("temp").is_some()); }
#[test] fn test_cat_drop_nonexistent() { let c = SystemCatalog::new(); assert!(c.drop_relation(999).is_err()); }
#[test] fn test_cat_concurrent_create() { let c = Arc::new(SystemCatalog::new()); let mut h = vec![]; for t in 0..4 { let c = c.clone(); h.push(std::thread::spawn(move || { for i in 0..25 { c.create_table(&format!("t_{}_{}", t, i), 1).unwrap(); } })); } for j in h { j.join().unwrap(); } assert_eq!(c.count(), 100); }
#[test] fn test_cat_index_no_table() { let c = SystemCatalog::new(); assert!(c.create_index("bad", 999, 1).is_err()); }
#[test] fn test_cat_relation_type() { let c = SystemCatalog::new(); let t = c.create_table("tbl", 1).unwrap(); let i = c.create_index("idx", t, 1).unwrap(); assert_eq!(c.lookup_by_oid(t).unwrap().rel_type, RelationType::Table); assert_eq!(c.lookup_by_oid(i).unwrap().rel_type, RelationType::Index); }
#[test] fn test_cat_tablespace_id() { let c = SystemCatalog::new(); let oid = c.create_table("ts_test", 42).unwrap(); assert_eq!(c.lookup_by_oid(oid).unwrap().tablespace_id, 42); }

// ===== ParallelScan deep =====
#[test] fn test_pscan_single_page() { let r = parallel_scan(1, 1, 1, |s, e| e - s); assert_eq!(r.iter().sum::<u32>(), 1); }
#[test] fn test_pscan_large() { let r = parallel_scan(10000, 8, 100, |s, e| e - s); assert_eq!(r.iter().sum::<u32>(), 10000); }
#[test] fn test_pscan_more_workers_than_pages() { let r = parallel_scan(3, 8, 1, |s, e| e - s); assert_eq!(r.iter().sum::<u32>(), 3); }
#[test] fn test_pscan_batch_1() { let r = parallel_scan(100, 4, 1, |s, _e| s); assert_eq!(r.len(), 100); }
#[test] fn test_pscan_coordinator_reset() { let c = ParallelScanCoordinator::new(10, 5); c.next_batch(); c.next_batch(); c.reset(); let (s, _) = c.next_batch().unwrap(); assert_eq!(s, 0); }

// ===== LOB deep =====
#[test] fn test_lob_1mb() { let s = LobStore::new(); let data = vec![0xAA; 1_000_000]; let h = s.store(&data).unwrap(); assert!(h.chunk_count > 100); let r = s.fetch(h.lob_id).unwrap(); assert_eq!(r.len(), 1_000_000); assert_eq!(r[0], 0xAA); assert_eq!(r[999_999], 0xAA); }
#[test] fn test_lob_exact_chunk_boundary() { let s = LobStore::new(); let data = vec![0xBB; LOB_CHUNK_SIZE * 3]; let h = s.store(&data).unwrap(); assert_eq!(h.chunk_count, 3); let r = s.fetch(h.lob_id).unwrap(); assert_eq!(r.len(), LOB_CHUNK_SIZE * 3); }
#[test] fn test_lob_delete_all() { let s = LobStore::new(); let ids: Vec<u32> = (0..10).map(|_| s.store(b"x").unwrap().lob_id).collect(); for id in &ids { s.delete(*id).unwrap(); } assert_eq!(s.count(), 0); }
#[test] fn test_lob_header_bytes_size() { let h = LobHeader::new(100, 1); assert_eq!(h.to_bytes().len(), 10); }
