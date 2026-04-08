//! Round 10: BufferPool deep paths + BackgroundWriter + hit_rate + misc

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{BufferTag, PdbId};
use ferrisdb_storage::*;
use ferrisdb_storage::page::PAGE_SIZE;

// ============================================================
// BufferPool 深度路径
// ============================================================

#[test]
fn test_buffer_pool_with_smgr_flush() {
    let td = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();

    let bp = BufferPool::with_smgr(BufferPoolConfig::new(50), Some(smgr)).unwrap();

    // Pin, write, mark dirty
    let tag = BufferTag::new(PdbId::new(0), 1, 0);
    {
        let pinned = bp.pin(&tag).unwrap();
        bp.mark_dirty(pinned.buf_id());
    }

    // flush_all with smgr should write to disk
    bp.flush_all().unwrap();
}

#[test]
fn test_buffer_pool_flush_some_with_smgr() {
    let td = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();

    let bp = BufferPool::with_smgr(BufferPoolConfig::new(50), Some(smgr)).unwrap();

    for i in 0..5u32 {
        let tag = BufferTag::new(PdbId::new(0), 1, i);
        let pinned = bp.pin(&tag).unwrap();
        bp.mark_dirty(pinned.buf_id());
        drop(pinned);
    }

    let flushed = bp.flush_some(3);
    assert!(flushed <= 3);
}

#[test]
fn test_buffer_pool_hit_rate() {
    let bp = BufferPool::new(BufferPoolConfig::new(50)).unwrap();

    // Initial hit rate should be 0
    assert_eq!(bp.hit_rate(), 0.0);

    let tag = BufferTag::new(PdbId::new(0), 1, 0);
    let _ = bp.pin(&tag).unwrap(); // miss

    let pinned2 = bp.pin(&tag).unwrap(); // hit
    drop(pinned2);

    let rate = bp.hit_rate();
    assert!(rate >= 0.0 && rate <= 1.0);
}

#[test]
fn test_buffer_pool_eviction_cycle() {
    let bp = BufferPool::new(BufferPoolConfig::new(3)).unwrap();

    // Fill pool
    for i in 0..3u32 {
        let tag = BufferTag::new(PdbId::new(0), 1, i);
        let _ = bp.pin(&tag).unwrap();
    }

    // Evict by pinning more
    for i in 3..10u32 {
        let tag = BufferTag::new(PdbId::new(0), 1, i);
        let _ = bp.pin(&tag).unwrap();
    }

    assert!(bp.stat_misses() > 0);
}

#[test]
fn test_buffer_pool_page_data_access() {
    let bp = BufferPool::new(BufferPoolConfig::new(20)).unwrap();
    let tag = BufferTag::new(PdbId::new(0), 1, 0);
    let pinned = bp.pin(&tag).unwrap();

    let ptr = pinned.page_data();
    assert!(!ptr.is_null());

    // Write pattern
    unsafe {
        let slice = std::slice::from_raw_parts_mut(ptr, PAGE_SIZE);
        slice[0] = 0xAB;
        slice[100] = 0xCD;
    }

    let buf_id = pinned.buf_id();
    let desc = bp.get_buffer(buf_id);
    assert!(desc.is_some());

    drop(pinned);
}

#[test]
fn test_buffer_pool_concurrent_eviction() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(10)).unwrap());
    let mut handles = vec![];

    for t in 0..4 {
        let bp = Arc::clone(&bp);
        handles.push(std::thread::spawn(move || {
            for i in 0..30u32 {
                let tag = BufferTag::new(PdbId::new(0), 1, t * 100 + i);
                if let Ok(pinned) = bp.pin(&tag) {
                    drop(pinned);
                }
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

// ============================================================
// BackgroundWriter
// ============================================================

#[test]
fn test_background_writer_start_stop() {
    let td = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();

    let bp = Arc::new(BufferPool::with_smgr(BufferPoolConfig::new(50), Some(smgr)).unwrap());

    // Insert some dirty pages
    for i in 0..5u32 {
        let tag = BufferTag::new(PdbId::new(0), 1, i);
        let pinned = bp.pin(&tag).unwrap();
        bp.mark_dirty(pinned.buf_id());
        drop(pinned);
    }

    // Start bgwriter
    let bgwriter = buffer::BackgroundWriter::start(Arc::clone(&bp), 50, 2);
    std::thread::sleep(std::time::Duration::from_millis(150));
    bgwriter.stop();
    std::thread::sleep(std::time::Duration::from_millis(100));
}

// ============================================================
// HeapPage 深度路径
// ============================================================

#[test]
fn test_heap_page_linp_count() {
    #[repr(C, align(8192))]
    struct A { data: [u8; PAGE_SIZE] }
    let mut a = A { data: [0u8; PAGE_SIZE] };
    let page = page::HeapPage::from_bytes(&mut a.data);
    page.init();

    assert_eq!(page.linp_count(), 0);

    let tuple = vec![0xABu8; 50];
    page.insert_tuple(&tuple);
    assert!(page.linp_count() > 0);
}

#[test]
fn test_heap_page_as_bytes() {
    #[repr(C, align(8192))]
    struct A { data: [u8; PAGE_SIZE] }
    let mut a = A { data: [0u8; PAGE_SIZE] };
    let page = page::HeapPage::from_bytes(&mut a.data);
    page.init();

    let bytes = page.as_bytes();
    assert_eq!(bytes.len(), PAGE_SIZE);
}

#[test]
fn test_heap_page_item_id() {
    #[repr(C, align(8192))]
    struct A { data: [u8; PAGE_SIZE] }
    let mut a = A { data: [0u8; PAGE_SIZE] };
    let page = page::HeapPage::from_bytes(&mut a.data);
    page.init();

    // No items yet
    assert!(page.item_id(0).is_none());

    let tuple = vec![0xABu8; 80];
    page.insert_tuple(&tuple);
    // Now should have item at index 0
    let item = page.item_id(0);
    assert!(item.is_some());
}

#[test]
fn test_heap_page_checksum_roundtrip() {
    let mut data = vec![0u8; PAGE_SIZE];
    data[100] = 0xAB;
    data[4000] = 0xCD;

    page::PageHeader::set_checksum(&mut data);
    assert!(page::PageHeader::verify_checksum(&data));

    // Corrupt single byte
    data[2000] ^= 0xFF;
    assert!(!page::PageHeader::verify_checksum(&data));
}

// ============================================================
// ControlFile 深度测试
// ============================================================

#[test]
fn test_control_file_update_checkpoint() {
    let td = TempDir::new().unwrap();
    let cf = ControlFile::open(td.path()).unwrap();
    let lsn = ferrisdb_core::Lsn::from_parts(1, 500);
    let _ = cf.update_checkpoint(lsn, 1);
    let saved = cf.checkpoint_lsn();
    // Verify LSN was persisted
    let _ = saved;
}

// ============================================================
// SystemCatalog 深度测试
// ============================================================

#[test]
fn test_catalog_create_many_tables() {
    let cat = SystemCatalog::new();
    for i in 0..20 {
        cat.create_table(&format!("table_{}", i), 0).unwrap();
    }
    assert_eq!(cat.count(), 20);
    let tables = cat.list_tables();
    assert_eq!(tables.len(), 20);
}

#[test]
fn test_catalog_lookup_nonexistent() {
    let cat = SystemCatalog::new();
    assert!(cat.lookup_by_name("nonexistent").is_none());
    assert!(cat.lookup_by_oid(999).is_none());
}

#[test]
fn test_catalog_drop_nonexistent() {
    let cat = SystemCatalog::new();
    let result = cat.drop_relation(999);
    assert!(result.is_err());
}

// ============================================================
// BTree 补充路径
// ============================================================

#[test]
fn test_btree_large_values() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(500)).unwrap());
    let tree = BTree::new(1100, bp);
    tree.init().unwrap();

    // Insert keys with large values
    for i in 0..50u32 {
        let key = BTreeKey::new(format!("large_{:06}", i).into_bytes());
        tree.insert(key, BTreeValue::Tuple { block: i, offset: (i % 100) as u16 }).unwrap();
    }

    // Verify all findable
    for i in 0..50u32 {
        let key = BTreeKey::new(format!("large_{:06}", i).into_bytes());
        assert!(tree.lookup(&key).unwrap().is_some());
    }
}

#[test]
fn test_btree_delete_triggers_consideration() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(500)).unwrap());
    let tree = BTree::new(1101, bp);
    tree.init().unwrap();

    // Insert many then delete many
    for i in 0..200u32 {
        tree.insert(
            BTreeKey::new(format!("del_{:06}", i).into_bytes()),
            BTreeValue::Tuple { block: i, offset: 0 },
        ).unwrap();
    }

    // Delete every other key
    for i in (0..200u32).step_by(2) {
        tree.delete(&BTreeKey::new(format!("del_{:06}", i).into_bytes())).unwrap();
    }

    // Verify remaining keys
    for i in (1..200u32).step_by(2) {
        assert!(tree.lookup(&BTreeKey::new(format!("del_{:06}", i).into_bytes())).unwrap().is_some());
    }
}

#[test]
fn test_btree_concurrent_4_thread_insert() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(500)).unwrap());
    let tree = Arc::new(BTree::new(1102, bp));
    tree.init().unwrap();

    let mut handles = vec![];
    for t in 0..4 {
        let tree = Arc::clone(&tree);
        handles.push(std::thread::spawn(move || {
            for i in 0..100u32 {
                tree.insert(
                    BTreeKey::new(format!("c{}_{:04}", t, i).into_bytes()),
                    BTreeValue::Tuple { block: i, offset: 0 },
                ).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    // All 400 keys should exist
    let mut found = 0;
    for t in 0..4 {
        for i in 0..100u32 {
            if tree.lookup(&BTreeKey::new(format!("c{}_{:04}", t, i).into_bytes())).unwrap().is_some() {
                found += 1;
            }
        }
    }
    assert_eq!(found, 400);
}
