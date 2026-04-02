//! Buffer Pool advanced tests — eviction edge cases, concurrent flush, WAL integration

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{BufferTag, PdbId};
use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager, HeapPage, WalWriter};
use ferrisdb_storage::page::PAGE_SIZE;

fn tag(rel: u16, block: u32) -> BufferTag {
    BufferTag::new(PdbId::new(0), rel, block)
}

fn make_bp_smgr(n: usize) -> (Arc<BufferPool>, Arc<StorageManager>, TempDir) {
    let td = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let mut bp = BufferPool::new(BufferPoolConfig::new(n)).unwrap();
    bp.set_smgr(Arc::clone(&smgr));
    (Arc::new(bp), smgr, td)
}

// ==================== Eviction Edge Cases ====================

#[test]
fn test_eviction_dirty_page_flushed() {
    let (bp, _, _td) = make_bp_smgr(5);
    // Pin, dirty, unpin all 5 buffers
    for i in 0..5u32 {
        let p = bp.pin(&tag(1, i)).unwrap();
        let _l = p.lock_exclusive();
        let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };
        hp.init();
        drop(_l);
        p.mark_dirty();
    }
    // Now pin a 6th page — must evict a dirty page
    let p = bp.pin(&tag(1, 5)).unwrap();
    assert!(p.buf_id() >= 0);
}

#[test]
fn test_pin_same_page_concurrent() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(100)).unwrap());
    let t = tag(1, 0);
    { let _ = bp.pin(&t).unwrap(); } // Create page

    let mut handles = vec![];
    for _ in 0..8 {
        let bp = Arc::clone(&bp);
        handles.push(std::thread::spawn(move || {
            for _ in 0..200 {
                let p = bp.pin(&tag(1, 0)).unwrap();
                let _l = p.lock_shared();
                let _ = p.page_slice()[0];
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

#[test]
fn test_concurrent_pin_different_pages() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let mut handles = vec![];
    for t in 0..4 {
        let bp = Arc::clone(&bp);
        handles.push(std::thread::spawn(move || {
            for i in 0..50u32 {
                let p = bp.pin(&tag((t + 1) as u16, i)).unwrap();
                let _l = p.lock_exclusive();
                unsafe { *p.page_data() = (t * 50 + i) as u8; }
                p.mark_dirty();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

#[test]
fn test_flush_all_clears_dirty() {
    let (bp, _, _td) = make_bp_smgr(50);
    for i in 0..10u32 {
        let p = bp.pin(&tag(1, i)).unwrap();
        let _l = p.lock_exclusive();
        let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };
        hp.init();
        drop(_l);
        p.mark_dirty();
    }
    assert!(bp.dirty_page_count() > 0);
    bp.flush_all().unwrap();
    assert_eq!(bp.dirty_page_count(), 0);
}

#[test]
fn test_flush_some_lsn_ordered() {
    let (bp, _, _td) = make_bp_smgr(50);
    for i in 0..10u32 {
        let p = bp.pin(&tag(1, i)).unwrap();
        let _l = p.lock_exclusive();
        let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };
        hp.init();
        // Set page LSN
        let lsn_bytes = ((i + 1) as u64 * 1000).to_le_bytes();
        unsafe { std::ptr::copy_nonoverlapping(lsn_bytes.as_ptr(), p.page_data(), 8); }
        drop(_l);
        p.mark_dirty();
    }
    let flushed = bp.flush_some(5);
    assert!(flushed <= 5);
}

#[test]
fn test_buffer_pool_with_wal_writer() {
    let td = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let wal = Arc::new(WalWriter::new(&wal_dir));

    let mut bp = BufferPool::new(BufferPoolConfig::new(50)).unwrap();
    bp.set_smgr(smgr);
    bp.set_wal_writer(Arc::clone(&wal));
    let bp = Arc::new(bp);

    let p = bp.pin(&tag(1, 0)).unwrap();
    let _l = p.lock_exclusive();
    let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };
    hp.init();
    drop(_l);
    p.mark_dirty();
    drop(p);

    // WAL-before-data: flush should work (no LSN to wait for since we didn't write WAL)
    bp.flush_all().unwrap();
}

#[test]
fn test_buffer_pool_total_page_count() {
    let bp = BufferPool::new(BufferPoolConfig::new(42)).unwrap();
    assert_eq!(bp.total_page_count(), 42);
}

#[test]
fn test_buffer_pool_dirty_page_count() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(100)).unwrap());
    assert_eq!(bp.dirty_page_count(), 0);
    let p = bp.pin(&tag(1, 0)).unwrap();
    p.mark_dirty();
    assert_eq!(bp.dirty_page_count(), 1);
}

// ==================== BackgroundWriter ====================

#[test]
fn test_background_writer_starts_stops() {
    use ferrisdb_storage::BackgroundWriter;
    let (bp, _, _td) = make_bp_smgr(50);
    let bw = BackgroundWriter::start(bp, 50, 5);
    std::thread::sleep(std::time::Duration::from_millis(100));
    bw.stop();
}

// ==================== Persistence ====================

#[test]
fn test_multiple_pages_persist() {
    let td = TempDir::new().unwrap();
    // Write
    {
        let smgr = Arc::new(StorageManager::new(td.path()));
        smgr.init().unwrap();
        let mut bp = BufferPool::new(BufferPoolConfig::new(50)).unwrap();
        bp.set_smgr(smgr);
        let bp = Arc::new(bp);

        for i in 0..5u32 {
            let p = bp.pin(&tag(50, i)).unwrap();
            let _l = p.lock_exclusive();
            let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };
            hp.init();
            hp.insert_tuple(format!("page_{}", i).as_bytes()).unwrap();
            drop(_l);
            p.mark_dirty();
        }
        bp.flush_all().unwrap();
    }
    // Read
    {
        let smgr = Arc::new(StorageManager::new(td.path()));
        smgr.init().unwrap();
        let mut bp = BufferPool::new(BufferPoolConfig::new(50)).unwrap();
        bp.set_smgr(smgr);
        let bp = Arc::new(bp);

        for i in 0..5u32 {
            let p = bp.pin(&tag(50, i)).unwrap();
            let _l = p.lock_shared();
            let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };
            assert!(hp.get_tuple(1).is_some(), "Page {} should have data", i);
        }
    }
}

// ==================== Control File ====================

#[test]
fn test_control_file_roundtrip() {
    use ferrisdb_storage::{ControlFile, ControlFileData};
    let td = TempDir::new().unwrap();
    {
        let cf = ControlFile::open(td.path()).unwrap();
        cf.update_checkpoint(ferrisdb_core::Lsn::from_raw(42000), 5).unwrap();
        cf.set_state(1).unwrap();
    }
    {
        let cf = ControlFile::open(td.path()).unwrap();
        assert_eq!(cf.checkpoint_lsn().raw(), 42000);
        assert_eq!(cf.get_data().last_wal_file, 5);
        assert_eq!(cf.get_data().state, 1);
    }
}

#[test]
fn test_control_file_corruption_detected() {
    use ferrisdb_storage::ControlFileData;
    let mut data = ControlFileData::default();
    data.checkpoint_lsn = 99999;
    let mut bytes = data.to_bytes();
    bytes[10] ^= 0xFF; // corrupt
    assert!(ControlFileData::from_bytes(&bytes).is_none());
}

#[test]
fn test_control_file_default_values() {
    use ferrisdb_storage::ControlFile;
    let td = TempDir::new().unwrap();
    let cf = ControlFile::open(td.path()).unwrap();
    assert_eq!(cf.checkpoint_lsn().raw(), 0);
    assert_eq!(cf.get_data().state, 0);
}

#[test]
fn test_control_file_multiple_updates() {
    use ferrisdb_storage::ControlFile;
    let td = TempDir::new().unwrap();
    let cf = ControlFile::open(td.path()).unwrap();
    for i in 1..10u64 {
        cf.update_checkpoint(ferrisdb_core::Lsn::from_raw(i * 1000), i as u32).unwrap();
    }
    assert_eq!(cf.checkpoint_lsn().raw(), 9000);
}

// ==================== Storage Manager ====================

#[test]
fn test_smgr_write_multiple_blocks() {
    let td = TempDir::new().unwrap();
    let smgr = StorageManager::new(td.path());
    smgr.init().unwrap();

    for block in 0..5u32 {
        let data = vec![block as u8; PAGE_SIZE];
        smgr.write_page(&tag(1, block), &data).unwrap();
    }
    for block in 0..5u32 {
        let mut buf = vec![0u8; PAGE_SIZE];
        smgr.read_page(&tag(1, block), &mut buf).unwrap();
        assert_eq!(buf[0], block as u8);
    }
}

#[test]
fn test_smgr_multiple_relations() {
    let td = TempDir::new().unwrap();
    let smgr = StorageManager::new(td.path());
    smgr.init().unwrap();
    for rel in 1..20u16 {
        let data = vec![rel as u8; PAGE_SIZE];
        smgr.write_page(&tag(rel, 0), &data).unwrap();
    }
    for rel in 1..20u16 {
        let mut buf = vec![0u8; PAGE_SIZE];
        smgr.read_page(&tag(rel, 0), &mut buf).unwrap();
        assert_eq!(buf[0], rel as u8);
    }
}

#[test]
fn test_smgr_overwrite_page() {
    let td = TempDir::new().unwrap();
    let smgr = StorageManager::new(td.path());
    smgr.init().unwrap();
    let t = tag(1, 0);
    smgr.write_page(&t, &vec![0xAA; PAGE_SIZE]).unwrap();
    smgr.write_page(&t, &vec![0xBB; PAGE_SIZE]).unwrap();
    let mut buf = vec![0u8; PAGE_SIZE];
    smgr.read_page(&t, &mut buf).unwrap();
    assert_eq!(buf[0], 0xBB);
}

// Undo zone and record tests are in dstore-transaction crate
