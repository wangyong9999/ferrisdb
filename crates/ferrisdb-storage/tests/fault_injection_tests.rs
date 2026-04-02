//! Fault injection and edge case tests — data integrity under adverse conditions

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{BufferTag, PdbId, Xid};
use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager, HeapPage};
use ferrisdb_storage::page::{PageHeader, PAGE_SIZE};
const PAGE_MAGIC: u16 = 0xD570;
use ferrisdb_storage::wal::{WalWriter, WalRecovery, RecoveryMode, WalRecordType};

// ==================== Page Checksum Corruption ====================

#[test]
fn test_page_checksum_valid_after_set() {
    let (bp, tag) = make_heap_page_test();
    let p = bp.pin(&tag).unwrap();
    let _l = p.lock_exclusive();
    let page = unsafe { std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE) };
    let hp = HeapPage::from_bytes(page);
    hp.insert_tuple(b"test data with checksum verification!").unwrap();
    let page = unsafe { std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE) };
    PageHeader::set_checksum(page);
    let page = unsafe { std::slice::from_raw_parts(p.page_data(), PAGE_SIZE) };
    assert!(PageHeader::verify_checksum(page));
}

#[test]
fn test_page_checksum_detects_corruption() {
    let (bp, tag) = make_heap_page_test();
    let p = bp.pin(&tag).unwrap();
    let _l = p.lock_exclusive();
    let page = unsafe { std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE) };
    let hp = HeapPage::from_bytes(page);
    hp.insert_tuple(b"important data").unwrap();
    let page = unsafe { std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE) };
    PageHeader::set_checksum(page);
    // Corrupt a byte
    page[4096] ^= 0xFF;
    assert!(!PageHeader::verify_checksum(page));
}

#[test]
fn test_page_checksum_zero_page() {
    let page = vec![0u8; PAGE_SIZE];
    assert!(PageHeader::verify_checksum(&page));
}

// ==================== Page Magic (Torn Page) ====================

#[test]
fn test_page_magic_on_init() {
    let (bp, tag) = make_heap_page_test();
    let p = bp.pin(&tag).unwrap();
    let page = unsafe { std::slice::from_raw_parts(p.page_data(), PAGE_SIZE) };
    assert!(PageHeader::is_valid_page(page));
}

#[test]
fn test_page_magic_detects_garbage() {
    let mut page = vec![0xDE; PAGE_SIZE];
    let magic_offset = std::mem::size_of::<PageHeader>() - 2;
    page[magic_offset] = 0xDE;
    page[magic_offset + 1] = 0xDE;
    assert!(!PageHeader::is_valid_page(&page));
}

#[test]
fn test_page_magic_valid_value() {
    let (bp, tag) = make_heap_page_test();
    let p = bp.pin(&tag).unwrap();
    let page = unsafe { std::slice::from_raw_parts(p.page_data(), PAGE_SIZE) };
    // HeapPageHeader wraps PageHeader; magic is inside PageHeader (base field)
    // Use is_valid_page which checks the correct offset
    assert!(PageHeader::is_valid_page(page), "Initialized page should have valid magic");
}

// ==================== WAL Torn Record Detection ====================

#[test]
fn test_wal_recovery_ignores_truncated_record() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    // Write valid WAL header + truncated record
    let w = WalWriter::new(&wal_dir);
    w.write(b"\x05\x00\x01\x00hello").unwrap(); // 5-byte payload, type=1 (HeapInsert)
    // Write garbage that looks like a truncated header
    w.write(&[0xFF; 2]).unwrap();
    w.sync().unwrap();
    drop(w);

    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    // Should not crash, should handle gracefully
    let _ = recovery.recover(RecoveryMode::CrashRecovery);
}

// ==================== Buffer Pool Under Stress ====================

#[test]
fn test_buffer_pool_exhaustion() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(3)).unwrap());
    let p0 = bp.pin(&BufferTag::new(PdbId::new(0), 1, 0)).unwrap();
    let p1 = bp.pin(&BufferTag::new(PdbId::new(0), 1, 1)).unwrap();
    let p2 = bp.pin(&BufferTag::new(PdbId::new(0), 1, 2)).unwrap();
    // All 3 pinned, 4th should fail
    assert!(bp.pin(&BufferTag::new(PdbId::new(0), 1, 3)).is_err());
    drop(p0);
    drop(p1);
    drop(p2);
}

#[test]
fn test_buffer_pool_churn() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(20)).unwrap());
    // Access 100 different pages with 20 buffers
    for i in 0..100u32 {
        let p = bp.pin(&BufferTag::new(PdbId::new(0), 1, i)).unwrap();
        drop(p);
    }
}

// ==================== Storage Manager Edge Cases ====================

#[test]
fn test_smgr_read_nonexistent_page() {
    let td = TempDir::new().unwrap();
    let smgr = StorageManager::new(td.path());
    smgr.init().unwrap();
    let tag = BufferTag::new(PdbId::new(0), 999, 0);
    let mut buf = vec![0u8; PAGE_SIZE];
    let result = smgr.read_page(&tag, &mut buf);
    // Should fail gracefully (file doesn't exist)
    assert!(result.is_err());
}

#[test]
fn test_smgr_write_then_read() {
    let td = TempDir::new().unwrap();
    let smgr = StorageManager::new(td.path());
    smgr.init().unwrap();
    let tag = BufferTag::new(PdbId::new(0), 42, 0);

    let mut data = vec![0u8; PAGE_SIZE];
    data[0..4].copy_from_slice(b"TEST");
    smgr.write_page(&tag, &data).unwrap();

    let mut read_buf = vec![0u8; PAGE_SIZE];
    smgr.read_page(&tag, &mut read_buf).unwrap();
    assert_eq!(&read_buf[0..4], b"TEST");
}

#[test]
fn test_smgr_multiple_relations() {
    let td = TempDir::new().unwrap();
    let smgr = StorageManager::new(td.path());
    smgr.init().unwrap();

    for rel in 1..10u16 {
        let tag = BufferTag::new(PdbId::new(0), rel, 0);
        let data = vec![rel as u8; PAGE_SIZE];
        smgr.write_page(&tag, &data).unwrap();
    }
    for rel in 1..10u16 {
        let tag = BufferTag::new(PdbId::new(0), rel, 0);
        let mut buf = vec![0u8; PAGE_SIZE];
        smgr.read_page(&tag, &mut buf).unwrap();
        assert_eq!(buf[0], rel as u8);
    }
}

// ==================== Heap Page Edge Cases (via BufferPool) ====================

fn make_heap_page_test() -> (Arc<BufferPool>, BufferTag) {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(10)).unwrap());
    let tag = BufferTag::new(PdbId::new(0), 999, 0);
    {
        let p = bp.pin(&tag).unwrap();
        let _l = p.lock_exclusive();
        let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };
        hp.init();
        drop(_l);
        p.mark_dirty();
    }
    (bp, tag)
}

#[test]
fn test_heap_page_insert_until_full() {
    let (bp, tag) = make_heap_page_test();
    let p = bp.pin(&tag).unwrap();
    let _l = p.lock_exclusive();
    let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };

    let mut count = 0;
    let data = vec![0xAA; 100];
    while hp.insert_tuple(&data).is_some() {
        count += 1;
    }
    assert!(count > 10);
    assert!(count < 100);
}

#[test]
fn test_heap_page_mark_dead_and_compact() {
    let (bp, tag) = make_heap_page_test();
    let p = bp.pin(&tag).unwrap();
    let _l = p.lock_exclusive();
    let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };

    for i in 0..5 {
        hp.insert_tuple(format!("tuple_{}", i).as_bytes()).unwrap();
    }
    hp.mark_dead(2);
    let reclaimed = hp.compact_dead();
    assert!(reclaimed > 0);
}

#[test]
fn test_heap_page_free_space_tracking() {
    let (bp, tag) = make_heap_page_test();
    let p = bp.pin(&tag).unwrap();
    let _l = p.lock_exclusive();
    let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };

    let initial_free = hp.free_space();
    hp.insert_tuple(b"some data here").unwrap();
    let after_insert = hp.free_space();
    assert!(after_insert < initial_free);
}
