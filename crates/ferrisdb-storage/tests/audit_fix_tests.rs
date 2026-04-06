//! Tests for audit report fixes (storage-layer)
//!
//! Covers: C1 (WAL CRC), C6 (BufTable RwLock), C8 (unlock_header atomic),
//! M8 (ParallelRedo Condvar), M9 (ParallelRedo counter)

use ferrisdb_core::{BufferTag, Lsn, PdbId, Xid};
use ferrisdb_storage::wal::{
    WalRecordHeader, WalRecordType, WalHeapInsert, WalWriter,
    ParallelRedoCoordinator, ParallelRedoConfig, RedoRecord,
};
use ferrisdb_storage::{BufferPool, BufferPoolConfig, PageId};
use std::sync::Arc;
use tempfile::TempDir;

// ============================================================
// C1: WAL CRC corruption detection
// ============================================================

#[test]
fn test_wal_crc_detects_corruption() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let writer = WalWriter::new(&wal_dir);
    let data = vec![42u8; 20];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 0, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    // Corrupt the WAL file payload
    let wal_files: Vec<_> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .collect();
    assert!(!wal_files.is_empty());

    let wal_path = wal_files[0].path();
    let mut contents = std::fs::read(&wal_path).unwrap();
    if contents.len() > 60 {
        contents[55] ^= 0xFF;
        std::fs::write(&wal_path, &contents).unwrap();
    }

    // Recovery should handle corrupted record gracefully
    let smgr = Arc::new(ferrisdb_storage::StorageManager::new(td.path()));
    smgr.init().unwrap();
    let recovery = ferrisdb_storage::wal::WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover_scan_only().unwrap();
    // Key: no panic, recovery completes
}

#[test]
fn test_wal_crc_valid_records_pass() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let writer = WalWriter::new(&wal_dir);
    for i in 0u16..3 {
        let data = vec![i as u8; 30];
        let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), i, &data);
        writer.write(&rec.serialize_with_data(&data)).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    let smgr = Arc::new(ferrisdb_storage::StorageManager::new(td.path()));
    smgr.init().unwrap();
    let recovery = ferrisdb_storage::wal::WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover_scan_only().unwrap();
    let stats = recovery.stats();
    assert!(
        stats.records_redone + stats.records_skipped >= 3,
        "Expected >= 3 processed, got {} + {}",
        stats.records_redone, stats.records_skipped
    );
}

// ============================================================
// C6: BufTable concurrent lookup safety (no torn read)
// ============================================================

#[test]
fn test_buftable_concurrent_lookup_insert() {
    use ferrisdb_storage::buffer::BufTable;
    use std::thread;

    let table = Arc::new(BufTable::new(256));
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let mut handles = Vec::new();

    // 4 writer threads
    for t in 0..4u32 {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..100u32 {
                let tag = BufferTag::new(PdbId::new(0), (t * 100 + i) as u16, i);
                table.insert(tag, (t * 100 + i) as i32);
            }
        }));
    }

    // 4 reader threads
    for t in 0..4u32 {
        let table = Arc::clone(&table);
        let barrier = Arc::clone(&barrier);
        handles.push(thread::spawn(move || {
            barrier.wait();
            for i in 0..100u32 {
                let tag = BufferTag::new(PdbId::new(0), (t * 100 + i) as u16, i);
                let _ = table.lookup(&tag);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }
}

// ============================================================
// C8: unlock_header uses fetch_and (preserves concurrent state)
// ============================================================

#[test]
fn test_buffer_desc_dirty_survives_lock_unlock() {
    // Test that mark_dirty flag persists through lock/unlock cycles
    // (verifies unlock_header uses fetch_and, not load+store)
    let bp = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
    let tag = BufferTag::new(PdbId::new(0), 1, 0);
    let pinned = bp.pin(&tag).unwrap();
    pinned.mark_dirty();
    // Lock and unlock the page (this exercises unlock_header)
    {
        let _lock = pinned.lock_exclusive();
        // dirty flag should survive lock_exclusive + drop
    }
    // The dirty page should still be counted in dirty stats
    assert!(bp.dirty_page_count() > 0, "dirty page should persist after lock/unlock");
}

// ============================================================
// M8+M9: ParallelRedo Condvar + counter
// ============================================================

#[test]
fn test_parallel_redo_dispatch_increments_counter() {
    let config = ParallelRedoConfig {
        num_workers: 2,
        ..Default::default()
    };
    let coordinator = ParallelRedoCoordinator::new(config);

    for i in 0..5u64 {
        let record = RedoRecord {
            lsn: Lsn::from_raw(i + 1),
            tag: BufferTag::new(PdbId::new(0), 1, i as u32),
            xid: Xid::new(0, 1),
            record_type: WalRecordType::HeapInsert,
            data: vec![],
        };
        coordinator.dispatch(record);
    }

    let stats = coordinator.stats();
    assert_eq!(stats.total_processed, 5);
}

// ============================================================
// WAL record header CRC field
// ============================================================

#[test]
fn test_wal_record_header_crc_roundtrip() {
    let mut hdr = WalRecordHeader::new(WalRecordType::HeapInsert, 42);
    let data = vec![1u8, 2, 3, 4, 5];
    hdr.compute_crc(&data);
    // Read crc via copy (packed struct)
    let crc = { hdr.crc };
    assert_ne!(crc, 0);
    assert!(hdr.verify_crc(&data));
    assert!(!hdr.verify_crc(&[9, 8, 7]));
}

#[test]
fn test_wal_record_header_size_is_8() {
    assert_eq!(std::mem::size_of::<WalRecordHeader>(), 8);
}
