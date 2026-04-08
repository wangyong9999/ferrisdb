//! Round 9: WAL writer deep paths + config GUC + reader edge cases

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{Lsn, PageId, Xid};
use ferrisdb_storage::*;
use ferrisdb_storage::wal::*;
use ferrisdb_storage::page::PAGE_SIZE;

// ============================================================
// WalWriter 深度测试
// ============================================================

#[test]
fn test_wal_writer_switch_file() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = WalWriter::new(&wal_dir);

    // Write some data
    let data = vec![0xABu8; 100];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();

    // Manually switch file
    let file_no_before = writer.file_no();
    writer.switch_file().unwrap();
    let file_no_after = writer.file_no();
    assert_eq!(file_no_after, file_no_before + 1);

    // Write more data to new file
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
}

#[test]
fn test_wal_writer_write_and_sync() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = WalWriter::new(&wal_dir);

    let data = vec![0xCDu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    let lsn = writer.write_and_sync(&rec.serialize_with_data(&data)).unwrap();
    assert!(lsn.is_valid());

    let flushed = writer.flushed_lsn();
    assert!(flushed.raw() > 0);
}

#[test]
fn test_wal_writer_flushed_lsn() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = WalWriter::new(&wal_dir);

    // Initially no flushed LSN
    let initial = writer.flushed_lsn();

    let data = vec![0xABu8; 30];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();

    let after = writer.flushed_lsn();
    assert!(after.raw() > initial.raw());
}

#[test]
fn test_wal_writer_wait_for_lsn() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = WalWriter::new(&wal_dir);

    let data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    let lsn = writer.write(&rec.serialize_with_data(&data)).unwrap();

    // Sync first, then wait should return immediately
    writer.sync().unwrap();
    writer.wait_for_lsn(lsn).unwrap();
}

#[test]
fn test_wal_writer_wait_for_lsn_triggers_sync() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = WalWriter::new(&wal_dir);

    let data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    let lsn = writer.write(&rec.serialize_with_data(&data)).unwrap();

    // Wait without prior sync — should trigger fallback sync
    writer.wait_for_lsn(lsn).unwrap();
}

#[test]
fn test_wal_writer_large_record() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = WalWriter::new(&wal_dir);

    // Large record > 512 bytes triggers heap allocation path
    let data = vec![0xABu8; 1000];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    let lsn = writer.write(&rec.serialize_with_data(&data)).unwrap();
    assert!(lsn.is_valid());
    writer.sync().unwrap();
}

#[test]
fn test_wal_writer_small_record() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = WalWriter::new(&wal_dir);

    // Small record < 8 bytes
    let lsn = writer.write(&[0xAA; 4]).unwrap();
    assert!(lsn.is_valid());
    writer.sync().unwrap();
}

#[test]
fn test_wal_writer_read_back() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = WalWriter::new(&wal_dir);

    let data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    let lsn = writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();

    // Read back
    let read_data = writer.read(lsn).unwrap();
    assert!(!read_data.is_empty());
}

#[test]
fn test_wal_writer_read_old_file_fails() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = WalWriter::new(&wal_dir);

    // Try to read from a non-current file
    let old_lsn = Lsn::from_parts(999, 100);
    let result = writer.read(old_lsn);
    assert!(result.is_err());
}

#[test]
fn test_wal_writer_flusher() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = Arc::new(WalWriter::new(&wal_dir));

    let flusher = writer.start_flusher(50); // 50ms interval
    let data = vec![0xABu8; 30];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();

    // Wait for flusher to sync
    std::thread::sleep(std::time::Duration::from_millis(100));
    drop(flusher); // Stop flusher
}

// ============================================================
// WalReader edge cases
// ============================================================

#[test]
fn test_wal_reader_corrupt_magic() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    // Create a WAL file with invalid magic
    let path = wal_dir.join("00000000.wal");
    let mut data = vec![0u8; 40]; // WAL header size
    data[36..40].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]); // bad magic
    std::fs::write(&path, &data).unwrap();

    let mut reader = WalReader::new(&wal_dir);
    let result = reader.open(0);
    assert!(result.is_err(), "Should fail with invalid magic");
}

#[test]
fn test_wal_reader_eof() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let writer = WalWriter::new(&wal_dir);
    let data = vec![0xABu8; 30];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let mut reader = WalReader::new(&wal_dir);
    reader.open(0).unwrap();

    // Read until EOF
    let mut count = 0;
    while let Ok(Some(_)) = reader.read_next() {
        count += 1;
        if count > 100 { break; }
    }
    assert!(reader.is_eof() || count > 0);
}

// ============================================================
// WalBuffer 深度测试
// ============================================================

#[test]
fn test_wal_buffer_write_multiple() {
    let buf = WalBuffer::new(64 * 1024);
    for i in 0..20 {
        let data = vec![i as u8; 100];
        let _ = buf.write(&data);
    }
    assert!(buf.unflushed() > 0);
    let _ = buf.current_lsn();
}

#[test]
fn test_wal_buffer_mark_flushed() {
    let buf = WalBuffer::new(64 * 1024);
    let _ = buf.write(&[0xABu8; 100]);
    let unflushed_before = buf.unflushed();
    assert!(unflushed_before > 0);

    let data = buf.get_unflushed_data();
    let flushed_size = data.len();
    buf.mark_flushed(flushed_size);

    assert!(buf.unflushed() < unflushed_before);
}

#[test]
fn test_wal_buffer_advance_flush() {
    let buf = WalBuffer::new(64 * 1024);
    let _ = buf.write(&[0xCDu8; 50]);

    let write_pos = buf.write_pos();
    buf.advance_flush(write_pos);
    assert_eq!(buf.flush_pos(), write_pos);
}

// ============================================================
// Config GUC 测试
// ============================================================

#[test]
fn test_config_all_guc_fields() {
    use ferrisdb_core::config::config;
    use std::sync::atomic::Ordering;

    let cfg = config();
    // Exercise all GUC fields
    let _ = cfg.shared_buffers.load(Ordering::Relaxed);
    let _ = cfg.wal_buffers.load(Ordering::Relaxed);
    let _ = cfg.max_connections.load(Ordering::Relaxed);
    let _ = cfg.bgwriter_delay_ms.load(Ordering::Relaxed);
    let _ = cfg.bgwriter_max_pages.load(Ordering::Relaxed);
    let _ = cfg.wal_flusher_delay_ms.load(Ordering::Relaxed);
    let _ = cfg.deadlock_timeout_ms.load(Ordering::Relaxed);
    let _ = cfg.log_level.load(Ordering::Relaxed);
    let _ = cfg.synchronous_commit.load(Ordering::Relaxed);
    let _ = cfg.checkpoint_interval_ms.load(Ordering::Relaxed);
    let _ = cfg.transaction_timeout_ms.load(Ordering::Relaxed);

    // Modify and read back
    cfg.shared_buffers.store(200, Ordering::Relaxed);
    assert_eq!(cfg.shared_buffers.load(Ordering::Relaxed), 200);
}

// ============================================================
// Checkpoint 深度测试
// ============================================================

#[test]
fn test_checkpoint_with_wal_ring() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = Arc::new(WalWriter::new(&wal_dir));

    let ring = Arc::new(ferrisdb_storage::wal::ring_buffer::WalRingBuffer::new(1024));

    let mut mgr = CheckpointManager::new(CheckpointConfig::default(), writer);
    mgr.set_wal_ring(ring);

    let stats = mgr.checkpoint(CheckpointType::Online).unwrap();
    let _ = stats.duration_ms;
}

#[test]
fn test_checkpoint_recovery() {
    use ferrisdb_storage::wal::CheckpointRecovery;

    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    // Write some WAL
    let writer = WalWriter::new(&wal_dir);
    let ckpt = WalCheckpoint::new(100, false);
    writer.write(&ckpt.to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = CheckpointRecovery::new(&wal_dir);
    let _ = recovery.find_latest_checkpoint();
}

// ============================================================
// StorageManager 深度测试
// ============================================================

#[test]
fn test_smgr_write_read_by_page_id() {
    let td = TempDir::new().unwrap();
    let smgr = StorageManager::new(td.path());
    smgr.init().unwrap();

    let page_id = PageId::new(0, 0, 1, 0);
    let data = vec![0xEEu8; PAGE_SIZE];
    smgr.write_page_by_id(&page_id, &data).unwrap();

    let mut buf = vec![0u8; PAGE_SIZE];
    smgr.read_page_by_id(&page_id, &mut buf).unwrap();
    assert_eq!(buf[0], 0xEE);
}

#[test]
fn test_smgr_extend_relation() {
    let td = TempDir::new().unwrap();
    let smgr = StorageManager::new(td.path());
    smgr.init().unwrap();

    // Write pages in sequence to "extend" the relation
    for page_no in 0..5u32 {
        let tag = ferrisdb_core::BufferTag::new(ferrisdb_core::PdbId::new(0), 1, page_no);
        let data = vec![page_no as u8; PAGE_SIZE];
        smgr.write_page(&tag, &data).unwrap();
    }

    // Read back all
    for page_no in 0..5u32 {
        let tag = ferrisdb_core::BufferTag::new(ferrisdb_core::PdbId::new(0), 1, page_no);
        let mut buf = vec![0u8; PAGE_SIZE];
        smgr.read_page(&tag, &mut buf).unwrap();
        assert_eq!(buf[0], page_no as u8);
    }
}
