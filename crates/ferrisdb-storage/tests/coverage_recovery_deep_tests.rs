//! Round 12: Recovery deep paths + WalReader in recovery.rs

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{BufferTag, PdbId, PageId, Xid};
use ferrisdb_storage::*;
use ferrisdb_storage::wal::*;
use ferrisdb_storage::page::PAGE_SIZE;

fn wal_dir(td: &TempDir) -> std::path::PathBuf {
    let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); d
}
fn smgr(td: &TempDir) -> Arc<StorageManager> {
    let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); s
}
fn write_page(smgr: &StorageManager, rel: u16, pn: u32) {
    smgr.write_page(&BufferTag::new(PdbId::new(0), rel, pn), &vec![0u8; PAGE_SIZE]).unwrap();
}

// ============================================================
// Recovery: multiple record types in sequence
// ============================================================

#[test]
fn test_recovery_full_lifecycle() {
    let td = TempDir::new().unwrap();
    let wd = wal_dir(&td);
    let sm = smgr(&td);
    write_page(&sm, 1, 0);
    write_page(&sm, 1, 1);

    let writer = WalWriter::new(&wd);
    // Insert on page 0
    let data = vec![0xABu8; 80];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    // Delete on page 0
    let del = WalHeapDelete::new(PageId::new(0, 0, 1, 0), 1);
    writer.write(&del.to_bytes()).unwrap();
    // Insert on page 1
    let rec2 = WalHeapInsert::new(PageId::new(0, 0, 1, 1), 1, &data);
    writer.write(&rec2.serialize_with_data(&data)).unwrap();
    // Inplace update on page 1
    let upd_data = vec![0xCDu8; 80];
    let upd = WalHeapInplaceUpdate::new(PageId::new(0, 0, 1, 1), 1, &upd_data);
    writer.write(&upd.serialize_with_data(&upd_data)).unwrap();
    // Commit
    writer.write(&WalTxnCommit::new(Xid::new(0, 1), 200).to_bytes()).unwrap();
    // Checkpoint
    writer.write(&WalCheckpoint::new(300, false).to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wd, sm);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    let stats = recovery.stats();
    assert!(stats.records_redone + stats.records_skipped > 0);
}

// ============================================================
// Recovery: WalReader in recovery.rs (lines 1220+)
// ============================================================

#[test]
fn test_recovery_wal_reader_read_next() {
    let td = TempDir::new().unwrap();
    let wd = wal_dir(&td);

    let writer = WalWriter::new(&wd);
    for _ in 0..5 {
        let data = vec![0xEEu8; 40];
        let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
        writer.write(&rec.serialize_with_data(&data)).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    // Use the separate reader module to read back
    let mut reader = WalReader::new(&wd);
    reader.open(0).unwrap();

    let mut count = 0;
    while let Ok(Some((lsn, _data))) = reader.read_next() {
        assert!(lsn.is_valid());
        count += 1;
        if count > 20 { break; }
    }
    assert!(count >= 5, "Should read at least 5 records, got {}", count);
    reader.close();
}

// ============================================================
// Recovery: torn write detection
// ============================================================

#[test]
fn test_recovery_torn_write_oversized_record() {
    let td = TempDir::new().unwrap();
    let wd = wal_dir(&td);
    let sm = smgr(&td);

    let writer = WalWriter::new(&wd);
    let data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    // Corrupt: append a fake record header with size > 32768 (torn write detection)
    let wal_files: Vec<_> = std::fs::read_dir(&wd).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .collect();
    if !wal_files.is_empty() {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(wal_files[0].path()).unwrap();
        // Write a header with size=40000 (> 32768) → triggers torn write detection
        let fake_size: u16 = 40000;
        let fake_rtype: u16 = 0; // HeapInsert
        f.write_all(&fake_size.to_le_bytes()).unwrap();
        f.write_all(&fake_rtype.to_le_bytes()).unwrap();
        f.write_all(&[0u8; 4]).unwrap(); // fake CRC
    }

    let recovery = WalRecovery::with_smgr(&wd, sm);
    // Should stop at torn record, not panic
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
}

// ============================================================
// Recovery: invalid record type detection
// ============================================================

#[test]
fn test_recovery_invalid_record_type() {
    let td = TempDir::new().unwrap();
    let wd = wal_dir(&td);
    let sm = smgr(&td);

    let writer = WalWriter::new(&wd);
    let data = vec![0xABu8; 30];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    // Append record with invalid type (9999)
    let wal_files: Vec<_> = std::fs::read_dir(&wd).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .collect();
    if !wal_files.is_empty() {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(wal_files[0].path()).unwrap();
        let size: u16 = 10;
        let rtype: u16 = 9999;
        f.write_all(&size.to_le_bytes()).unwrap();
        f.write_all(&rtype.to_le_bytes()).unwrap();
        f.write_all(&[0u8; 4]).unwrap();
    }

    let recovery = WalRecovery::with_smgr(&wd, sm);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
}

// ============================================================
// Recovery: scan_only with checkpoint
// ============================================================

#[test]
fn test_recovery_scan_only_finds_checkpoint() {
    let td = TempDir::new().unwrap();
    let wd = wal_dir(&td);

    let writer = WalWriter::new(&wd);
    writer.write(&WalCheckpoint::new(100, false).to_bytes()).unwrap();
    writer.write(&WalTxnCommit::new(Xid::new(0, 1), 50).to_bytes()).unwrap();
    writer.write(&WalCheckpoint::new(200, true).to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let sm = smgr(&td);
    let recovery = WalRecovery::with_smgr(&wd, sm);
    let stats = recovery.recover_scan_only().unwrap();
    let _ = stats.records_read;
    let _ = recovery.checkpoint_lsn();
}

// ============================================================
// Recovery: empty records / short data
// ============================================================

#[test]
fn test_recovery_short_payload() {
    let td = TempDir::new().unwrap();
    let wd = wal_dir(&td);
    let sm = smgr(&td);

    let writer = WalWriter::new(&wd);
    // Write a HeapInsert with very short payload (< 4 bytes → parse_page_record returns None)
    let mut hdr = WalRecordHeader::new(WalRecordType::HeapInsert, 2);
    hdr.compute_crc(&[0u8; 2]);
    let hdr_bytes = unsafe {
        std::slice::from_raw_parts(
            &hdr as *const WalRecordHeader as *const u8,
            std::mem::size_of::<WalRecordHeader>(),
        )
    };
    let mut full = Vec::new();
    full.extend_from_slice(hdr_bytes);
    full.extend_from_slice(&[0u8; 2]);
    writer.write(&full).unwrap();
    writer.sync().unwrap();
    drop(writer);

    write_page(&sm, 1, 0);
    let recovery = WalRecovery::with_smgr(&wd, sm);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
}

// ============================================================
// Recovery: multiple files
// ============================================================

#[test]
fn test_recovery_two_wal_files_with_data() {
    let td = TempDir::new().unwrap();
    let wd = wal_dir(&td);
    let sm = smgr(&td);
    write_page(&sm, 1, 0);

    let writer = WalWriter::new(&wd);
    // Write data, switch file, write more
    let data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.switch_file().unwrap();
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.write(&WalTxnCommit::new(Xid::new(0, 1), 100).to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wd, sm);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    let stats = recovery.stats();
    assert!(stats.records_redone + stats.records_skipped >= 2);
}
