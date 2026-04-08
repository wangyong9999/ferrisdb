//! WAL Recovery / Writer / Reader 覆盖测试
//! 目标: recovery.rs redo handlers, writer lifecycle, reader read_next

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{BufferTag, PdbId, PageId, Xid};
use ferrisdb_storage::*;
use ferrisdb_storage::wal::*;
use ferrisdb_storage::page::PAGE_SIZE;

// Re-export WalReader from wal module (from reader.rs)
use ferrisdb_storage::wal::WalReader;

// ============================================================
// Helper: 构建有效的 WAL 文件
// ============================================================

fn make_wal_dir(td: &TempDir) -> std::path::PathBuf {
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    wal_dir
}

fn make_smgr(td: &TempDir) -> Arc<StorageManager> {
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    smgr
}

fn write_empty_page(smgr: &StorageManager, rel: u16, page_no: u32) {
    let tag = BufferTag::new(PdbId::new(0), rel, page_no);
    let page = vec![0u8; PAGE_SIZE];
    smgr.write_page(&tag, &page).unwrap();
}


// ============================================================
// WalWriter 测试
// ============================================================

#[test]
fn test_wal_writer_create_and_write() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let writer = WalWriter::new(&wal_dir);

    let data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    let lsn = writer.write(&rec.serialize_with_data(&data)).unwrap();
    assert!(lsn.is_valid());

    assert!(writer.offset() > 40); // > header size
    assert_eq!(writer.file_no(), 0);
    let _ = writer.current_lsn();
    let _ = writer.wal_dir();
}

#[test]
fn test_wal_writer_sync() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let writer = WalWriter::new(&wal_dir);

    let data = vec![0xCDu8; 30];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
}

#[test]
fn test_wal_writer_multiple_records() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let writer = WalWriter::new(&wal_dir);

    for i in 0..20u32 {
        let data = vec![i as u8; 100];
        let rec = WalHeapInsert::new(PageId::new(0, 0, 1, i), 1, &data);
        writer.write(&rec.serialize_with_data(&data)).unwrap();
    }
    writer.sync().unwrap();
    assert!(writer.offset() > 2000);
}

#[test]
fn test_wal_writer_commit_abort_records() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let writer = WalWriter::new(&wal_dir);

    let commit = WalTxnCommit::new(Xid::new(0, 1), 100);
    writer.write(&commit.to_bytes()).unwrap();

    let abort = WalTxnCommit::new_abort(Xid::new(0, 2));
    writer.write(&abort.to_bytes()).unwrap();

    let checkpoint = WalCheckpoint::new(12345, false);
    writer.write(&checkpoint.to_bytes()).unwrap();

    let checkpoint_shutdown = WalCheckpoint::new(99999, true);
    writer.write(&checkpoint_shutdown.to_bytes()).unwrap();

    writer.sync().unwrap();
}

// ============================================================
// WalReader 测试
// ============================================================

#[test]
fn test_wal_reader_open_nonexistent() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let mut reader = WalReader::new(&wal_dir);
    assert!(reader.open(999).is_err());
}

#[test]
fn test_wal_reader_read_records() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);

    // Write some records
    let writer = WalWriter::new(&wal_dir);
    let data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    // Read back
    let mut reader = WalReader::new(&wal_dir);
    reader.open(0).unwrap();
    assert!(!reader.is_eof());

    let mut count = 0;
    while let Ok(Some((lsn, data))) = reader.read_next() {
        assert!(lsn.is_valid());
        assert!(!data.is_empty());
        count += 1;
        if count > 10 { break; } // safety
    }
    assert!(count >= 2, "Should read at least 2 records, got {}", count);

    let _ = reader.current_lsn();
    let _ = reader.offset();
    reader.close();
}

#[test]
fn test_wal_reader_seek() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);

    let writer = WalWriter::new(&wal_dir);
    let data = vec![0xCDu8; 30];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let mut reader = WalReader::new(&wal_dir);
    reader.open(0).unwrap();
    reader.seek(40).unwrap(); // seek to header
}

#[test]
fn test_wal_reader_seek_without_open() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let mut reader = WalReader::new(&wal_dir);
    assert!(reader.seek(100).is_err());
}

#[test]
fn test_wal_reader_read_without_open() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let mut reader = WalReader::new(&wal_dir);
    assert!(reader.read_next().is_err());
}

// ============================================================
// Recovery: Heap redo handlers
// ============================================================

#[test]
fn test_recovery_heap_insert_redo() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);

    // Create empty data page
    write_empty_page(&smgr, 1, 0);

    // Write WAL: heap insert
    let writer = WalWriter::new(&wal_dir);
    let tuple_data = vec![0xABu8; 100];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &tuple_data);
    writer.write(&rec.serialize_with_data(&tuple_data)).unwrap();

    // Write commit
    let commit = WalTxnCommit::new(Xid::new(0, 1), 100);
    writer.write(&commit.to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();

    let stats = recovery.stats();
    assert!(stats.records_redone > 0 || stats.records_skipped > 0);
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
}

#[test]
fn test_recovery_heap_delete_redo() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);
    write_empty_page(&smgr, 1, 0);

    let writer = WalWriter::new(&wal_dir);
    // First insert
    let tuple_data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &tuple_data);
    writer.write(&rec.serialize_with_data(&tuple_data)).unwrap();

    // Then delete
    let del = WalHeapDelete::new(PageId::new(0, 0, 1, 0), 1);
    writer.write(&del.to_bytes()).unwrap();

    let commit = WalTxnCommit::new(Xid::new(0, 1), 100);
    writer.write(&commit.to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
}

#[test]
fn test_recovery_heap_inplace_update_redo() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);
    write_empty_page(&smgr, 2, 0);

    let writer = WalWriter::new(&wal_dir);
    // Insert first
    let data = vec![0xAAu8; 64];
    let ins = WalHeapInsert::new(PageId::new(0, 0, 2, 0), 1, &data);
    writer.write(&ins.serialize_with_data(&data)).unwrap();

    // Inplace update
    let new_data = vec![0xBBu8; 64];
    let upd = WalHeapInplaceUpdate::new(PageId::new(0, 0, 2, 0), 1, &new_data);
    writer.write(&upd.serialize_with_data(&new_data)).unwrap();

    let commit = WalTxnCommit::new(Xid::new(0, 1), 100);
    writer.write(&commit.to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
}

#[test]
fn test_recovery_checkpoint_redo() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);

    let writer = WalWriter::new(&wal_dir);
    let ckpt = WalCheckpoint::new(100, true);
    writer.write(&ckpt.to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
}

#[test]
fn test_recovery_txn_commit_abort_tracking() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);

    let writer = WalWriter::new(&wal_dir);
    // Commit txn 1
    let commit = WalTxnCommit::new(Xid::new(0, 1), 100);
    writer.write(&commit.to_bytes()).unwrap();
    // Abort txn 2
    let abort = WalTxnCommit::new_abort(Xid::new(0, 2));
    writer.write(&abort.to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
}

#[test]
fn test_recovery_empty_wal_dir() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
}

#[test]
fn test_recovery_scan_only_mode() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);

    let writer = WalWriter::new(&wal_dir);
    let data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let smgr = make_smgr(&td);
    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    let stats = recovery.recover_scan_only().unwrap();
    assert!(stats.records_read > 0 || stats.records_redone > 0 || stats.records_skipped > 0);
}

#[test]
fn test_recovery_no_smgr_scan_only() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);

    let writer = WalWriter::new(&wal_dir);
    let data = vec![0u8; 30];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::new(&wal_dir);
    let _ = recovery.recover_scan_only();
    let _ = recovery.stage();
    let _ = recovery.checkpoint_lsn();
}

#[test]
fn test_recovery_checkpoint_recovery_mode() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);

    let writer = WalWriter::new(&wal_dir);
    // Write checkpoint first
    let ckpt = WalCheckpoint::new(100, false);
    writer.write(&ckpt.to_bytes()).unwrap();
    // Write data after checkpoint
    let data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    write_empty_page(&smgr, 1, 0);
    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CheckpointRecovery).unwrap();
}

// ============================================================
// Recovery: Undo + uncommitted rollback
// ============================================================

#[test]
fn test_recovery_undo_insert_rollback() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);
    write_empty_page(&smgr, 1, 0);

    let writer = WalWriter::new(&wal_dir);

    // Write heap insert (for page to have data)
    let tuple_data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &tuple_data);
    writer.write(&rec.serialize_with_data(&tuple_data)).unwrap();

    // Write undo record for uncommitted txn
    let undo_header = WalRecordHeader::new(WalRecordType::UndoInsertRecord, 23);
    let hdr_bytes = unsafe {
        std::slice::from_raw_parts(
            &undo_header as *const _ as *const u8,
            std::mem::size_of::<WalRecordHeader>(),
        )
    };
    let mut undo_payload = Vec::new();
    undo_payload.extend_from_slice(&10u64.to_le_bytes()); // xid = 10
    undo_payload.push(0); // action_type = Insert
    undo_payload.extend_from_slice(&1u32.to_le_bytes()); // table_oid
    undo_payload.extend_from_slice(&0u32.to_le_bytes()); // page_no
    undo_payload.extend_from_slice(&1u16.to_le_bytes()); // offset
    undo_payload.extend_from_slice(&0u32.to_le_bytes()); // data_len

    let mut full = Vec::new();
    full.extend_from_slice(hdr_bytes);
    full.extend_from_slice(&undo_payload);
    writer.write(&full).unwrap();

    // No commit for xid=10 → should be rolled back
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
}

#[test]
fn test_recovery_undo_delete_rollback() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);
    write_empty_page(&smgr, 1, 0);

    let writer = WalWriter::new(&wal_dir);

    // Undo delete record with old data
    let undo_header = WalRecordHeader::new(WalRecordType::UndoDeleteRecord, 73);
    let hdr_bytes = unsafe {
        std::slice::from_raw_parts(
            &undo_header as *const _ as *const u8,
            std::mem::size_of::<WalRecordHeader>(),
        )
    };
    let mut undo_payload = Vec::new();
    undo_payload.extend_from_slice(&20u64.to_le_bytes()); // xid = 20
    undo_payload.push(2); // action_type = Delete
    undo_payload.extend_from_slice(&1u32.to_le_bytes()); // table_oid
    undo_payload.extend_from_slice(&0u32.to_le_bytes()); // page_no
    undo_payload.extend_from_slice(&1u16.to_le_bytes()); // offset
    undo_payload.extend_from_slice(&50u32.to_le_bytes()); // data_len = 50
    undo_payload.extend_from_slice(&vec![0xABu8; 50]); // old data

    let mut full = Vec::new();
    full.extend_from_slice(hdr_bytes);
    full.extend_from_slice(&undo_payload);
    writer.write(&full).unwrap();

    // No commit → rolled back
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
}

// ============================================================
// Recovery: BTree redo
// ============================================================

// BTree insert leaf redo test omitted: BTreePage::from_bytes requires 8KB alignment
// which recovery handles internally via AlignedPageBuf but involves complex WAL
// record construction. Covered through integration tests in btree_tests.

#[test]
fn test_recovery_ddl_record() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);

    let writer = WalWriter::new(&wal_dir);
    // DDL create record
    let mut hdr = WalRecordHeader::new(WalRecordType::DdlCreate, 4);
    let payload = [0u8; 4];
    hdr.compute_crc(&payload);
    let hdr_bytes = unsafe {
        std::slice::from_raw_parts(
            &hdr as *const WalRecordHeader as *const u8,
            std::mem::size_of::<WalRecordHeader>(),
        )
    };
    let mut full = Vec::new();
    full.extend_from_slice(hdr_bytes);
    full.extend_from_slice(&payload);
    writer.write(&full).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
}

// ============================================================
// Recovery: CRC verification
// ============================================================

#[test]
fn test_recovery_crc_verified() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);

    // Write a valid record with CRC
    let writer = WalWriter::new(&wal_dir);
    let data = vec![0xABu8; 50];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    // Corrupt the CRC in the WAL file
    let wal_files: Vec<_> = std::fs::read_dir(&wal_dir).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .collect();
    if !wal_files.is_empty() {
        let mut data = std::fs::read(wal_files[0].path()).unwrap();
        if data.len() > 44 {
            // Corrupt CRC field (at offset 40+4 = within record header)
            data[44] ^= 0xFF;
            std::fs::write(wal_files[0].path(), &data).unwrap();
        }
    }

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    // Should not panic, may stop at corrupted record
    let _ = recovery.recover(RecoveryMode::CrashRecovery);
}

// ============================================================
// WalRecordHeader CRC compute + verify
// ============================================================

#[test]
fn test_wal_record_header_crc_roundtrip() {
    let data = vec![0xABu8; 100];
    let mut hdr = WalRecordHeader::new(WalRecordType::HeapInsert, data.len() as u16);
    hdr.compute_crc(&data);
    let crc_val = { hdr.crc };
    assert_ne!(crc_val, 0);
    assert!(hdr.verify_crc(&data));
    assert!(!hdr.verify_crc(&[0u8; 100])); // different data
}

#[test]
fn test_wal_record_header_all_types() {
    for rtype in [
        WalRecordType::HeapBatchInsert,
        WalRecordType::HeapSamePageAppend,
        WalRecordType::HeapAnotherPageAppendUpdateNewPage,
        WalRecordType::HeapAnotherPageAppendUpdateOldPage,
        WalRecordType::HeapAllocTd,
        WalRecordType::HeapPrune,
        WalRecordType::BtreeBuild,
        WalRecordType::BtreeInitMetaPage,
        WalRecordType::BtreeUpdateMetaRoot,
        WalRecordType::BtreeInsertOnInternal,
        WalRecordType::BtreeSplitInternal,
        WalRecordType::BtreeSplitLeaf,
        WalRecordType::BtreeDeleteOnInternal,
        WalRecordType::BtreeDeleteOnLeaf,
        WalRecordType::HeapPageFull,
        WalRecordType::NextCsn,
        WalRecordType::BarrierCsn,
        WalRecordType::DdlDrop,
    ] {
        let hdr = WalRecordHeader::new(rtype, 0);
        assert_eq!(hdr.record_type(), Some(rtype));
    }
}

// ============================================================
// WalFileHeader 序列化
// ============================================================

#[test]
fn test_wal_file_header_roundtrip() {
    let hdr = WalFileHeader::new(12345, 1);
    let bytes = hdr.to_bytes();
    let hdr2 = WalFileHeader::from_bytes(&bytes);
    // Packed struct: copy fields to locals to avoid UB
    let magic = { hdr2.magic };
    let plsn = { hdr2.start_plsn };
    let tid = { hdr2.timeline_id };
    let ver = { hdr2.version };
    assert_eq!(magic, WAL_FILE_MAGIC);
    assert_eq!(plsn, 12345);
    assert_eq!(tid, 1);
    assert_eq!(ver, 1);
    assert_eq!(WalFileHeader::size(), 40);
}

// ============================================================
// Recovery: multiple WAL files
// ============================================================

#[test]
fn test_recovery_multiple_wal_files() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);

    // Create two WAL files manually
    for file_no in 0..2u32 {
        let file_name = format!("{:08X}.wal", file_no);
        let path = wal_dir.join(file_name);

        let hdr = WalFileHeader::new(0, 1);
        let hdr_bytes = hdr.to_bytes();

        let mut file = std::fs::File::create(&path).unwrap();
        use std::io::Write;
        file.write_all(&hdr_bytes).unwrap();

        // Write a commit record after header
        let commit = WalTxnCommit::new(Xid::new(0, file_no + 1), 100);
        let commit_bytes = commit.to_bytes();
        file.write_all(&commit_bytes).unwrap();
    }

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
}

// ============================================================
// Recovery stage tracking
// ============================================================

#[test]
fn test_recovery_stage_transitions() {
    let td = TempDir::new().unwrap();
    let wal_dir = make_wal_dir(&td);
    let smgr = make_smgr(&td);

    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    assert_eq!(recovery.stage(), RecoveryStage::NotStarted);

    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert_eq!(recovery.stage(), RecoveryStage::Completed);
}
