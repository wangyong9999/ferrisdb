//! 故障注入 + 错误路径覆盖

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

fn make_page_payload(rtype: WalRecordType, page_id: PageId, extra: &[u8]) -> Vec<u8> {
    let page_rec = WalRecordForPage::new(rtype, page_id, extra.len() as u16);
    let bytes = unsafe {
        std::slice::from_raw_parts(
            &page_rec as *const WalRecordForPage as *const u8,
            std::mem::size_of::<WalRecordForPage>(),
        )
    };
    let mut buf = Vec::new();
    buf.extend_from_slice(bytes);
    buf.extend_from_slice(extra);
    buf
}

fn write_wal_record(writer: &WalWriter, rtype: WalRecordType, payload: &[u8]) {
    let mut hdr = WalRecordHeader::new(rtype, payload.len() as u16);
    hdr.compute_crc(payload);
    let hdr_bytes = unsafe {
        std::slice::from_raw_parts(
            &hdr as *const WalRecordHeader as *const u8,
            std::mem::size_of::<WalRecordHeader>(),
        )
    };
    let mut full = Vec::new();
    full.extend_from_slice(hdr_bytes);
    full.extend_from_slice(payload);
    writer.write(&full).unwrap();
}

fn write_initialized_heap_page(smgr: &StorageManager, rel: u16, page_no: u32) {
    #[repr(C, align(8192))]
    struct A { data: [u8; PAGE_SIZE] }
    let mut a = A { data: [0u8; PAGE_SIZE] };
    let hp = page::HeapPage::from_bytes(&mut a.data);
    hp.init();
    hp.insert_tuple(&[0xABu8; 100]);
    hp.insert_tuple(&[0xCDu8; 80]);
    smgr.write_page(&BufferTag::new(PdbId::new(0), rel, page_no), &a.data).unwrap();
}

// ============================================================
// Recovery: redo delete/update on pre-populated pages
// ============================================================

#[test]
fn test_redo_heap_delete_on_real_page() {
    let td = TempDir::new().unwrap();
    let wd = wal_dir(&td);
    let sm = smgr(&td);
    write_initialized_heap_page(&sm, 1, 0);

    let writer = WalWriter::new(&wd);
    let pid = PageId::new(0, 0, 1, 0);

    let mut extra = Vec::new();
    extra.extend_from_slice(&1u16.to_le_bytes());
    let payload = make_page_payload(WalRecordType::HeapDelete, pid, &extra);
    write_wal_record(&writer, WalRecordType::HeapDelete, &payload);
    writer.write(&WalTxnCommit::new(Xid::new(0, 1), 100).to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wd, sm);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert!(recovery.stats().records_redone > 0);
}

#[test]
fn test_redo_heap_inplace_update_on_real_page() {
    let td = TempDir::new().unwrap();
    let wd = wal_dir(&td);
    let sm = smgr(&td);
    write_initialized_heap_page(&sm, 2, 0);

    let writer = WalWriter::new(&wd);
    let pid = PageId::new(0, 0, 2, 0);

    let upd = vec![0xEEu8; 80];
    let mut extra = Vec::new();
    extra.extend_from_slice(&1u16.to_le_bytes());
    extra.extend_from_slice(&(upd.len() as u16).to_le_bytes());
    extra.extend_from_slice(&upd);
    let payload = make_page_payload(WalRecordType::HeapInplaceUpdate, pid, &extra);
    write_wal_record(&writer, WalRecordType::HeapInplaceUpdate, &payload);
    writer.write(&WalTxnCommit::new(Xid::new(0, 1), 100).to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wd, sm);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
}

// ============================================================
// Recovery: short/truncated payload error paths
// ============================================================

#[test]
fn test_redo_all_short_payloads() {
    let td = TempDir::new().unwrap();
    let wd = wal_dir(&td);
    let sm = smgr(&td);

    let writer = WalWriter::new(&wd);
    // Write records with too-short payloads → triggers Ok(false) in redo handlers
    for rtype in [WalRecordType::HeapInsert, WalRecordType::HeapDelete,
                  WalRecordType::HeapInplaceUpdate, WalRecordType::HeapPrune] {
        let mut hdr = WalRecordHeader::new(rtype, 2);
        hdr.compute_crc(&[0, 0]);
        let hdr_bytes = unsafe {
            std::slice::from_raw_parts(
                &hdr as *const WalRecordHeader as *const u8,
                std::mem::size_of::<WalRecordHeader>(),
            )
        };
        let mut full = Vec::new();
        full.extend_from_slice(hdr_bytes);
        full.extend_from_slice(&[0, 0]);
        writer.write(&full).unwrap();
    }
    writer.sync().unwrap();
    drop(writer);

    let recovery = WalRecovery::with_smgr(&wd, sm);
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
}

// ============================================================
// WAL Writer: multi-switch + close + read error
// ============================================================

#[test]
fn test_writer_lifecycle_full() {
    let td = TempDir::new().unwrap();
    let wd = td.path().join("wal");
    std::fs::create_dir_all(&wd).unwrap();
    let writer = WalWriter::new(&wd);

    for _ in 0..3 {
        writer.write(&[0xAB; 100]).unwrap();
        writer.switch_file().unwrap();
    }
    assert!(writer.file_no() >= 3);

    // Read from wrong file
    let bad_lsn = ferrisdb_core::Lsn::from_parts(999, 100);
    assert!(writer.read(bad_lsn).is_err());

    writer.close().unwrap();
}

// ============================================================
// Checkpoint + Archive integration
// ============================================================

struct MockFlusher;
impl DirtyPageFlusher for MockFlusher {
    fn flush_dirty_pages(&self, _lsn: ferrisdb_core::Lsn) -> ferrisdb_core::Result<u64> { Ok(2) }
    fn dirty_page_count(&self) -> u64 { 5 }
    fn total_page_count(&self) -> u64 { 50 }
}

#[test]
fn test_checkpoint_archive_cleanup() {
    let td = TempDir::new().unwrap();
    let wd = td.path().join("wal");
    std::fs::create_dir_all(&wd).unwrap();

    let writer = Arc::new(WalWriter::new(&wd));
    writer.write(&[0xAB; 100]).unwrap();
    writer.switch_file().unwrap();
    writer.write(&[0xCD; 100]).unwrap();
    writer.switch_file().unwrap();
    writer.write(&[0xEF; 100]).unwrap();
    writer.sync().unwrap();

    let archiver = Arc::new(ferrisdb_storage::wal::archive::WalArchiver::new(
        ferrisdb_storage::wal::archive::WalArchiveConfig {
            archive_dir: td.path().join("archive"),
            enabled: true,
            use_hardlink: false,
        },
    ));
    archiver.init().unwrap();

    let mut mgr = CheckpointManager::new(CheckpointConfig::default(), writer);
    mgr.set_flusher(Arc::new(MockFlusher));
    mgr.set_archiver(archiver.clone());
    mgr.checkpoint(CheckpointType::Online).unwrap();
}

// ============================================================
// Buffer: eviction dirty without smgr
// ============================================================

#[test]
fn test_pool_evict_dirty_no_smgr() {
    let bp = BufferPool::new(BufferPoolConfig::new(3)).unwrap();
    for i in 0..3u32 {
        let tag = BufferTag::new(PdbId::new(0), 1, i);
        let pinned = bp.pin(&tag).unwrap();
        bp.mark_dirty(pinned.buf_id());
        drop(pinned);
    }
    // Eviction: dirty pages without smgr
    for i in 3..6u32 {
        let tag = BufferTag::new(PdbId::new(0), 1, i);
        let _ = bp.pin(&tag);
    }
}

// ============================================================
// StorageManager: sync + num_pages + extend
// ============================================================

#[test]
fn test_smgr_full_lifecycle() {
    let td = TempDir::new().unwrap();
    let sm = StorageManager::new(td.path());
    sm.init().unwrap();

    let tag = BufferTag::new(PdbId::new(0), 1, 0);
    sm.write_page(&tag, &vec![0xABu8; PAGE_SIZE]).unwrap();
    sm.sync(&tag).unwrap();
    assert!(sm.num_pages(&tag).unwrap() >= 1);

    let ext = sm.extend(&tag).unwrap();
    assert!(ext >= 0);
}
