//! 生产代码覆盖 — 存储层未触达路径

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{BufferTag, PdbId, PageId, Xid, Lsn};
use ferrisdb_storage::*;
use ferrisdb_storage::wal::*;
use ferrisdb_storage::page::PAGE_SIZE;

// ============================================================
// recovery.rs: redo handler 实际执行路径
// ============================================================

fn init_heap_page_on_disk(smgr: &StorageManager, rel: u16, page_no: u32, tuple_count: usize) {
    #[repr(C, align(8192))]
    struct A { data: [u8; PAGE_SIZE] }
    let mut a = A { data: [0u8; PAGE_SIZE] };
    let hp = page::HeapPage::from_bytes(&mut a.data);
    hp.init();
    for _ in 0..tuple_count {
        hp.insert_tuple(&[0xABu8; 80]);
    }
    smgr.write_page(&BufferTag::new(PdbId::new(0), rel, page_no), &a.data).unwrap();
}

fn make_page_payload(rtype: WalRecordType, pid: PageId, extra: &[u8]) -> Vec<u8> {
    let pr = WalRecordForPage::new(rtype, pid, extra.len() as u16);
    let b = unsafe { std::slice::from_raw_parts(&pr as *const _ as *const u8, std::mem::size_of::<WalRecordForPage>()) };
    let mut v = Vec::new();
    v.extend_from_slice(b);
    v.extend_from_slice(extra);
    v
}

fn write_typed_record(w: &WalWriter, rtype: WalRecordType, payload: &[u8]) {
    let mut h = WalRecordHeader::new(rtype, payload.len() as u16);
    h.compute_crc(payload);
    let hb = unsafe { std::slice::from_raw_parts(&h as *const _ as *const u8, std::mem::size_of::<WalRecordHeader>()) };
    let mut f = Vec::new();
    f.extend_from_slice(hb);
    f.extend_from_slice(payload);
    w.write(&f).unwrap();
}

/// redo HeapDelete 在有 tuple 的页面上实际执行 mark_dead
#[test]
fn test_redo_delete_executes_mark_dead() {
    let td = TempDir::new().unwrap();
    let wd = td.path().join("wal");
    std::fs::create_dir_all(&wd).unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    init_heap_page_on_disk(&smgr, 1, 0, 3);

    let w = WalWriter::new(&wd);
    let pid = PageId::new(0, 0, 1, 0);
    let mut ex = Vec::new();
    ex.extend_from_slice(&1u16.to_le_bytes());
    let payload = make_page_payload(WalRecordType::HeapDelete, pid, &ex);
    write_typed_record(&w, WalRecordType::HeapDelete, &payload);
    w.write(&WalTxnCommit::new(Xid::new(0, 1), 100).to_bytes()).unwrap();
    w.sync().unwrap();
    drop(w);

    let r = WalRecovery::with_smgr(&wd, smgr);
    r.recover(RecoveryMode::CrashRecovery).unwrap();
}

/// redo HeapInplaceUpdate 在有 tuple 的页面上实际覆盖数据
#[test]
fn test_redo_inplace_update_executes() {
    let td = TempDir::new().unwrap();
    let wd = td.path().join("wal");
    std::fs::create_dir_all(&wd).unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    init_heap_page_on_disk(&smgr, 2, 0, 2);

    let w = WalWriter::new(&wd);
    let pid = PageId::new(0, 0, 2, 0);
    let upd = vec![0xEEu8; 80];
    let mut ex = Vec::new();
    ex.extend_from_slice(&1u16.to_le_bytes());
    ex.extend_from_slice(&(upd.len() as u16).to_le_bytes());
    ex.extend_from_slice(&upd);
    let payload = make_page_payload(WalRecordType::HeapInplaceUpdate, pid, &ex);
    write_typed_record(&w, WalRecordType::HeapInplaceUpdate, &payload);
    w.write(&WalTxnCommit::new(Xid::new(0, 1), 100).to_bytes()).unwrap();
    w.sync().unwrap();
    drop(w);

    let r = WalRecovery::with_smgr(&wd, smgr);
    r.recover(RecoveryMode::CrashRecovery).unwrap();
}

// ============================================================
// writer.rs: read + wait_for_lsn + switch
// ============================================================

#[test]
fn test_writer_read_and_switch() {
    let td = TempDir::new().unwrap();
    let wd = td.path().join("wal");
    std::fs::create_dir_all(&wd).unwrap();
    let w = WalWriter::new(&wd);

    // Write a proper WAL record
    let rec = WalHeapInsert::new(PageId::new(0,0,1,0), 1, &[0xABu8; 50]);
    let lsn = w.write(&rec.serialize_with_data(&[0xABu8; 50])).unwrap();
    w.sync().unwrap();
    let data = w.read(lsn).unwrap();
    assert!(!data.is_empty());

    w.switch_file().unwrap();
    w.write(&[0xCDu8; 100]).unwrap();
    w.sync().unwrap();

    // Read from old file should fail
    assert!(w.read(lsn).is_err());

    w.close().unwrap();
}

#[test]
fn test_writer_wait_for_lsn_fallback() {
    let td = TempDir::new().unwrap();
    let wd = td.path().join("wal");
    std::fs::create_dir_all(&wd).unwrap();
    let w = WalWriter::new(&wd);

    let lsn = w.write(&[0xABu8; 100]).unwrap();
    // wait_for_lsn without prior sync → triggers fallback sync
    w.wait_for_lsn(lsn).unwrap();
    assert!(w.flushed_lsn().raw() >= lsn.raw());
}

// ============================================================
// checkpoint.rs: stats + concurrent checkpoint error
// ============================================================

struct NoopFlusher;
impl DirtyPageFlusher for NoopFlusher {
    fn flush_dirty_pages(&self, _: Lsn) -> ferrisdb_core::Result<u64> { Ok(0) }
    fn dirty_page_count(&self) -> u64 { 0 }
    fn total_page_count(&self) -> u64 { 100 }
}

#[test]
fn test_checkpoint_stats_and_state() {
    let td = TempDir::new().unwrap();
    let wd = td.path().join("wal");
    std::fs::create_dir_all(&wd).unwrap();
    let w = Arc::new(WalWriter::new(&wd));

    let mgr = CheckpointManager::new(CheckpointConfig::default(), w);
    let stats = mgr.stats();
    let _ = stats.checkpoint_lsn;
    assert_eq!(mgr.state(), CheckpointState::Idle);
}

// ============================================================
// smgr: file eviction + extend + sync
// ============================================================

#[test]
fn test_smgr_extend_and_sync() {
    let td = TempDir::new().unwrap();
    let sm = StorageManager::new(td.path());
    sm.init().unwrap();

    let tag = BufferTag::new(PdbId::new(0), 1, 0);
    let n1 = sm.extend(&tag).unwrap();
    let n2 = sm.extend(&tag).unwrap();
    assert!(n2 > n1);

    sm.write_page(&tag, &vec![0xABu8; PAGE_SIZE]).unwrap();
    sm.sync(&tag).unwrap();
    assert!(sm.num_pages(&tag).unwrap() >= 2);
}

// ============================================================
// pool: buffer_count=0 error + page_slice_mut
// ============================================================

#[test]
fn test_pool_zero_buffers_error() {
    let result = BufferPool::new(BufferPoolConfig::new(0));
    assert!(result.is_err());
}

// ============================================================
// wal/record.rs: all WAL record types serialization
// ============================================================

#[test]
fn test_wal_record_serialization() {
    let pid = PageId::new(0, 0, 1, 0);

    // HeapUpdateOldPage
    let old = WalHeapUpdateOldPage::new(pid, 1);
    let _ = old.serialize_with_header(&[0u8; 32]);

    // WalCheckpoint shutdown
    let ckpt = WalCheckpoint::new(12345, true);
    let _ = ckpt.to_bytes();
    // WalCheckpoint online
    let ckpt2 = WalCheckpoint::new(54321, false);
    let _ = ckpt2.to_bytes();

    // WalAtomicGroupHeader
    let agh = WalAtomicGroupHeader::new(Xid::new(0, 1));
    assert_eq!(WalAtomicGroupHeader::header_size(), std::mem::size_of::<WalAtomicGroupHeader>());
    let _ = agh;

    // WalRecordHeader CRC
    let mut hdr = WalRecordHeader::new(WalRecordType::HeapInsert, 50);
    hdr.compute_crc(&[0u8; 50]);
    assert!(hdr.verify_crc(&[0u8; 50]));
    assert!(!hdr.verify_crc(&[1u8; 50]));
}
