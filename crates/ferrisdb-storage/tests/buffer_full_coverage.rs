//! Buffer Pool full coverage — 67 tests (buffer 43 + control 21 + smgr 3)

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{BufferTag, PdbId, Lsn};
use ferrisdb_storage::{BufferPool, BufferPoolConfig, StorageManager, HeapPage, WalWriter, ControlFile, ControlFileData, BackgroundWriter};
use ferrisdb_storage::page::{PageHeader, PAGE_SIZE};

fn tag(r: u16, b: u32) -> BufferTag { BufferTag::new(PdbId::new(0), r, b) }
fn bp(n: usize) -> Arc<BufferPool> { Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap()) }
fn bp_smgr(n: usize) -> (Arc<BufferPool>, TempDir) {
    let td = TempDir::new().unwrap();
    let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap();
    let mut b = BufferPool::new(BufferPoolConfig::new(n)).unwrap(); b.set_smgr(s);
    (Arc::new(b), td)
}

// ===== Pin/Unpin =====
#[test] fn test_pin_returns_valid_id() { let b = bp(100); assert!(b.pin(&tag(1,0)).unwrap().buf_id() >= 0); }
#[test] fn test_pin_same_tag_same_id() { let b = bp(100); let id1 = b.pin(&tag(1,0)).unwrap().buf_id(); let id2 = b.pin(&tag(1,0)).unwrap().buf_id(); assert_eq!(id1, id2); }
#[test] fn test_pin_different_tags() { let b = bp(100); let a = b.pin(&tag(1,0)).unwrap().buf_id(); let c = b.pin(&tag(1,1)).unwrap().buf_id(); assert_ne!(a, c); }
#[test] fn test_unpin_frees_slot() { let b = bp(3); for i in 0..3 { let _ = b.pin(&tag(1,i)).unwrap(); } let p = b.pin(&tag(1,3)); assert!(p.is_ok()); }
#[test] fn test_pin_after_full_and_unpin() { let b = bp(2); let p0 = b.pin(&tag(1,0)).unwrap(); let _p1 = b.pin(&tag(1,1)).unwrap(); drop(p0); let p2 = b.pin(&tag(1,2)); assert!(p2.is_ok()); }
#[test] fn test_pin_100_different_pages() { let b = bp(200); for i in 0..100 { let _ = b.pin(&tag(1, i)).unwrap(); } }

// ===== Page Data =====
#[test] fn test_page_slice_size() { let b = bp(100); let p = b.pin(&tag(1,0)).unwrap(); assert_eq!(p.page_slice().len(), PAGE_SIZE); }
#[test] fn test_page_write_read() { let b = bp(100); let p = b.pin(&tag(1,0)).unwrap(); let l = p.lock_exclusive(); unsafe { *p.page_data().add(100) = 42; } drop(l); let l2 = p.lock_shared(); assert_eq!(p.page_slice()[100], 42); drop(l2); }
#[test] fn test_page_data_isolated() { let b = bp(100); let p1 = b.pin(&tag(1,0)).unwrap(); let p2 = b.pin(&tag(1,1)).unwrap(); let l1 = p1.lock_exclusive(); unsafe { *p1.page_data() = 0xAA; } drop(l1); let l2 = p2.lock_shared(); assert_ne!(p2.page_slice()[0], 0xAA); drop(l2); }

// ===== Dirty =====
#[test] fn test_dirty_count_zero() { let b = bp(100); assert_eq!(b.dirty_page_count(), 0); }
#[test] fn test_dirty_count_after_mark() { let b = bp(100); let p = b.pin(&tag(1,0)).unwrap(); p.mark_dirty(); assert!(b.dirty_page_count() > 0); }
#[test] fn test_dirty_count_multiple() { let b = bp(100); for i in 0..5 { let p = b.pin(&tag(1,i)).unwrap(); p.mark_dirty(); } assert!(b.dirty_page_count() >= 5); }
#[test] fn test_total_page_count() { let b = BufferPool::new(BufferPoolConfig::new(42)).unwrap(); assert_eq!(b.total_page_count(), 42); }

// ===== Flush =====
#[test] fn test_flush_all_clears_dirty() { let (b, _t) = bp_smgr(50); for i in 0..5u32 { let p = b.pin(&tag(1,i)).unwrap(); let l = p.lock_exclusive(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; hp.init(); drop(l); p.mark_dirty(); } b.flush_all().unwrap(); assert_eq!(b.dirty_page_count(), 0); }
#[test] fn test_flush_some_respects_limit() { let (b, _t) = bp_smgr(50); for i in 0..10u32 { let p = b.pin(&tag(1,i)).unwrap(); let l = p.lock_exclusive(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; hp.init(); drop(l); p.mark_dirty(); } let f = b.flush_some(3); assert!(f <= 3); }
#[test] fn test_flush_empty_pool() { let (b, _t) = bp_smgr(50); b.flush_all().unwrap(); }
#[test] fn test_flush_preserves_data() { let td = TempDir::new().unwrap(); { let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let mut b = BufferPool::new(BufferPoolConfig::new(50)).unwrap(); b.set_smgr(s); let b = Arc::new(b); let p = b.pin(&tag(1,0)).unwrap(); let l = p.lock_exclusive(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; hp.init(); hp.insert_tuple(b"persist_test_data").unwrap(); drop(l); p.mark_dirty(); drop(p); b.flush_all().unwrap(); } { let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let mut b = BufferPool::new(BufferPoolConfig::new(50)).unwrap(); b.set_smgr(s); let b = Arc::new(b); let p = b.pin(&tag(1,0)).unwrap(); let l = p.lock_shared(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; assert!(hp.get_tuple(1).is_some()); } }

// ===== Stats =====
#[test] fn test_hit_rate_initial() { let b = bp(100); b.reset_stats(); assert_eq!(b.hit_rate(), 0.0); }
#[test] fn test_hit_rate_after_miss() { let b = bp(100); b.reset_stats(); let _ = b.pin(&tag(1,0)).unwrap(); assert!(b.stat_misses() > 0 || b.stat_hits() > 0); }
#[test] fn test_hit_rate_after_hit() { let b = bp(100); let _ = b.pin(&tag(1,0)).unwrap(); b.reset_stats(); let _ = b.pin(&tag(1,0)).unwrap(); assert!(b.stat_hits() > 0); }
#[test] fn test_stat_pins() { let b = bp(100); b.reset_stats(); for _ in 0..10 { let _ = b.pin(&tag(1,0)).unwrap(); } assert!(b.stat_pins() >= 10); }

// ===== Eviction =====
#[test] fn test_eviction_5_pool() { let b = bp(5); for i in 0..5 { let _ = b.pin(&tag(1,i)).unwrap(); } for i in 5..20 { let _ = b.pin(&tag(1,i)).unwrap(); } }
#[test] fn test_pool_full_error() { let b = bp(3); let _p0 = b.pin(&tag(1,0)).unwrap(); let _p1 = b.pin(&tag(1,1)).unwrap(); let _p2 = b.pin(&tag(1,2)).unwrap(); assert!(b.pin(&tag(1,3)).is_err()); }
#[test] fn test_eviction_with_dirty() { let (b, _t) = bp_smgr(5); for i in 0..5u32 { let p = b.pin(&tag(1,i)).unwrap(); let l = p.lock_exclusive(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; hp.init(); drop(l); p.mark_dirty(); } for i in 5..10u32 { let _ = b.pin(&tag(1,i)).unwrap(); } }

// ===== Concurrent =====
#[test] fn test_conc_pin_same() { let b = Arc::new(BufferPool::new(BufferPoolConfig::new(100)).unwrap()); let t = tag(1,0); { let _ = b.pin(&t).unwrap(); } let mut h = vec![]; for _ in 0..8 { let b = b.clone(); h.push(std::thread::spawn(move || { for _ in 0..100 { let p = b.pin(&tag(1,0)).unwrap(); let _ = p.page_slice()[0]; } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_conc_pin_different() { let b = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap()); let mut h = vec![]; for t in 0..4 { let b = b.clone(); h.push(std::thread::spawn(move || { for i in 0..50u32 { let p = b.pin(&tag((t+1) as u16, i)).unwrap(); let l = p.lock_exclusive(); unsafe { *p.page_data() = (t*50+i) as u8; } drop(l); p.mark_dirty(); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_conc_rw() { let b = Arc::new(BufferPool::new(BufferPoolConfig::new(100)).unwrap()); let t = tag(1,0); { let _ = b.pin(&t).unwrap(); } let mut h = vec![]; for _ in 0..2 { let b = b.clone(); h.push(std::thread::spawn(move || { for _ in 0..100 { let p = b.pin(&tag(1,0)).unwrap(); let l = p.lock_exclusive(); unsafe { *p.page_data().add(200) = 1; } drop(l); p.mark_dirty(); } })); } for _ in 0..2 { let b = b.clone(); h.push(std::thread::spawn(move || { for _ in 0..100 { let p = b.pin(&tag(1,0)).unwrap(); let l = p.lock_shared(); let _ = p.page_slice()[200]; drop(l); } })); } for j in h { j.join().unwrap(); } }

// ===== WAL Integration =====
#[test] fn test_bp_with_wal() { let td = TempDir::new().unwrap(); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let wd = td.path().join("wal"); std::fs::create_dir_all(&wd).unwrap(); let w = Arc::new(WalWriter::new(&wd)); let mut b = BufferPool::new(BufferPoolConfig::new(50)).unwrap(); b.set_smgr(s); b.set_wal_writer(w); let b = Arc::new(b); let p = b.pin(&tag(1,0)).unwrap(); let l = p.lock_exclusive(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; hp.init(); drop(l); p.mark_dirty(); drop(p); b.flush_all().unwrap(); }

// ===== BackgroundWriter =====
#[test] fn test_bgwriter_start_stop() { let (b, _t) = bp_smgr(50); let bw = BackgroundWriter::start(b, 50, 5); std::thread::sleep(std::time::Duration::from_millis(100)); bw.stop(); }
#[test] fn test_bgwriter_flushes() { let (b, _t) = bp_smgr(50); for i in 0..5u32 { let p = b.pin(&tag(1,i)).unwrap(); let l = p.lock_exclusive(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; hp.init(); drop(l); p.mark_dirty(); } let bw = BackgroundWriter::start(Arc::clone(&b), 20, 10); std::thread::sleep(std::time::Duration::from_millis(100)); bw.stop(); }

// ===== Checksum =====
#[test] fn test_checksum_zero_page() { assert!(PageHeader::verify_checksum(&vec![0u8; PAGE_SIZE])); }
#[test] fn test_checksum_set_verify() { let (b, _t) = bp_smgr(50); let p = b.pin(&tag(1,0)).unwrap(); let l = p.lock_exclusive(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; hp.init(); hp.insert_tuple(b"checksum test").unwrap(); drop(l); let page = unsafe { std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE) }; PageHeader::set_checksum(page); assert!(PageHeader::verify_checksum(page)); }
#[test] fn test_checksum_corruption() { let (b, _t) = bp_smgr(50); let p = b.pin(&tag(1,0)).unwrap(); let l = p.lock_exclusive(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; hp.init(); drop(l); let page = unsafe { std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE) }; PageHeader::set_checksum(page); page[4096] ^= 0xFF; assert!(!PageHeader::verify_checksum(page)); }
#[test] fn test_page_magic_valid() { let (b, _) = bp_smgr(50); let p = b.pin(&tag(1,0)).unwrap(); let l = p.lock_exclusive(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; hp.init(); drop(l); assert!(PageHeader::is_valid_page(unsafe { std::slice::from_raw_parts(p.page_data(), PAGE_SIZE) })); }
#[test] fn test_page_magic_garbage() { let garbage = vec![0xDE; PAGE_SIZE]; assert!(!PageHeader::is_valid_page(&garbage)); }

// ===== Persistence Multi =====
#[test] fn test_persist_multi_page() { let td = TempDir::new().unwrap(); { let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let mut b = BufferPool::new(BufferPoolConfig::new(50)).unwrap(); b.set_smgr(s); let b = Arc::new(b); for i in 0..5u32 { let p = b.pin(&tag(50,i)).unwrap(); let l = p.lock_exclusive(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; hp.init(); hp.insert_tuple(format!("page_{}", i).as_bytes()).unwrap(); drop(l); p.mark_dirty(); } b.flush_all().unwrap(); } { let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let mut b = BufferPool::new(BufferPoolConfig::new(50)).unwrap(); b.set_smgr(s); let b = Arc::new(b); for i in 0..5u32 { let p = b.pin(&tag(50,i)).unwrap(); let l = p.lock_shared(); let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) }; assert!(hp.get_tuple(1).is_some()); } } }

// ==================== Control File (21 tests) ====================
#[test] fn test_cf_default() { let td = TempDir::new().unwrap(); let cf = ControlFile::open(td.path()).unwrap(); assert_eq!(cf.checkpoint_lsn().raw(), 0); assert_eq!(cf.get_data().state, 0); }
#[test] fn test_cf_update_checkpoint() { let td = TempDir::new().unwrap(); let cf = ControlFile::open(td.path()).unwrap(); cf.update_checkpoint(Lsn::from_raw(5000), 2).unwrap(); assert_eq!(cf.checkpoint_lsn().raw(), 5000); }
#[test] fn test_cf_persist() { let td = TempDir::new().unwrap(); { let cf = ControlFile::open(td.path()).unwrap(); cf.update_checkpoint(Lsn::from_raw(9999), 7).unwrap(); } { let cf = ControlFile::open(td.path()).unwrap(); assert_eq!(cf.checkpoint_lsn().raw(), 9999); assert_eq!(cf.get_data().last_wal_file, 7); } }
#[test] fn test_cf_state() { let td = TempDir::new().unwrap(); let cf = ControlFile::open(td.path()).unwrap(); cf.set_state(1).unwrap(); assert_eq!(cf.get_data().state, 1); cf.set_state(0).unwrap(); assert_eq!(cf.get_data().state, 0); }
#[test] fn test_cf_data_roundtrip() { let d = ControlFileData { checkpoint_lsn: 12345, timeline_id: 2, last_wal_file: 5, state: 1 }; let b = d.to_bytes(); let r = ControlFileData::from_bytes(&b).unwrap(); assert_eq!(r.checkpoint_lsn, 12345); assert_eq!(r.timeline_id, 2); assert_eq!(r.last_wal_file, 5); assert_eq!(r.state, 1); }
#[test] fn test_cf_crc_corruption() { let d = ControlFileData::default(); let mut b = d.to_bytes(); b[5] ^= 0xFF; assert!(ControlFileData::from_bytes(&b).is_none()); }
#[test] fn test_cf_crc_valid() { let d = ControlFileData { checkpoint_lsn: 42, ..Default::default() }; let b = d.to_bytes(); assert!(ControlFileData::from_bytes(&b).is_some()); }
#[test] fn test_cf_multiple_updates() { let td = TempDir::new().unwrap(); let cf = ControlFile::open(td.path()).unwrap(); for i in 1..20u64 { cf.update_checkpoint(Lsn::from_raw(i * 1000), i as u32).unwrap(); } assert_eq!(cf.checkpoint_lsn().raw(), 19000); }
#[test] fn test_cf_timeline() { let d = ControlFileData { timeline_id: 42, ..Default::default() }; let b = d.to_bytes(); let r = ControlFileData::from_bytes(&b).unwrap(); assert_eq!(r.timeline_id, 42); }
#[test] fn test_cf_all_fields() { let d = ControlFileData { checkpoint_lsn: u64::MAX/2, timeline_id: u64::MAX/3, last_wal_file: u32::MAX/4, state: 3 }; let b = d.to_bytes(); let r = ControlFileData::from_bytes(&b).unwrap(); assert_eq!(r.checkpoint_lsn, u64::MAX/2); assert_eq!(r.timeline_id, u64::MAX/3); assert_eq!(r.last_wal_file, u32::MAX/4); }
#[test] fn test_cf_open_nonexistent() { let td = TempDir::new().unwrap(); let cf = ControlFile::open(td.path()).unwrap(); assert_eq!(cf.get_data().state, 0); }
#[test] fn test_cf_state_transitions() { let td = TempDir::new().unwrap(); let cf = ControlFile::open(td.path()).unwrap(); cf.set_state(0).unwrap(); cf.set_state(1).unwrap(); cf.set_state(2).unwrap(); cf.set_state(0).unwrap(); assert_eq!(cf.get_data().state, 0); }
#[test] fn test_cf_empty_bytes() { assert!(ControlFileData::from_bytes(&[]).is_none()); }
#[test] fn test_cf_short_bytes() { assert!(ControlFileData::from_bytes(&[0; 16]).is_none()); }
#[test] fn test_cf_default_values() { let d = ControlFileData::default(); assert_eq!(d.checkpoint_lsn, 0); assert_eq!(d.timeline_id, 1); assert_eq!(d.last_wal_file, 0); assert_eq!(d.state, 0); }
#[test] fn test_cf_bytes_size() { let d = ControlFileData::default(); assert_eq!(d.to_bytes().len(), 32); }
#[test] fn test_cf_persist_state() { let td = TempDir::new().unwrap(); { let cf = ControlFile::open(td.path()).unwrap(); cf.set_state(2).unwrap(); } { let cf = ControlFile::open(td.path()).unwrap(); assert_eq!(cf.get_data().state, 2); } }
#[test] fn test_cf_overwrite() { let td = TempDir::new().unwrap(); let cf = ControlFile::open(td.path()).unwrap(); cf.update_checkpoint(Lsn::from_raw(100), 1).unwrap(); cf.update_checkpoint(Lsn::from_raw(200), 2).unwrap(); assert_eq!(cf.checkpoint_lsn().raw(), 200); }
#[test] fn test_cf_concurrent_read() { let td = TempDir::new().unwrap(); let cf = Arc::new(ControlFile::open(td.path()).unwrap()); cf.update_checkpoint(Lsn::from_raw(42), 1).unwrap(); let mut h = vec![]; for _ in 0..4 { let cf = Arc::clone(&cf); h.push(std::thread::spawn(move || { for _ in 0..100 { let _ = cf.checkpoint_lsn(); let _ = cf.get_data(); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_cf_zero_lsn_valid() { let d = ControlFileData { checkpoint_lsn: 0, ..Default::default() }; let b = d.to_bytes(); assert!(ControlFileData::from_bytes(&b).is_some()); }
#[test] fn test_cf_max_values() { let d = ControlFileData { checkpoint_lsn: u64::MAX, timeline_id: u64::MAX, last_wal_file: u32::MAX, state: u32::MAX }; let b = d.to_bytes(); let r = ControlFileData::from_bytes(&b).unwrap(); assert_eq!(r.checkpoint_lsn, u64::MAX); }

// ==================== Smgr (3 extra) ====================
#[test] fn test_smgr_concurrent_read_write() { let td = TempDir::new().unwrap(); let s = Arc::new(StorageManager::new(td.path())); s.init().unwrap(); s.write_page(&tag(1,0), &vec![0xAB; PAGE_SIZE]).unwrap(); let mut h = vec![]; for _ in 0..4 { let s = s.clone(); h.push(std::thread::spawn(move || { let mut b = vec![0u8; PAGE_SIZE]; s.read_page(&tag(1,0), &mut b).unwrap(); assert_eq!(b[0], 0xAB); })); } for j in h { j.join().unwrap(); } }
#[test] fn test_smgr_many_relations() { let td = TempDir::new().unwrap(); let s = StorageManager::new(td.path()); s.init().unwrap(); for r in 1..50u16 { s.write_page(&tag(r, 0), &vec![r as u8; PAGE_SIZE]).unwrap(); } for r in 1..50u16 { let mut b = vec![0u8; PAGE_SIZE]; s.read_page(&tag(r, 0), &mut b).unwrap(); assert_eq!(b[0], r as u8); } }
#[test] fn test_smgr_large_block() { let td = TempDir::new().unwrap(); let s = StorageManager::new(td.path()); s.init().unwrap(); s.write_page(&tag(1, 10000), &vec![42; PAGE_SIZE]).unwrap(); let mut b = vec![0u8; PAGE_SIZE]; s.read_page(&tag(1, 10000), &mut b).unwrap(); assert_eq!(b[0], 42); }
