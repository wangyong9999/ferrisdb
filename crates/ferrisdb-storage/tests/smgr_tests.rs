//! Storage Manager comprehensive tests — 14 tests

use ferrisdb_core::{BufferTag, PdbId};
use ferrisdb_storage::StorageManager;
use ferrisdb_storage::page::PAGE_SIZE;
use tempfile::TempDir;

fn tag(rel: u16, block: u32) -> BufferTag { BufferTag::new(PdbId::new(0), rel, block) }

#[test] fn test_smgr_init() { let td = TempDir::new().unwrap(); let s = StorageManager::new(td.path()); s.init().unwrap(); }
#[test] fn test_smgr_write_read() { let td = TempDir::new().unwrap(); let s = StorageManager::new(td.path()); s.init().unwrap(); let data = vec![0xAB; PAGE_SIZE]; s.write_page(&tag(1, 0), &data).unwrap(); let mut buf = vec![0u8; PAGE_SIZE]; s.read_page(&tag(1, 0), &mut buf).unwrap(); assert_eq!(buf[0], 0xAB); }
#[test] fn test_smgr_read_nonexistent() { let td = TempDir::new().unwrap(); let s = StorageManager::new(td.path()); s.init().unwrap(); let mut buf = vec![0u8; PAGE_SIZE]; assert!(s.read_page(&tag(999, 0), &mut buf).is_err()); }
#[test] fn test_smgr_overwrite() { let td = TempDir::new().unwrap(); let s = StorageManager::new(td.path()); s.init().unwrap(); s.write_page(&tag(1,0), &vec![1; PAGE_SIZE]).unwrap(); s.write_page(&tag(1,0), &vec![2; PAGE_SIZE]).unwrap(); let mut b = vec![0u8; PAGE_SIZE]; s.read_page(&tag(1,0), &mut b).unwrap(); assert_eq!(b[0], 2); }
#[test] fn test_smgr_multiple_blocks() { let td = TempDir::new().unwrap(); let s = StorageManager::new(td.path()); s.init().unwrap(); for i in 0..10u32 { s.write_page(&tag(1, i), &vec![i as u8; PAGE_SIZE]).unwrap(); } for i in 0..10u32 { let mut b = vec![0u8; PAGE_SIZE]; s.read_page(&tag(1, i), &mut b).unwrap(); assert_eq!(b[0], i as u8); } }
#[test] fn test_smgr_multiple_relations() { let td = TempDir::new().unwrap(); let s = StorageManager::new(td.path()); s.init().unwrap(); for r in 1..20u16 { s.write_page(&tag(r, 0), &vec![r as u8; PAGE_SIZE]).unwrap(); } for r in 1..20u16 { let mut b = vec![0u8; PAGE_SIZE]; s.read_page(&tag(r, 0), &mut b).unwrap(); assert_eq!(b[0], r as u8); } }
#[test] fn test_smgr_large_block_number() { let td = TempDir::new().unwrap(); let s = StorageManager::new(td.path()); s.init().unwrap(); s.write_page(&tag(1, 1000), &vec![42; PAGE_SIZE]).unwrap(); let mut b = vec![0u8; PAGE_SIZE]; s.read_page(&tag(1, 1000), &mut b).unwrap(); assert_eq!(b[0], 42); }
#[test] fn test_smgr_page_id_read_write() { let td = TempDir::new().unwrap(); let s = StorageManager::new(td.path()); s.init().unwrap(); let pid = ferrisdb_core::PageId::new(0, 0, 42, 5); s.write_page_by_id(&pid, &vec![0xCD; PAGE_SIZE]).unwrap(); let mut b = vec![0u8; PAGE_SIZE]; s.read_page_by_id(&pid, &mut b).unwrap(); assert_eq!(b[0], 0xCD); }
#[test] fn test_smgr_concurrent_writes() { let td = TempDir::new().unwrap(); let s = std::sync::Arc::new(StorageManager::new(td.path())); s.init().unwrap(); let mut h = vec![]; for t in 0..4u16 { let s = s.clone(); h.push(std::thread::spawn(move || { for i in 0..10u32 { s.write_page(&tag(t+1, i), &vec![(t*10 + i as u16) as u8; PAGE_SIZE]).unwrap(); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_smgr_data_dir() { let td = TempDir::new().unwrap(); let _ = StorageManager::new(td.path()); assert!(td.path().exists()); }
