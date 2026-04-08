//! 鲁棒性测试 — 故障注入 + 边界值 + 异常路径（仅 storage crate）

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{BufferTag, PdbId};
use ferrisdb_storage::*;
use ferrisdb_storage::wal::*;
use ferrisdb_storage::page::PAGE_SIZE;

// ============================================================
// 1. WAL 故障注入
// ============================================================

#[test]
fn test_recovery_truncated_wal_header() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::write(wal_dir.join("00000001.wal"), &[0u8; 10]).unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    let _ = recovery.recover_scan_only(); // 不 panic
}

#[test]
fn test_recovery_zero_filled_wal() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    std::fs::write(wal_dir.join("00000001.wal"), &vec![0u8; 1024]).unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let _ = WalRecovery::with_smgr(&wal_dir, smgr).recover_scan_only();
}

#[test]
fn test_recovery_random_garbage_wal() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let garbage: Vec<u8> = (0..4096).map(|i| (i * 37 + 13) as u8).collect();
    std::fs::write(wal_dir.join("00000001.wal"), &garbage).unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let _ = WalRecovery::with_smgr(&wal_dir, smgr).recover_scan_only();
}

// ============================================================
// 2. Buffer Pool 极端
// ============================================================

#[test]
fn test_buffer_pool_size_one() {
    let bp = BufferPool::new(BufferPoolConfig::new(1)).unwrap();
    let tag = BufferTag::new(PdbId::new(0), 1, 0);
    let p = bp.pin(&tag).unwrap(); drop(p);
    let tag2 = BufferTag::new(PdbId::new(0), 1, 1);
    let p2 = bp.pin(&tag2).unwrap(); drop(p2);
}

#[test]
fn test_buffer_pool_same_page_multi_pin() {
    let bp = BufferPool::new(BufferPoolConfig::new(10)).unwrap();
    let tag = BufferTag::new(PdbId::new(0), 1, 0);
    let p1 = bp.pin(&tag).unwrap();
    let p2 = bp.pin(&tag).unwrap();
    let p3 = bp.pin(&tag).unwrap();
    drop(p1); drop(p2); drop(p3);
}

// ============================================================
// 3. BTree 极端输入
// ============================================================

#[test]
fn test_btree_empty_key() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let tree = BTree::new(500, bp);
    tree.init().unwrap();
    tree.insert(BTreeKey::new(vec![]), BTreeValue::Tuple { block: 0, offset: 0 }).unwrap();
    assert!(tree.lookup(&BTreeKey::new(vec![])).unwrap().is_some());
}

#[test]
fn test_btree_256_single_byte_keys() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let tree = BTree::new(501, bp);
    tree.init().unwrap();
    for i in 0..=255u8 {
        tree.insert(BTreeKey::new(vec![i]), BTreeValue::Tuple { block: i as u32, offset: 0 }).unwrap();
    }
    for i in 0..=255u8 {
        assert!(tree.lookup(&BTreeKey::new(vec![i])).unwrap().is_some());
    }
}

#[test]
fn test_btree_100_duplicate_keys() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let tree = BTree::new(502, bp);
    tree.init().unwrap();
    let key = BTreeKey::new(b"dup".to_vec());
    for i in 0..100u32 {
        tree.insert(key.clone(), BTreeValue::Tuple { block: i, offset: 0 }).unwrap();
    }
    assert!(tree.lookup(&key).unwrap().is_some());
}

// ============================================================
// 4. 并发 BTree insert+delete+scan
// ============================================================

#[test]
fn test_concurrent_btree_insert_delete_scan() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(500)).unwrap());
    let tree = Arc::new(BTree::new(600, Arc::clone(&bp)));
    tree.init().unwrap();
    let barrier = Arc::new(std::sync::Barrier::new(4));
    let mut handles = vec![];

    for t in 0..2 {
        let tree = Arc::clone(&tree); let bar = Arc::clone(&barrier);
        handles.push(std::thread::spawn(move || {
            bar.wait();
            for i in 0..200u32 {
                let _ = tree.insert(BTreeKey::new(format!("ids_{}_{:04}", t, i).into_bytes()), BTreeValue::Tuple { block: i, offset: 0 });
            }
        }));
    }
    { let tree = Arc::clone(&tree); let bar = Arc::clone(&barrier);
      handles.push(std::thread::spawn(move || {
          bar.wait();
          for i in 0..100u32 { let _ = tree.delete(&BTreeKey::new(format!("ids_0_{:04}", i).into_bytes())); }
      }));
    }
    { let tree = Arc::clone(&tree); let bar = Arc::clone(&barrier);
      handles.push(std::thread::spawn(move || {
          bar.wait();
          for _ in 0..50 { let _ = tree.scan_prefix(b"ids_"); std::thread::yield_now(); }
      }));
    }
    for h in handles { h.join().unwrap(); }
}

// ============================================================
// 5. Control file 损坏
// ============================================================

#[test]
fn test_control_file_corruption() {
    let td = TempDir::new().unwrap();
    { let cf = ControlFile::open(td.path()).unwrap(); cf.set_state(1).unwrap(); }
    let cf_path = td.path().join("dstore_control");
    if cf_path.exists() {
        let mut data = std::fs::read(&cf_path).unwrap();
        if data.len() > 5 { data[3] ^= 0xFF; std::fs::write(&cf_path, &data).unwrap(); }
    }
    let _ = ControlFile::open(td.path()); // 不 panic
}

// ============================================================
// 6. Ring buffer 极端
// ============================================================

#[test]
fn test_ring_buffer_exact_size_writes() {
    use ferrisdb_storage::wal::ring_buffer::WalRingBuffer;
    let ring = Arc::new(WalRingBuffer::new(100));
    let ring2 = Arc::clone(&ring);
    let drain = std::thread::spawn(move || {
        for _ in 0..500 {
            let (data, pos) = ring2.get_flush_data();
            if !data.is_empty() { ring2.advance_flush(pos); }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });
    for _ in 0..50 { ring.write(&[0xAA; 10]); }
    ring.shutdown(); drain.join().unwrap();
}

#[test]
fn test_ring_buffer_single_byte_writes() {
    use ferrisdb_storage::wal::ring_buffer::WalRingBuffer;
    let ring = Arc::new(WalRingBuffer::new(256));
    let ring2 = Arc::clone(&ring);
    let drain = std::thread::spawn(move || {
        for _ in 0..100 {
            let (data, pos) = ring2.get_flush_data();
            if !data.is_empty() { ring2.advance_flush(pos); }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    });
    for i in 0..200u8 { ring.write(&[i]); }
    ring.shutdown(); drain.join().unwrap();
}

// ============================================================
// 7. Segment 边界
// ============================================================

#[test]
fn test_smgr_segment_boundary_pages() {
    let td = TempDir::new().unwrap();
    let smgr = StorageManager::new(td.path());
    smgr.init().unwrap();
    let tag_last = BufferTag::new(PdbId::new(0), 1, 16383); // last page seg 0
    let tag_first = BufferTag::new(PdbId::new(0), 1, 16384); // first page seg 1
    smgr.write_page(&tag_last, &vec![0xEEu8; PAGE_SIZE]).unwrap();
    smgr.write_page(&tag_first, &vec![0xFFu8; PAGE_SIZE]).unwrap();
    let mut buf = vec![0u8; PAGE_SIZE];
    smgr.read_page(&tag_last, &mut buf).unwrap();
    assert_eq!(buf[0], 0xEE);
    smgr.read_page(&tag_first, &mut buf).unwrap();
    assert_eq!(buf[0], 0xFF);
}

// ============================================================
// 8. Page checksum 损坏
// ============================================================

#[test]
fn test_page_checksum_corruption_detection() {
    let mut data = vec![0u8; PAGE_SIZE];
    data[100] = 0xAB;
    page::PageHeader::set_checksum(&mut data);
    assert!(page::PageHeader::verify_checksum(&data));
    data[100] = 0xFF; // corrupt
    assert!(!page::PageHeader::verify_checksum(&data));
}
