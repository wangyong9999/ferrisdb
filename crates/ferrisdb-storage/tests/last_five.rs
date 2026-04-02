//! Last 5 tests to reach 100% coverable coverage

use std::sync::Arc;
use ferrisdb_core::{BufferTag, PdbId, Xid};
use ferrisdb_storage::*;
use ferrisdb_storage::wal::*;
use ferrisdb_storage::page::PAGE_SIZE;
use tempfile::TempDir;

// 1. BTree 深度树 — 插入足够多 key 产生 10+ level
#[test]
fn test_btree_deep_tree_10k() {
    let t = BTree::new(1, Arc::new(BufferPool::new(BufferPoolConfig::new(20000)).unwrap()));
    t.init().unwrap();
    for i in 0..10000u32 {
        t.insert(BTreeKey::new(format!("{:010}", i).into_bytes()), BTreeValue::Tuple { block: i, offset: 1 }).unwrap();
    }
    // Verify first, middle, last
    assert!(t.lookup(&BTreeKey::new(b"0000000000".to_vec())).unwrap().is_some());
    assert!(t.lookup(&BTreeKey::new(b"0000005000".to_vec())).unwrap().is_some());
    assert!(t.lookup(&BTreeKey::new(b"0000009999".to_vec())).unwrap().is_some());
    // Verify scan sorted
    let all = t.scan_prefix(b"").unwrap();
    assert!(all.len() >= 10000);
    for w in all.windows(2) { assert!(w[0].0 <= w[1].0); }
}

// 2. BTree 深度树 delete 验证
#[test]
fn test_btree_deep_delete_verify() {
    let t = BTree::new(2, Arc::new(BufferPool::new(BufferPoolConfig::new(20000)).unwrap()));
    t.init().unwrap();
    for i in 0..10000u32 {
        t.insert(BTreeKey::new(format!("{:010}", i).into_bytes()), BTreeValue::Tuple { block: i, offset: 1 }).unwrap();
    }
    // Delete every 10th
    for i in (0..10000).step_by(10) {
        t.delete(&BTreeKey::new(format!("{:010}", i).into_bytes())).unwrap();
    }
    // Verify deletions
    for i in (0..10000).step_by(10) {
        assert!(t.lookup(&BTreeKey::new(format!("{:010}", i).into_bytes())).unwrap().is_none());
    }
    // Verify remaining
    assert!(t.lookup(&BTreeKey::new(b"0000000001".to_vec())).unwrap().is_some());
}

// 3. WAL atomic group roundtrip
#[test]
fn test_wal_atomic_group_roundtrip() {
    let buf = WalBuffer::new(1024 * 1024);
    let xid = Xid::new(0, 42);
    let gpos = buf.begin_atomic_group(xid).unwrap();
    for i in 0..5 {
        let data = format!("atomic_record_{}", i);
        buf.append_to_atomic_group(gpos, data.as_bytes()).unwrap();
    }
    let lsn = buf.end_atomic_group(gpos).unwrap();
    assert!(lsn.is_valid());
    assert!(buf.write_pos() > gpos);
}

// 4. Buffer pool + checkpoint 集成
#[test]
fn test_buffer_checkpoint_integration() {
    let td = TempDir::new().unwrap();
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let wd = td.path().join("wal");
    std::fs::create_dir_all(&wd).unwrap();
    let wal = Arc::new(WalWriter::new(&wd));

    let mut bp = BufferPool::new(BufferPoolConfig::new(100)).unwrap();
    bp.set_smgr(Arc::clone(&smgr));
    bp.set_wal_writer(Arc::clone(&wal));
    let bp = Arc::new(bp);

    // Write data
    for i in 0..10u32 {
        let p = bp.pin(&BufferTag::new(PdbId::new(0), 1, i)).unwrap();
        let l = p.lock_exclusive();
        let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };
        hp.init();
        hp.insert_tuple(format!("checkpoint_data_{}", i).as_bytes()).unwrap();
        drop(l);
        p.mark_dirty();
    }

    // Checkpoint: flush + control file
    let cf = ControlFile::open(td.path()).unwrap();
    bp.flush_all().unwrap();
    wal.sync().unwrap();
    cf.update_checkpoint(wal.current_lsn(), wal.file_no()).unwrap();

    // Verify control file persisted
    let cf2 = ControlFile::open(td.path()).unwrap();
    assert!(cf2.checkpoint_lsn().raw() > 0);
}

// 5. HeapPage 边界 — 填满到最后一个 slot
#[test]
fn test_heap_page_fill_to_boundary() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(10)).unwrap());
    let tag = BufferTag::new(PdbId::new(0), 999, 0);
    let p = bp.pin(&tag).unwrap();
    let l = p.lock_exclusive();
    let hp = unsafe { HeapPage::from_bytes(std::slice::from_raw_parts_mut(p.page_data(), PAGE_SIZE)) };
    hp.init();

    let mut count = 0;
    // Insert 32-byte tuples until page is full
    let small = [0xCC; 32];
    while hp.insert_tuple(&small).is_some() {
        count += 1;
    }
    assert!(count > 50, "Should fit many 32-byte tuples, got {}", count);
    assert!(count < 300, "But not too many");

    // Verify free space is near zero
    assert!(hp.free_space() < 40, "Should be nearly full, free={}", hp.free_space());

    // Verify all tuples readable
    for i in 1..=count as u16 {
        assert!(hp.get_tuple(i).is_some(), "Tuple {} should be readable", i);
    }
}
