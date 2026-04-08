//! 覆盖 BTree cursor/delete/merge + Recovery redo/undo 路径

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_core::{BufferTag, PdbId, Xid};
use ferrisdb_storage::*;
use ferrisdb_storage::wal::*;
use ferrisdb_storage::page::PAGE_SIZE;

// ============================================================
// BTree Cursor / Scan 覆盖
// ============================================================

#[test]
fn test_btree_cursor_next() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let tree = BTree::new(400, bp);
    tree.init().unwrap();

    for i in 0..50u32 {
        tree.insert(
            BTreeKey::new(format!("c{:04}", i).into_bytes()),
            BTreeValue::Tuple { block: i, offset: 0 },
        ).unwrap();
    }

    // Forward scan via scan_prefix
    let results = tree.scan_prefix(b"c").unwrap();
    assert!(results.len() >= 50, "Scan should find all keys, got {}", results.len());
}

#[test]
fn test_btree_cursor_empty_tree() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let tree = BTree::new(401, bp);
    tree.init().unwrap();

    let results = tree.scan_prefix(b"").unwrap();
    assert!(results.is_empty(), "Empty tree scan should return empty");
}

#[test]
fn test_btree_reverse_scan() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let tree = BTree::new(402, bp);
    tree.init().unwrap();

    for i in 0..30u32 {
        tree.insert(
            BTreeKey::new(format!("r{:04}", i).into_bytes()),
            BTreeValue::Tuple { block: i, offset: 0 },
        ).unwrap();
    }

    let results = tree.scan_prefix_reverse(b"r").unwrap();
    assert!(results.len() >= 30, "Reverse scan should return all keys, got {}", results.len());
}

// ============================================================
// BTree Delete + Merge 覆盖
// ============================================================

#[test]
fn test_btree_delete_all_keys() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let tree = BTree::new(403, bp);
    tree.init().unwrap();

    // 插入 50 个 key
    for i in 0..50u32 {
        tree.insert(
            BTreeKey::new(format!("d{:04}", i).into_bytes()),
            BTreeValue::Tuple { block: i, offset: 0 },
        ).unwrap();
    }

    // 全部删除
    for i in 0..50u32 {
        tree.delete(&BTreeKey::new(format!("d{:04}", i).into_bytes())).unwrap();
    }

    // 验证全部不存在
    for i in 0..50u32 {
        assert!(tree.lookup(&BTreeKey::new(format!("d{:04}", i).into_bytes())).unwrap().is_none());
    }
}

#[test]
fn test_btree_delete_nonexistent_key() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let tree = BTree::new(404, bp);
    tree.init().unwrap();

    tree.insert(BTreeKey::new(b"exists".to_vec()), BTreeValue::Tuple { block: 1, offset: 0 }).unwrap();

    // 删除不存在的 key
    let result = tree.delete(&BTreeKey::new(b"not_exists".to_vec())).unwrap();
    assert!(!result, "Delete of non-existent key should return false");
}

#[test]
fn test_btree_insert_after_delete() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let tree = BTree::new(405, bp);
    tree.init().unwrap();

    // 插入 → 删除 → 再插入（验证空间复用）
    let key = BTreeKey::new(b"reinsert_key".to_vec());
    tree.insert(key.clone(), BTreeValue::Tuple { block: 1, offset: 0 }).unwrap();
    tree.delete(&key).unwrap();
    tree.insert(key.clone(), BTreeValue::Tuple { block: 2, offset: 0 }).unwrap();

    let val = tree.lookup(&key).unwrap().unwrap();
    match val {
        BTreeValue::Tuple { block, .. } => assert_eq!(block, 2),
        _ => panic!("Expected Tuple"),
    }
}

// ============================================================
// BTree Split 触发 + 深度树
// ============================================================

#[test]
fn test_btree_many_inserts_trigger_multiple_splits() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(500)).unwrap());
    let tree = BTree::new(406, bp);
    tree.init().unwrap();

    // 1000 个 key 触发多次 split
    for i in 0..1000u32 {
        tree.insert(
            BTreeKey::new(format!("ms{:06}", i).into_bytes()),
            BTreeValue::Tuple { block: i, offset: 0 },
        ).unwrap();
    }

    let stats = tree.stats();
    assert!(stats.splits.load(std::sync::atomic::Ordering::Relaxed) > 0, "Should have splits");

    // 验证所有 key 可查找
    let mut found = 0;
    for i in 0..1000u32 {
        if tree.lookup(&BTreeKey::new(format!("ms{:06}", i).into_bytes())).unwrap().is_some() {
            found += 1;
        }
    }
    assert_eq!(found, 1000);
}

// ============================================================
// Recovery 完整 redo/undo 路径覆盖
// ============================================================

#[test]
fn test_recovery_full_heap_redo() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();

    // 写入完整的 heap insert WAL + 创建对应的数据页面
    let writer = WalWriter::new(&wal_dir);
    let page_id = PageId::new(0, 0, 1, 0);

    // 先写一个空页面到磁盘
    let tag = BufferTag::new(PdbId::new(0), 1, 0);
    let empty_page = vec![0u8; PAGE_SIZE];
    smgr.write_page(&tag, &empty_page).unwrap();

    // 写 WAL insert record
    let tuple_data = vec![0xABu8; 100];
    let rec = WalHeapInsert::new(page_id, 1, &tuple_data);
    writer.write(&rec.serialize_with_data(&tuple_data)).unwrap();

    // 写 commit record
    let commit = WalTxnCommit::new(Xid::new(0, 1), 100);
    writer.write(&commit.to_bytes()).unwrap();
    writer.sync().unwrap();
    drop(writer);

    // Recovery 应该 redo insert
    let recovery = WalRecovery::with_smgr(&wal_dir, Arc::clone(&smgr));
    recovery.recover(RecoveryMode::CrashRecovery).unwrap();
    assert!(recovery.stats().records_redone > 0 || recovery.stats().records_skipped > 0);
}

#[test]
fn test_recovery_with_undo_records() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let writer = WalWriter::new(&wal_dir);

    // 写 undo record (UndoInsertRecord)
    let undo_header = WalRecordHeader::new(WalRecordType::UndoInsertRecord, 23);
    let header_bytes = unsafe {
        std::slice::from_raw_parts(
            &undo_header as *const _ as *const u8,
            std::mem::size_of::<WalRecordHeader>(),
        )
    };
    let mut undo_payload = Vec::new();
    undo_payload.extend_from_slice(&1u64.to_le_bytes()); // xid
    undo_payload.push(0); // action_type = Insert
    undo_payload.extend_from_slice(&1u32.to_le_bytes()); // table_oid
    undo_payload.extend_from_slice(&0u32.to_le_bytes()); // page_no
    undo_payload.extend_from_slice(&1u16.to_le_bytes()); // offset
    undo_payload.extend_from_slice(&0u32.to_le_bytes()); // data_len

    let mut full = Vec::new();
    full.extend_from_slice(header_bytes);
    full.extend_from_slice(&undo_payload);
    writer.write(&full).unwrap();

    writer.sync().unwrap();
    drop(writer);

    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover_scan_only().unwrap();
}

#[test]
fn test_recovery_corrupt_wal_stops_at_bad_crc() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    // 写一条合法记录
    let writer = WalWriter::new(&wal_dir);
    let data = vec![0u8; 30];
    let rec = WalHeapInsert::new(PageId::new(0, 0, 1, 0), 1, &data);
    writer.write(&rec.serialize_with_data(&data)).unwrap();
    writer.sync().unwrap();
    drop(writer);

    // 在 WAL 文件末尾追加垃圾数据
    let wal_files: Vec<_> = std::fs::read_dir(&wal_dir).unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
        .collect();
    if !wal_files.is_empty() {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(wal_files[0].path()).unwrap();
        f.write_all(&[0xFF; 20]).unwrap(); // 垃圾数据
    }

    // Recovery 应在 CRC 校验失败时停止，不 panic
    let smgr = Arc::new(StorageManager::new(td.path()));
    smgr.init().unwrap();
    let recovery = WalRecovery::with_smgr(&wal_dir, smgr);
    recovery.recover_scan_only().unwrap();
}

// ============================================================
// BTree get_free_pages / set_free_pages
// ============================================================

#[test]
fn test_btree_free_pages_persist() {
    let bp = Arc::new(BufferPool::new(BufferPoolConfig::new(200)).unwrap());
    let tree = BTree::new(407, bp);
    tree.init().unwrap();

    // 初始无 free pages
    assert!(tree.get_free_pages().is_empty());

    // 模拟设置 free pages
    tree.set_free_pages(vec![10, 20, 30]);
    let fps = tree.get_free_pages();
    assert_eq!(fps, vec![10, 20, 30]);
}

// ============================================================
// Page header checksum
// ============================================================

#[test]
fn test_page_header_set_verify_checksum() {
    let mut data = vec![0u8; PAGE_SIZE];
    // 写入一些数据
    data[100] = 0xAB;
    data[200] = 0xCD;

    page::PageHeader::set_checksum(&mut data);
    assert!(page::PageHeader::verify_checksum(&data));

    // 损坏数据
    data[100] = 0xFF;
    assert!(!page::PageHeader::verify_checksum(&data));
}
