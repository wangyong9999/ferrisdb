//! BTree advanced tests — deep trees, boundary conditions, WAL integration

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb_storage::{BTree, BTreeKey, BTreeValue, BufferPool, BufferPoolConfig, WalWriter};

fn make_bp(n: usize) -> Arc<BufferPool> {
    Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap())
}
fn key(s: &str) -> BTreeKey { BTreeKey::new(s.as_bytes().to_vec()) }
fn tuple(b: u32, o: u16) -> BTreeValue { BTreeValue::Tuple { block: b, offset: o } }

// ==================== Deep Tree ====================

#[test]
fn test_btree_deep_tree_5000_keys() {
    let tree = BTree::new(100, make_bp(5000));
    tree.init().unwrap();
    for i in 0..5000u32 {
        tree.insert(key(&format!("{:08}", i)), tuple(i, 1)).unwrap();
    }
    // Verify random lookups
    for i in (0..5000).step_by(100) {
        assert!(tree.lookup(&key(&format!("{:08}", i))).unwrap().is_some());
    }
    assert!(tree.stats().splits.load(std::sync::atomic::Ordering::Relaxed) > 5);
}

#[test]
fn test_btree_insert_delete_reinsert() {
    let tree = BTree::new(101, make_bp(1000));
    tree.init().unwrap();
    // Insert 200 keys
    for i in 0..200u32 {
        tree.insert(key(&format!("idr_{:04}", i)), tuple(i, 1)).unwrap();
    }
    // Delete all
    for i in 0..200u32 {
        tree.delete(&key(&format!("idr_{:04}", i))).unwrap();
    }
    // Reinsert
    for i in 0..200u32 {
        tree.insert(key(&format!("idr_{:04}", i)), tuple(i + 1000, 1)).unwrap();
    }
    // All should be found with new values
    for i in 0..200u32 {
        let val = tree.lookup(&key(&format!("idr_{:04}", i))).unwrap().unwrap();
        if let BTreeValue::Tuple { block, .. } = val {
            assert_eq!(block, i + 1000);
        }
    }
}

#[test]
fn test_btree_scan_sorted_order() {
    let tree = BTree::new(102, make_bp(1000));
    tree.init().unwrap();
    // Insert in random-ish order
    for i in [50, 10, 90, 30, 70, 20, 80, 40, 60, 0u32] {
        tree.insert(key(&format!("{:03}", i)), tuple(i, 1)).unwrap();
    }
    let results = tree.scan_prefix(b"").unwrap();
    assert_eq!(results.len(), 10);
    // Must be sorted
    for i in 1..results.len() {
        assert!(results[i-1].0 <= results[i].0, "Keys must be sorted");
    }
}

#[test]
fn test_btree_duplicate_value_different_keys() {
    let tree = BTree::new(103, make_bp(500));
    tree.init().unwrap();
    // Same value, different keys
    for i in 0..50 {
        tree.insert(key(&format!("dk_{}", i)), tuple(42, 1)).unwrap();
    }
    // All should exist
    for i in 0..50 {
        assert!(tree.lookup(&key(&format!("dk_{}", i))).unwrap().is_some());
    }
}

#[test]
fn test_btree_binary_key() {
    let tree = BTree::new(104, make_bp(500));
    tree.init().unwrap();
    // Keys with null bytes and special chars
    tree.insert(BTreeKey::new(vec![0, 1, 2, 3]), tuple(1, 1)).unwrap();
    tree.insert(BTreeKey::new(vec![0, 0, 0, 0]), tuple(2, 1)).unwrap();
    tree.insert(BTreeKey::new(vec![255, 255, 255]), tuple(3, 1)).unwrap();
    assert!(tree.lookup(&BTreeKey::new(vec![0, 1, 2, 3])).unwrap().is_some());
    assert!(tree.lookup(&BTreeKey::new(vec![0, 0, 0, 0])).unwrap().is_some());
    assert!(tree.lookup(&BTreeKey::new(vec![255, 255, 255])).unwrap().is_some());
}

// ==================== WAL Integration ====================

#[test]
fn test_btree_with_wal_writer() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let wal = Arc::new(WalWriter::new(&wal_dir));

    let tree = BTree::with_wal(200, make_bp(1000), Arc::clone(&wal));
    tree.init().unwrap();
    for i in 0..100 {
        tree.insert(key(&format!("wal_{:04}", i)), tuple(i, 1)).unwrap();
    }
    wal.sync().unwrap();
    // WAL should have data
    assert!(wal.offset() > 100);
}

#[test]
fn test_btree_split_writes_wal() {
    let td = TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let wal = Arc::new(WalWriter::new(&wal_dir));

    let tree = BTree::with_wal(201, make_bp(1000), Arc::clone(&wal));
    tree.init().unwrap();
    let before = wal.offset();
    // Insert enough to trigger split
    for i in 0..500 {
        tree.insert(key(&format!("sp_{:06}", i)), tuple(i, 1)).unwrap();
    }
    assert!(wal.offset() > before + 1000, "Split should write WAL");
}

// ==================== Concurrent Advanced ====================

#[test]
fn test_btree_concurrent_insert_lookup_delete() {
    let tree = Arc::new(BTree::new(300, make_bp(2000)));
    tree.init().unwrap();
    // Pre-populate
    for i in 0..200 {
        tree.insert(key(&format!("cad_{:04}", i)), tuple(i, 1)).unwrap();
    }
    let mut handles = vec![];
    // Inserters
    for t in 0..2 {
        let tree = Arc::clone(&tree);
        handles.push(std::thread::spawn(move || {
            for i in 0..100 {
                tree.insert(key(&format!("new_{}_{:04}", t, i)), tuple(i+1000, 1)).unwrap();
            }
        }));
    }
    // Lookers
    for _ in 0..2 {
        let tree = Arc::clone(&tree);
        handles.push(std::thread::spawn(move || {
            for i in 0..200 {
                let _ = tree.lookup(&key(&format!("cad_{:04}", i)));
            }
        }));
    }
    // Deleters
    let tree2 = Arc::clone(&tree);
    handles.push(std::thread::spawn(move || {
        for i in (0..200).step_by(3) {
            let _ = tree2.delete(&key(&format!("cad_{:04}", i)));
        }
    }));
    for h in handles { h.join().unwrap(); }
}

#[test]
fn test_btree_scan_range() {
    let tree = BTree::new(301, make_bp(1000));
    tree.init().unwrap();
    for i in 0..100 {
        tree.insert(key(&format!("{:03}", i)), tuple(i, 1)).unwrap();
    }
    let range = tree.scan_range(b"030", b"060").unwrap();
    // Should have keys 030-059
    assert!(range.len() >= 25 && range.len() <= 35,
        "Range scan should return ~30 keys, got {}", range.len());
}

#[test]
fn test_btree_unique_prevents_duplicate() {
    let tree = BTree::new(302, make_bp(500));
    tree.init().unwrap();
    tree.insert_unique(key("unique_key"), tuple(1, 1)).unwrap();
    let result = tree.insert_unique(key("unique_key"), tuple(2, 1));
    assert!(result.is_err(), "Should reject duplicate");
    // Original value preserved
    let val = tree.lookup(&key("unique_key")).unwrap().unwrap();
    if let BTreeValue::Tuple { block, .. } = val {
        assert_eq!(block, 1);
    }
}
