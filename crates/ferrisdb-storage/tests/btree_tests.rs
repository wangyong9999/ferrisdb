//! BTree comprehensive tests — covering insert, delete, split, scan, unique, merge, concurrent ops

use std::sync::Arc;
use ferrisdb_storage::{BTree, BTreeKey, BTreeValue, BTreeItem, BufferPool, BufferPoolConfig};

fn make_bp() -> Arc<BufferPool> {
    Arc::new(BufferPool::new(BufferPoolConfig::new(2000)).unwrap())
}

fn key(s: &str) -> BTreeKey { BTreeKey::new(s.as_bytes().to_vec()) }
fn tuple(b: u32, o: u16) -> BTreeValue { BTreeValue::Tuple { block: b, offset: o } }

// ==================== Basic Operations ====================

#[test]
fn test_btree_init() {
    let tree = BTree::new(1, make_bp());
    tree.init().unwrap();
    assert_ne!(tree.root_page(), u32::MAX);
}

#[test]
fn test_btree_insert_single() {
    let tree = BTree::new(2, make_bp());
    tree.init().unwrap();
    tree.insert(key("hello"), tuple(1, 1)).unwrap();
    let val = tree.lookup(&key("hello")).unwrap();
    assert!(val.is_some());
}

#[test]
fn test_btree_insert_multiple() {
    let tree = BTree::new(3, make_bp());
    tree.init().unwrap();
    for i in 0..100 {
        tree.insert(key(&format!("key_{:04}", i)), tuple(i, 1)).unwrap();
    }
    // Verify all keys present
    for i in 0..100 {
        assert!(tree.lookup(&key(&format!("key_{:04}", i))).unwrap().is_some(),
            "key_{:04} should exist", i);
    }
}

#[test]
fn test_btree_lookup_nonexistent() {
    let tree = BTree::new(4, make_bp());
    tree.init().unwrap();
    tree.insert(key("exists"), tuple(1, 1)).unwrap();
    assert!(tree.lookup(&key("nope")).unwrap().is_none());
}

#[test]
fn test_btree_delete_basic() {
    let tree = BTree::new(5, make_bp());
    tree.init().unwrap();
    tree.insert(key("del_me"), tuple(1, 1)).unwrap();
    assert!(tree.delete(&key("del_me")).unwrap());
    assert!(tree.lookup(&key("del_me")).unwrap().is_none());
}

#[test]
fn test_btree_delete_nonexistent() {
    let tree = BTree::new(6, make_bp());
    tree.init().unwrap();
    assert!(!tree.delete(&key("ghost")).unwrap());
}

#[test]
fn test_btree_delete_multiple() {
    let tree = BTree::new(7, make_bp());
    tree.init().unwrap();
    for i in 0..50 {
        tree.insert(key(&format!("d_{:03}", i)), tuple(i, 1)).unwrap();
    }
    // Delete even keys
    for i in (0..50).step_by(2) {
        assert!(tree.delete(&key(&format!("d_{:03}", i))).unwrap());
    }
    // Odd keys should still exist
    for i in (1..50).step_by(2) {
        assert!(tree.lookup(&key(&format!("d_{:03}", i))).unwrap().is_some());
    }
    // Even keys gone
    for i in (0..50).step_by(2) {
        assert!(tree.lookup(&key(&format!("d_{:03}", i))).unwrap().is_none());
    }
}

// ==================== Split ====================

#[test]
fn test_btree_split_triggered() {
    let tree = BTree::new(10, make_bp());
    tree.init().unwrap();
    // Insert enough to trigger splits (page ~8KB, each key ~20 bytes → ~300 keys per page)
    for i in 0..500 {
        tree.insert(key(&format!("split_{:05}", i)), tuple(i, 1)).unwrap();
    }
    assert!(tree.stats().splits.load(std::sync::atomic::Ordering::Relaxed) > 0,
        "Should have triggered at least one split");
    // All keys still findable
    for i in 0..500 {
        assert!(tree.lookup(&key(&format!("split_{:05}", i))).unwrap().is_some(),
            "split_{:05} should exist after splits", i);
    }
}

#[test]
fn test_btree_many_inserts() {
    let tree = BTree::new(11, make_bp());
    tree.init().unwrap();
    for i in 0..2000 {
        tree.insert(key(&format!("{:06}", i)), tuple(i, 1)).unwrap();
    }
    for i in 0..2000 {
        assert!(tree.lookup(&key(&format!("{:06}", i))).unwrap().is_some());
    }
}

// ==================== Scan ====================

#[test]
fn test_btree_scan_prefix() {
    let tree = BTree::new(20, make_bp());
    tree.init().unwrap();
    tree.insert(key("apple"), tuple(1, 1)).unwrap();
    tree.insert(key("app"), tuple(2, 1)).unwrap();
    tree.insert(key("banana"), tuple(3, 1)).unwrap();
    tree.insert(key("application"), tuple(4, 1)).unwrap();

    let results = tree.scan_prefix(b"app").unwrap();
    assert_eq!(results.len(), 3); // app, apple, application
}

#[test]
fn test_btree_scan_prefix_empty() {
    let tree = BTree::new(21, make_bp());
    tree.init().unwrap();
    tree.insert(key("hello"), tuple(1, 1)).unwrap();
    let results = tree.scan_prefix(b"xyz").unwrap();
    assert_eq!(results.len(), 0);
}

#[test]
fn test_btree_scan_prefix_all() {
    let tree = BTree::new(22, make_bp());
    tree.init().unwrap();
    for i in 0..20 {
        tree.insert(key(&format!("k{}", i)), tuple(i, 1)).unwrap();
    }
    let all = tree.scan_prefix(b"k").unwrap();
    assert_eq!(all.len(), 20);
}

#[test]
fn test_btree_scan_reverse() {
    let tree = BTree::new(23, make_bp());
    tree.init().unwrap();
    tree.insert(key("a"), tuple(1, 1)).unwrap();
    tree.insert(key("b"), tuple(2, 1)).unwrap();
    tree.insert(key("c"), tuple(3, 1)).unwrap();

    let rev = tree.scan_prefix_reverse(b"").unwrap();
    assert_eq!(rev.len(), 3);
    assert_eq!(rev[0].0, b"c");
    assert_eq!(rev[2].0, b"a");
}

// ==================== Unique Constraint ====================

#[test]
fn test_btree_insert_unique_success() {
    let tree = BTree::new(30, make_bp());
    tree.init().unwrap();
    assert!(tree.insert_unique(key("unique"), tuple(1, 1)).is_ok());
    assert!(tree.insert_unique(key("other"), tuple(2, 1)).is_ok());
}

#[test]
fn test_btree_insert_unique_duplicate() {
    let tree = BTree::new(31, make_bp());
    tree.init().unwrap();
    tree.insert_unique(key("dup"), tuple(1, 1)).unwrap();
    assert!(tree.insert_unique(key("dup"), tuple(2, 1)).is_err());
}

// ==================== Serialization ====================

#[test]
fn test_btree_item_serialize_tuple() {
    let item = BTreeItem { key: key("test"), value: tuple(42, 7) };
    let bytes = item.serialize();
    let restored = BTreeItem::deserialize(&bytes).unwrap();
    assert_eq!(restored.key, item.key);
    if let BTreeValue::Tuple { block, offset } = restored.value {
        assert_eq!(block, 42);
        assert_eq!(offset, 7);
    } else { panic!("Expected Tuple"); }
}

#[test]
fn test_btree_item_serialize_child() {
    let item = BTreeItem { key: key("child"), value: BTreeValue::Child(99) };
    let bytes = item.serialize();
    let restored = BTreeItem::deserialize(&bytes).unwrap();
    if let BTreeValue::Child(c) = restored.value {
        assert_eq!(c, 99);
    } else { panic!("Expected Child"); }
}

// ==================== Merge ====================

#[test]
fn test_btree_delete_triggers_consideration() {
    let tree = BTree::new(40, make_bp());
    tree.init().unwrap();
    for i in 0..20 {
        tree.insert(key(&format!("m_{:03}", i)), tuple(i, 1)).unwrap();
    }
    // Delete most keys
    for i in 0..18 {
        tree.delete(&key(&format!("m_{:03}", i))).unwrap();
    }
    // Remaining keys still accessible
    assert!(tree.lookup(&key("m_018")).unwrap().is_some());
    assert!(tree.lookup(&key("m_019")).unwrap().is_some());
}

// ==================== Concurrent ====================

#[test]
fn test_btree_concurrent_insert() {
    let tree = Arc::new(BTree::new(50, make_bp()));
    tree.init().unwrap();

    let mut handles = vec![];
    for t in 0..4 {
        let tree = Arc::clone(&tree);
        handles.push(std::thread::spawn(move || {
            for i in 0..200 {
                tree.insert(key(&format!("t{}_{:04}", t, i)), tuple(i, 1)).unwrap();
            }
        }));
    }
    for h in handles { h.join().unwrap(); }

    // All 800 keys should exist
    let mut found = 0;
    for t in 0..4 {
        for i in 0..200 {
            if tree.lookup(&key(&format!("t{}_{:04}", t, i))).unwrap().is_some() {
                found += 1;
            }
        }
    }
    assert_eq!(found, 800, "All 800 keys should be found, got {}", found);
}

#[test]
fn test_btree_concurrent_insert_and_lookup() {
    let tree = Arc::new(BTree::new(51, make_bp()));
    tree.init().unwrap();

    // Pre-insert
    for i in 0..100 {
        tree.insert(key(&format!("pre_{:04}", i)), tuple(i, 1)).unwrap();
    }

    let mut handles = vec![];
    // Writers
    for t in 0..2 {
        let tree = Arc::clone(&tree);
        handles.push(std::thread::spawn(move || {
            for i in 0..100 {
                tree.insert(key(&format!("w{}_{:04}", t, i)), tuple(i, 1)).unwrap();
            }
        }));
    }
    // Readers
    for _ in 0..2 {
        let tree = Arc::clone(&tree);
        handles.push(std::thread::spawn(move || {
            for i in 0..100 {
                let _ = tree.lookup(&key(&format!("pre_{:04}", i)));
            }
        }));
    }
    for h in handles { h.join().unwrap(); }
}

// ==================== Edge Cases ====================

#[test]
fn test_btree_empty_key() {
    let tree = BTree::new(60, make_bp());
    tree.init().unwrap();
    tree.insert(key(""), tuple(1, 1)).unwrap();
    assert!(tree.lookup(&key("")).unwrap().is_some());
}

#[test]
fn test_btree_large_key() {
    let tree = BTree::new(61, make_bp());
    tree.init().unwrap();
    let big = "x".repeat(500);
    tree.insert(key(&big), tuple(1, 1)).unwrap();
    assert!(tree.lookup(&key(&big)).unwrap().is_some());
}

#[test]
fn test_btree_sequential_insert_order() {
    let tree = BTree::new(62, make_bp());
    tree.init().unwrap();
    // Insert in order
    for i in 0..100u32 {
        tree.insert(key(&format!("{:05}", i)), tuple(i, 1)).unwrap();
    }
    let all = tree.scan_prefix(b"").unwrap();
    // Should be sorted
    for i in 1..all.len() {
        assert!(all[i - 1].0 <= all[i].0, "Keys should be sorted");
    }
}

#[test]
fn test_btree_reverse_insert_order() {
    let tree = BTree::new(63, make_bp());
    tree.init().unwrap();
    for i in (0..100u32).rev() {
        tree.insert(key(&format!("{:05}", i)), tuple(i, 1)).unwrap();
    }
    let all = tree.scan_prefix(b"").unwrap();
    for i in 1..all.len() {
        assert!(all[i - 1].0 <= all[i].0);
    }
}

#[test]
fn test_btree_stats() {
    let tree = BTree::new(64, make_bp());
    tree.init().unwrap();
    for i in 0..10 {
        tree.insert(key(&format!("s{}", i)), tuple(i, 1)).unwrap();
    }
    assert_eq!(tree.stats().inserts.load(std::sync::atomic::Ordering::Relaxed), 10);
    tree.lookup(&key("s0")).unwrap();
    assert!(tree.stats().lookups.load(std::sync::atomic::Ordering::Relaxed) >= 1);
}
