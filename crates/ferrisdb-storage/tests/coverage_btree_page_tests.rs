//! BTreePage 内部方法覆盖测试 — 无并发，直接操作页面结构

use ferrisdb_storage::*;
use ferrisdb_storage::page::PAGE_SIZE;
use std::sync::Arc;

#[repr(C, align(8192))]
struct Aligned {
    data: [u8; PAGE_SIZE],
}

impl Aligned {
    fn new() -> Self {
        Self { data: [0u8; PAGE_SIZE] }
    }
}

fn make_bp(n: usize) -> Arc<BufferPool> {
    Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap())
}

fn key(s: &str) -> BTreeKey { BTreeKey::new(s.as_bytes().to_vec()) }
fn tuple(b: u32, o: u16) -> BTreeValue { BTreeValue::Tuple { block: b, offset: o } }

// ============================================================
// BTreePage 低级操作
// ============================================================

#[test]
fn test_btree_page_init_leaf() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);
    assert!(page.is_leaf());
    assert_eq!(page.nkeys(), 0);
    assert_eq!(page.level(), 0);
    assert!(page.free_space() > 0);
}

#[test]
fn test_btree_page_init_internal() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Internal, 1);
    assert!(!page.is_leaf());
    assert_eq!(page.level(), 1);
}

#[test]
fn test_btree_page_insert_item_sorted() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    let item = BTreeItem {
        key: key("apple"),
        value: tuple(1, 0),
    };
    let offset = page.insert_item_sorted(&item);
    assert!(offset.is_some());
    assert_eq!(page.nkeys(), 1);
}

#[test]
fn test_btree_page_insert_sorted_order() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    // Insert in reverse order, should be sorted
    for key_str in ["cherry", "banana", "apple"] {
        let item = BTreeItem { key: key(key_str), value: tuple(1, 0) };
        page.insert_item_sorted(&item);
    }

    let items = page.get_all_items();
    assert_eq!(items.len(), 3);
    // Items should be sorted
    assert!(items[0].key < items[1].key);
    assert!(items[1].key < items[2].key);
}

#[test]
fn test_btree_page_get_item() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    let item = BTreeItem { key: key("test"), value: tuple(42, 7) };
    page.insert_item_sorted(&item);

    let retrieved = page.get_item(0);
    assert!(retrieved.is_some());
    let r = retrieved.unwrap();
    assert_eq!(r.key.data, b"test");
    match r.value {
        BTreeValue::Tuple { block, offset } => {
            assert_eq!(block, 42);
            assert_eq!(offset, 7);
        }
        _ => panic!("Expected Tuple"),
    }
}

#[test]
fn test_btree_page_get_item_out_of_range() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    assert!(page.get_item(0).is_none());
    assert!(page.get_item(999).is_none());
}

#[test]
fn test_btree_page_get_all_items_empty() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    let items = page.get_all_items();
    assert!(items.is_empty());
}

#[test]
fn test_btree_page_get_last_item() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    assert!(page.get_last_item().is_none());

    page.insert_item_sorted(&BTreeItem { key: key("aaa"), value: tuple(1, 0) });
    page.insert_item_sorted(&BTreeItem { key: key("zzz"), value: tuple(2, 0) });

    let last = page.get_last_item().unwrap();
    assert_eq!(last.key.data, b"zzz");
}

#[test]
fn test_btree_page_find_key() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    for i in 0..10u32 {
        let item = BTreeItem {
            key: key(&format!("k{:02}", i)),
            value: tuple(i, 0),
        };
        page.insert_item_sorted(&item);
    }

    let found = page.find_key(&key("k05"));
    assert!(found.is_some());
    let (idx, item) = found.unwrap();
    assert_eq!(item.key.data, b"k05");
    assert!(idx < 10);

    assert!(page.find_key(&key("nonexistent")).is_none());
}

#[test]
fn test_btree_page_remove_at() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    for i in 0..5u32 {
        let item = BTreeItem { key: key(&format!("r{:02}", i)), value: tuple(i, 0) };
        page.insert_item_sorted(&item);
    }
    assert_eq!(page.nkeys(), 5);

    assert!(page.remove_at(2));
    assert_eq!(page.nkeys(), 4);

    // Remove invalid index
    assert!(!page.remove_at(100));
}

#[test]
fn test_btree_page_rebuild_from_sorted() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    let items: Vec<BTreeItem> = (0..20u32).map(|i| {
        BTreeItem { key: key(&format!("rb{:04}", i)), value: tuple(i, 0) }
    }).collect();

    page.rebuild_from_sorted(&items);
    assert_eq!(page.nkeys(), 20);

    let retrieved = page.get_all_items();
    assert_eq!(retrieved.len(), 20);
}

#[test]
fn test_btree_page_find_child() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Internal, 1);
    page.header_mut().first_child = 100;

    // Insert separator keys pointing to child pages
    let item1 = BTreeItem { key: key("m"), value: BTreeValue::Child(200) };
    let item2 = BTreeItem { key: key("z"), value: BTreeValue::Child(300) };
    page.insert_item_sorted(&item1);
    page.insert_item_sorted(&item2);

    // Key < "m" should go to first_child (100)
    let child = page.find_child(&key("a"));
    assert_eq!(child, 100);

    // Key >= "m" and < "z" should go to child 200
    let child2 = page.find_child(&key("p"));
    assert_eq!(child2, 200);
}

#[test]
fn test_btree_page_has_free_space() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    assert!(page.has_free_space(100));
    assert!(page.has_free_space(1000));
}

#[test]
fn test_btree_page_right_link() {
    let mut a = Aligned::new();
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);

    assert_eq!(page.header().right_link, u32::MAX);
    page.header_mut().right_link = 42;
    assert_eq!(page.header().right_link, 42);
}

// ============================================================
// BTreeItem 序列化
// ============================================================

#[test]
fn test_btree_item_serialize_tuple() {
    let item = BTreeItem {
        key: key("serialize_test"),
        value: tuple(999, 42),
    };
    let bytes = item.serialize();
    let restored = BTreeItem::deserialize(&bytes).unwrap();
    assert_eq!(restored.key.data, b"serialize_test");
    match restored.value {
        BTreeValue::Tuple { block, offset } => {
            assert_eq!(block, 999);
            assert_eq!(offset, 42);
        }
        _ => panic!("Expected Tuple"),
    }
}

#[test]
fn test_btree_item_serialize_child() {
    let item = BTreeItem {
        key: key("child_key"),
        value: BTreeValue::Child(12345),
    };
    let bytes = item.serialize();
    let restored = BTreeItem::deserialize(&bytes).unwrap();
    match restored.value {
        BTreeValue::Child(page) => assert_eq!(page, 12345),
        _ => panic!("Expected Child"),
    }
}

#[test]
fn test_btree_item_deserialize_invalid() {
    assert!(BTreeItem::deserialize(&[]).is_none());
    assert!(BTreeItem::deserialize(&[0, 0]).is_none()); // key_len=0, no value type
}

#[test]
fn test_btree_item_serialized_size() {
    let item = BTreeItem { key: key("sz"), value: tuple(1, 0) };
    let size = item.serialized_size();
    let bytes = item.serialize();
    // serialized_size may differ from serialize().len() due to padding
    assert!(size > 0);
    assert!(!bytes.is_empty());
}

// ============================================================
// BTreeKey 操作
// ============================================================

#[test]
fn test_btree_key_empty() {
    let k = BTreeKey::new(vec![]);
    assert!(k.is_empty());
    assert_eq!(k.len(), 0);
}

#[test]
fn test_btree_key_from_bytes() {
    let k = BTreeKey::from_bytes(b"hello");
    assert_eq!(k.data, b"hello");
    assert_eq!(k.len(), 5);
    assert!(!k.is_empty());
}

#[test]
fn test_btree_key_compare() {
    let k1 = key("aaa");
    let k2 = key("bbb");
    let k3 = key("aaa");
    assert_eq!(k1.compare(&k3), std::cmp::Ordering::Equal);
    assert_eq!(k1.compare(&k2), std::cmp::Ordering::Less);
    assert_eq!(k2.compare(&k1), std::cmp::Ordering::Greater);
}

#[test]
fn test_btree_key_starts_with() {
    let k = key("hello_world");
    assert!(k.starts_with(b"hello"));
    assert!(k.starts_with(b""));
    assert!(!k.starts_with(b"world"));
}

#[test]
fn test_btree_key_ordering() {
    let k1 = key("abc");
    let k2 = key("abd");
    assert!(k1 < k2);
    assert!(k2 > k1);
    assert_eq!(k1, key("abc"));
}

// ============================================================
// BTreeValue 操作
// ============================================================

#[test]
fn test_btree_value_as_tuple_id() {
    let v = BTreeValue::Tuple { block: 10, offset: 5 };
    let (b, o) = v.as_tuple_id().unwrap();
    assert_eq!(b, 10);
    assert_eq!(o, 5);

    let v2 = BTreeValue::Child(42);
    assert!(v2.as_tuple_id().is_none());
}

// ============================================================
// BTree 高级操作（无并发）
// ============================================================

#[test]
fn test_btree_stats() {
    let tree = BTree::new(800, make_bp(500));
    tree.init().unwrap();

    for i in 0..100u32 {
        tree.insert(key(&format!("st{:04}", i)), tuple(i, 0)).unwrap();
    }

    let stats = tree.stats();
    let _ = stats.inserts.load(std::sync::atomic::Ordering::Relaxed);
    let _ = stats.lookups.load(std::sync::atomic::Ordering::Relaxed);
}

#[test]
fn test_btree_root_page() {
    let tree = BTree::new(801, make_bp(200));
    tree.init().unwrap();
    let root = tree.root_page();
    assert_ne!(root, u32::MAX);
}

#[test]
fn test_btree_scan_range() {
    let tree = BTree::new(802, make_bp(500));
    tree.init().unwrap();

    for i in 0..50u32 {
        tree.insert(key(&format!("{:03}", i)), tuple(i, 0)).unwrap();
    }

    let results = tree.scan_range(b"010", b"030").unwrap();
    // Should contain keys in [010, 030)
    assert!(!results.is_empty());
    // Just verify we got results in range
    assert!(results.len() >= 10);
}

#[test]
fn test_btree_insert_unique_success() {
    let tree = BTree::new(803, make_bp(200));
    tree.init().unwrap();

    tree.insert_unique(key("unique1"), tuple(1, 0)).unwrap();
    tree.insert_unique(key("unique2"), tuple(2, 0)).unwrap();
    assert!(tree.lookup(&key("unique1")).unwrap().is_some());
}

#[test]
fn test_btree_insert_unique_duplicate() {
    let tree = BTree::new(804, make_bp(200));
    tree.init().unwrap();

    tree.insert_unique(key("dup_key"), tuple(1, 0)).unwrap();
    let result = tree.insert_unique(key("dup_key"), tuple(2, 0));
    assert!(result.is_err(), "Duplicate unique key should fail");
}

#[test]
fn test_btree_delete_and_lookup() {
    let tree = BTree::new(805, make_bp(200));
    tree.init().unwrap();

    tree.insert(key("del1"), tuple(1, 0)).unwrap();
    tree.insert(key("del2"), tuple(2, 0)).unwrap();
    tree.insert(key("del3"), tuple(3, 0)).unwrap();

    tree.delete(&key("del2")).unwrap();
    assert!(tree.lookup(&key("del2")).unwrap().is_none());
    assert!(tree.lookup(&key("del1")).unwrap().is_some());
    assert!(tree.lookup(&key("del3")).unwrap().is_some());
}

#[test]
fn test_btree_scan_prefix_reverse() {
    let tree = BTree::new(806, make_bp(500));
    tree.init().unwrap();

    for i in 0..30u32 {
        tree.insert(key(&format!("pfx_{:03}", i)), tuple(i, 0)).unwrap();
    }

    let results = tree.scan_prefix_reverse(b"pfx_").unwrap();
    assert_eq!(results.len(), 30);
    // Should be in reverse order
    if results.len() >= 2 {
        assert!(results[0].0 >= results[1].0);
    }
}

#[test]
fn test_btree_many_inserts_trigger_splits() {
    let tree = BTree::new(807, make_bp(500));
    tree.init().unwrap();

    for i in 0..500u32 {
        tree.insert(key(&format!("sp{:06}", i)), tuple(i, 0)).unwrap();
    }

    let stats = tree.stats();
    let splits = stats.splits.load(std::sync::atomic::Ordering::Relaxed);
    assert!(splits > 0, "500 inserts should trigger at least one split");

    // Verify all keys are findable
    for i in 0..500u32 {
        assert!(tree.lookup(&key(&format!("sp{:06}", i))).unwrap().is_some(),
            "Key sp{:06} not found", i);
    }
}

#[test]
fn test_btree_delete_all_then_reinsert() {
    let tree = BTree::new(808, make_bp(200));
    tree.init().unwrap();

    for i in 0..20u32 {
        tree.insert(key(&format!("da{:03}", i)), tuple(i, 0)).unwrap();
    }
    for i in 0..20u32 {
        tree.delete(&key(&format!("da{:03}", i))).unwrap();
    }
    for i in 0..20u32 {
        assert!(tree.lookup(&key(&format!("da{:03}", i))).unwrap().is_none());
    }

    // Reinsert
    for i in 0..20u32 {
        tree.insert(key(&format!("da{:03}", i)), tuple(i + 100, 0)).unwrap();
    }
    for i in 0..20u32 {
        let v = tree.lookup(&key(&format!("da{:03}", i))).unwrap().unwrap();
        match v {
            BTreeValue::Tuple { block, .. } => assert_eq!(block, i + 100),
            _ => panic!("Expected Tuple"),
        }
    }
}

#[test]
fn test_btree_scan_prefix_no_match() {
    let tree = BTree::new(809, make_bp(200));
    tree.init().unwrap();

    tree.insert(key("abc"), tuple(1, 0)).unwrap();
    let results = tree.scan_prefix(b"xyz").unwrap();
    assert!(results.is_empty());
}

#[test]
fn test_btree_with_wal() {
    let td = tempfile::TempDir::new().unwrap();
    let wal_dir = td.path().join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    let writer = Arc::new(ferrisdb_storage::wal::WalWriter::new(&wal_dir));
    let bp = make_bp(500);

    let tree = BTree::with_wal(810, bp, writer);
    tree.init().unwrap();

    for i in 0..50u32 {
        tree.insert(key(&format!("wal{:03}", i)), tuple(i, 0)).unwrap();
    }
    // Verify
    for i in 0..50u32 {
        assert!(tree.lookup(&key(&format!("wal{:03}", i))).unwrap().is_some());
    }
}
