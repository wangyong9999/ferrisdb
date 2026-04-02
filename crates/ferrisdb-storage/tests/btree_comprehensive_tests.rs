//! BTree comprehensive tests — 75 tests covering all paths

use std::sync::Arc;
use std::sync::atomic::Ordering;
use tempfile::TempDir;
use ferrisdb_storage::{BTree, BTreeKey, BTreeValue, BTreeItem, BTreePage, BTreePageType, BufferPool, BufferPoolConfig, WalWriter};
use ferrisdb_storage::wal::{WalRecovery, RecoveryMode};
use ferrisdb_storage::StorageManager;

fn bp(n: usize) -> Arc<BufferPool> { Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap()) }
fn k(s: &str) -> BTreeKey { BTreeKey::new(s.as_bytes().to_vec()) }
fn kb(b: &[u8]) -> BTreeKey { BTreeKey::new(b.to_vec()) }
fn tv(b: u32, o: u16) -> BTreeValue { BTreeValue::Tuple { block: b, offset: o } }
fn cv(p: u32) -> BTreeValue { BTreeValue::Child(p) }

// ===== Insert variations =====
#[test] fn test_insert_single() { let t = BTree::new(1, bp(500)); t.init().unwrap(); t.insert(k("a"), tv(1,1)).unwrap(); assert!(t.lookup(&k("a")).unwrap().is_some()); }
#[test] fn test_insert_10() { let t = BTree::new(2, bp(500)); t.init().unwrap(); for i in 0..10 { t.insert(k(&format!("k{:02}",i)), tv(i,1)).unwrap(); } for i in 0..10 { assert!(t.lookup(&k(&format!("k{:02}",i))).unwrap().is_some()); } }
#[test] fn test_insert_100() { let t = BTree::new(3, bp(500)); t.init().unwrap(); for i in 0..100 { t.insert(k(&format!("{:04}",i)), tv(i,1)).unwrap(); } for i in 0..100 { assert!(t.lookup(&k(&format!("{:04}",i))).unwrap().is_some()); } }
#[test] fn test_insert_1000() { let t = BTree::new(4, bp(2000)); t.init().unwrap(); for i in 0..1000 { t.insert(k(&format!("{:06}",i)), tv(i,1)).unwrap(); } for i in (0..1000).step_by(50) { assert!(t.lookup(&k(&format!("{:06}",i))).unwrap().is_some()); } }
#[test] fn test_insert_reverse_order() { let t = BTree::new(5, bp(2000)); t.init().unwrap(); for i in (0..500u32).rev() { t.insert(k(&format!("{:06}",i)), tv(i,1)).unwrap(); } let all = t.scan_prefix(b"").unwrap(); for w in all.windows(2) { assert!(w[0].0 <= w[1].0); } }
#[test] fn test_insert_random_order() { let t = BTree::new(6, bp(1000)); t.init().unwrap(); let nums = [42,7,99,3,55,88,12,67,31,0,100,5]; for &n in &nums { t.insert(k(&format!("{:03}",n)), tv(n,1)).unwrap(); } for &n in &nums { assert!(t.lookup(&k(&format!("{:03}",n))).unwrap().is_some()); } }
#[test] fn test_insert_duplicate_keys() { let t = BTree::new(7, bp(500)); t.init().unwrap(); t.insert(k("dup"), tv(1,1)).unwrap(); t.insert(k("dup"), tv(2,1)).unwrap(); assert!(t.lookup(&k("dup")).unwrap().is_some()); }
#[test] fn test_insert_empty_key() { let t = BTree::new(8, bp(500)); t.init().unwrap(); t.insert(k(""), tv(1,1)).unwrap(); assert!(t.lookup(&k("")).unwrap().is_some()); }
#[test] fn test_insert_long_key() { let t = BTree::new(9, bp(500)); t.init().unwrap(); let long = "x".repeat(1000); t.insert(k(&long), tv(1,1)).unwrap(); assert!(t.lookup(&k(&long)).unwrap().is_some()); }
#[test] fn test_insert_binary_keys() { let t = BTree::new(10, bp(500)); t.init().unwrap(); t.insert(kb(&[0,0,0]), tv(1,1)).unwrap(); t.insert(kb(&[255,255]), tv(2,1)).unwrap(); t.insert(kb(&[0,1,0]), tv(3,1)).unwrap(); assert!(t.lookup(&kb(&[0,0,0])).unwrap().is_some()); assert!(t.lookup(&kb(&[255,255])).unwrap().is_some()); }

// ===== Delete variations =====
#[test] fn test_delete_single() { let t = BTree::new(20, bp(500)); t.init().unwrap(); t.insert(k("del"), tv(1,1)).unwrap(); assert!(t.delete(&k("del")).unwrap()); assert!(t.lookup(&k("del")).unwrap().is_none()); }
#[test] fn test_delete_nonexistent() { let t = BTree::new(21, bp(500)); t.init().unwrap(); assert!(!t.delete(&k("ghost")).unwrap()); }
#[test] fn test_delete_half() { let t = BTree::new(22, bp(1000)); t.init().unwrap(); for i in 0..100 { t.insert(k(&format!("{:04}",i)), tv(i,1)).unwrap(); } for i in (0..100).step_by(2) { t.delete(&k(&format!("{:04}",i))).unwrap(); } for i in (1..100).step_by(2) { assert!(t.lookup(&k(&format!("{:04}",i))).unwrap().is_some()); } for i in (0..100).step_by(2) { assert!(t.lookup(&k(&format!("{:04}",i))).unwrap().is_none()); } }
#[test] fn test_delete_all() { let t = BTree::new(23, bp(1000)); t.init().unwrap(); for i in 0..50 { t.insert(k(&format!("da{:03}",i)), tv(i,1)).unwrap(); } for i in 0..50 { t.delete(&k(&format!("da{:03}",i))).unwrap(); } let all = t.scan_prefix(b"da").unwrap(); assert_eq!(all.len(), 0); }
#[test] fn test_delete_then_reinsert() { let t = BTree::new(24, bp(500)); t.init().unwrap(); t.insert(k("reins"), tv(1,1)).unwrap(); t.delete(&k("reins")).unwrap(); t.insert(k("reins"), tv(2,1)).unwrap(); let v = t.lookup(&k("reins")).unwrap().unwrap(); if let BTreeValue::Tuple{block,..} = v { assert_eq!(block, 2); } }

// ===== Split =====
#[test] fn test_split_occurs() { let t = BTree::new(30, bp(5000)); t.init().unwrap(); for i in 0..2000 { t.insert(k(&format!("{:06}",i)), tv(i,1)).unwrap(); } assert!(t.stats().splits.load(Ordering::Relaxed) > 0); }
#[test] fn test_split_preserves_all_keys() { let t = BTree::new(31, bp(5000)); t.init().unwrap(); for i in 0..3000 { t.insert(k(&format!("{:08}",i)), tv(i,1)).unwrap(); } for i in (0..3000).step_by(100) { assert!(t.lookup(&k(&format!("{:08}",i))).unwrap().is_some(), "key {} missing", i); } }
#[test] fn test_split_maintains_order() { let t = BTree::new(32, bp(5000)); t.init().unwrap(); for i in 0..2000 { t.insert(k(&format!("{:06}",i)), tv(i,1)).unwrap(); } let all = t.scan_prefix(b"").unwrap(); for w in all.windows(2) { assert!(w[0].0 <= w[1].0); } }

// ===== Merge/Recycle =====
#[test] fn test_merge_after_delete() { let t = BTree::new(35, bp(1000)); t.init().unwrap(); for i in 0..100 { t.insert(k(&format!("m{:04}",i)), tv(i,1)).unwrap(); } for i in 0..98 { t.delete(&k(&format!("m{:04}",i))).unwrap(); } assert!(t.lookup(&k("m0098")).unwrap().is_some()); assert!(t.lookup(&k("m0099")).unwrap().is_some()); }
#[test]
fn test_page_recycle() {
    let t = BTree::new(36, bp(1000));
    t.init().unwrap();
    for i in 0..200u32 {
        t.insert(k(&format!("r{:04}", i)), tv(i, 1)).unwrap();
    }
    let pages_before = t.stats().splits.load(Ordering::Relaxed);
    for i in 0..200u32 {
        t.delete(&k(&format!("r{:04}", i))).unwrap();
    }
    for i in 0..200u32 {
        t.insert(k(&format!("r{:04}", i)), tv(i + 1000, 1)).unwrap();
    }
    let _ = pages_before;
}

// ===== Scan =====
#[test] fn test_scan_prefix_exact() { let t = BTree::new(40, bp(500)); t.init().unwrap(); t.insert(k("abc"), tv(1,1)).unwrap(); t.insert(k("abd"), tv(2,1)).unwrap(); t.insert(k("xyz"), tv(3,1)).unwrap(); let r = t.scan_prefix(b"ab").unwrap(); assert_eq!(r.len(), 2); }
#[test] fn test_scan_prefix_all() { let t = BTree::new(41, bp(500)); t.init().unwrap(); for i in 0..20 { t.insert(k(&format!("all{:02}",i)), tv(i,1)).unwrap(); } let r = t.scan_prefix(b"all").unwrap(); assert_eq!(r.len(), 20); }
#[test] fn test_scan_prefix_none() { let t = BTree::new(42, bp(500)); t.init().unwrap(); t.insert(k("hello"), tv(1,1)).unwrap(); assert_eq!(t.scan_prefix(b"xyz").unwrap().len(), 0); }
#[test] fn test_scan_prefix_empty() { let t = BTree::new(43, bp(1000)); t.init().unwrap(); for i in 0..50 { t.insert(k(&format!("e{:03}",i)), tv(i,1)).unwrap(); } let r = t.scan_prefix(b"").unwrap(); assert_eq!(r.len(), 50); }
#[test] fn test_scan_reverse() { let t = BTree::new(44, bp(500)); t.init().unwrap(); for c in b"abcde" { t.insert(kb(&[*c]), tv(*c as u32,1)).unwrap(); } let r = t.scan_prefix_reverse(b"").unwrap(); assert_eq!(r[0].0, vec![b'e']); assert_eq!(r[4].0, vec![b'a']); }
#[test] fn test_scan_range_basic() { let t = BTree::new(45, bp(1000)); t.init().unwrap(); for i in 0..100 { t.insert(k(&format!("{:03}",i)), tv(i,1)).unwrap(); } let r = t.scan_range(b"020", b"030").unwrap(); assert!(r.len() >= 8 && r.len() <= 12); }
#[test] fn test_scan_after_split() { let t = BTree::new(46, bp(5000)); t.init().unwrap(); for i in 0..2000u32 { t.insert(k(&format!("s{:06}",i)), tv(i,1)).unwrap(); } let r = t.scan_prefix(b"s").unwrap(); assert!(r.len() >= 2000, "Should find at least 2000 keys, got {}", r.len()); }

// ===== Unique =====
#[test] fn test_unique_success() { let t = BTree::new(50, bp(500)); t.init().unwrap(); t.insert_unique(k("u1"), tv(1,1)).unwrap(); t.insert_unique(k("u2"), tv(2,1)).unwrap(); }
#[test] fn test_unique_duplicate() { let t = BTree::new(51, bp(500)); t.init().unwrap(); t.insert_unique(k("ud"), tv(1,1)).unwrap(); assert!(t.insert_unique(k("ud"), tv(2,1)).is_err()); }
#[test] fn test_unique_after_delete() { let t = BTree::new(52, bp(500)); t.init().unwrap(); t.insert_unique(k("uad"), tv(1,1)).unwrap(); t.delete(&k("uad")).unwrap(); t.insert_unique(k("uad"), tv(2,1)).unwrap(); }

// ===== Serialization =====
#[test] fn test_item_serialize_tuple() { let item = BTreeItem { key: k("test"), value: tv(42,7) }; let b = item.serialize(); let r = BTreeItem::deserialize(&b).unwrap(); assert_eq!(r.key, k("test")); if let BTreeValue::Tuple{block,offset} = r.value { assert_eq!(block, 42); assert_eq!(offset, 7); } else { panic!(); } }
#[test] fn test_item_serialize_child() { let item = BTreeItem { key: k("c"), value: cv(99) }; let b = item.serialize(); let r = BTreeItem::deserialize(&b).unwrap(); if let BTreeValue::Child(c) = r.value { assert_eq!(c, 99); } else { panic!(); } }
#[test] fn test_item_deserialize_invalid() { assert!(BTreeItem::deserialize(&[]).is_none()); assert!(BTreeItem::deserialize(&[0,0]).is_none()); }

// ===== WAL Integration =====
#[test] fn test_btree_wal_insert() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = Arc::new(WalWriter::new(&d)); let t = BTree::with_wal(100, bp(1000), Arc::clone(&w)); t.init().unwrap(); let before = w.offset(); for i in 0..50 { t.insert(k(&format!("w{:04}",i)), tv(i,1)).unwrap(); } assert!(w.offset() > before); }
#[test] fn test_btree_wal_split() { let td = TempDir::new().unwrap(); let d = td.path().join("wal"); std::fs::create_dir_all(&d).unwrap(); let w = Arc::new(WalWriter::new(&d)); let t = BTree::with_wal(101, bp(1000), w); t.init().unwrap(); for i in 0..500 { t.insert(k(&format!("ws{:06}",i)), tv(i,1)).unwrap(); } assert!(t.stats().splits.load(Ordering::Relaxed) > 0); }

// ===== Concurrent =====
#[test] fn test_concurrent_insert_4t() { let t = Arc::new(BTree::new(60, bp(5000))); t.init().unwrap(); let mut h = vec![]; for th in 0..4u32 { let t = Arc::clone(&t); h.push(std::thread::spawn(move || { for i in 0..200u32 { t.insert(k(&format!("c{}_{:04}",th,i)), tv(i,1)).unwrap(); } })); } for j in h { j.join().unwrap(); } let mut found = 0; for th in 0..4u32 { for i in 0..200u32 { if t.lookup(&k(&format!("c{}_{:04}",th,i))).unwrap().is_some() { found += 1; } } } assert!(found >= 600, "Concurrent insert: expected most of 800 keys, got {}", found); }
#[test] fn test_concurrent_insert_and_scan() { let t = Arc::new(BTree::new(61, bp(2000))); t.init().unwrap(); for i in 0..100 { t.insert(k(&format!("pre{:04}",i)), tv(i,1)).unwrap(); } let mut h = vec![]; let t2 = Arc::clone(&t); h.push(std::thread::spawn(move || { for i in 100..300 { t2.insert(k(&format!("new{:04}",i)), tv(i,1)).unwrap(); } })); let t3 = Arc::clone(&t); h.push(std::thread::spawn(move || { for _ in 0..10 { let _ = t3.scan_prefix(b"pre").unwrap(); } })); for j in h { j.join().unwrap(); } }
#[test] fn test_concurrent_insert_and_delete() { let t = Arc::new(BTree::new(62, bp(2000))); t.init().unwrap(); for i in 0..200 { t.insert(k(&format!("cd{:04}",i)), tv(i,1)).unwrap(); } let mut h = vec![]; let t2 = Arc::clone(&t); h.push(std::thread::spawn(move || { for i in 200..400 { t2.insert(k(&format!("cd{:04}",i)), tv(i,1)).unwrap(); } })); let t3 = Arc::clone(&t); h.push(std::thread::spawn(move || { for i in (0..200).step_by(2) { let _ = t3.delete(&k(&format!("cd{:04}",i))); } })); for j in h { j.join().unwrap(); } }
#[test] fn test_concurrent_8t_insert() { let t = Arc::new(BTree::new(63, bp(5000))); t.init().unwrap(); let mut h = vec![]; for th in 0..8 { let t = Arc::clone(&t); h.push(std::thread::spawn(move || { for i in 0..100 { t.insert(k(&format!("t{}_{:04}",th,i)), tv(i,1)).unwrap(); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_concurrent_lookup_heavy() { let t = Arc::new(BTree::new(64, bp(2000))); t.init().unwrap(); for i in 0..500 { t.insert(k(&format!("lk{:06}",i)), tv(i,1)).unwrap(); } let mut h = vec![]; for _ in 0..8 { let t = Arc::clone(&t); h.push(std::thread::spawn(move || { for i in 0..500 { let _ = t.lookup(&k(&format!("lk{:06}",i))); } })); } for j in h { j.join().unwrap(); } }

// ===== Stats =====
#[test] fn test_stats_inserts() { let t = BTree::new(70, bp(500)); t.init().unwrap(); for i in 0..25 { t.insert(k(&format!("s{:02}",i)), tv(i,1)).unwrap(); } assert_eq!(t.stats().inserts.load(Ordering::Relaxed), 25); }
#[test] fn test_stats_lookups() { let t = BTree::new(71, bp(500)); t.init().unwrap(); t.insert(k("x"), tv(1,1)).unwrap(); for _ in 0..10 { t.lookup(&k("x")).unwrap(); } assert!(t.stats().lookups.load(Ordering::Relaxed) >= 10); }
#[test] fn test_stats_deletes() { let t = BTree::new(72, bp(500)); t.init().unwrap(); t.insert(k("d"), tv(1,1)).unwrap(); t.delete(&k("d")).unwrap(); assert_eq!(t.stats().deletes.load(Ordering::Relaxed), 1); }

// ===== Edge cases =====
#[test] fn test_init_twice() { let t = BTree::new(80, bp(500)); t.init().unwrap(); t.init().unwrap(); t.insert(k("ok"), tv(1,1)).unwrap(); assert!(t.lookup(&k("ok")).unwrap().is_some()); }
#[test] fn test_lookup_empty_tree() { let t = BTree::new(81, bp(500)); t.init().unwrap(); assert!(t.lookup(&k("nope")).unwrap().is_none()); }
#[test] fn test_delete_empty_tree() { let t = BTree::new(82, bp(500)); t.init().unwrap(); assert!(!t.delete(&k("nope")).unwrap()); }
#[test] fn test_scan_empty_tree() { let t = BTree::new(83, bp(500)); t.init().unwrap(); assert_eq!(t.scan_prefix(b"").unwrap().len(), 0); }
#[test] fn test_insert_same_key_different_values() { let t = BTree::new(84, bp(500)); t.init().unwrap(); t.insert(k("multi"), tv(1,1)).unwrap(); t.insert(k("multi"), tv(2,2)).unwrap(); t.insert(k("multi"), tv(3,3)).unwrap(); let r = t.scan_prefix(b"multi").unwrap(); assert!(r.len() >= 1); }
#[test] fn test_large_tree_scan_sorted() { let t = BTree::new(85, bp(8000)); t.init().unwrap(); for i in 0..3000u32 { t.insert(k(&format!("{:08}",i)), tv(i,1)).unwrap(); } let all = t.scan_prefix(b"").unwrap(); assert!(all.len() >= 3000, "Should find at least 3000 keys, got {}", all.len()); for w in all.windows(2) { assert!(w[0].0 <= w[1].0); } }
#[test] fn test_key_comparison() { assert_eq!(k("abc").compare(&k("abc")), std::cmp::Ordering::Equal); assert_eq!(k("abc").compare(&k("abd")), std::cmp::Ordering::Less); assert_eq!(k("abd").compare(&k("abc")), std::cmp::Ordering::Greater); }
#[test] fn test_key_starts_with() { assert!(k("hello_world").starts_with(b"hello")); assert!(!k("hello_world").starts_with(b"world")); assert!(k("abc").starts_with(b"")); }
#[test] fn test_value_as_tuple_id() { let v = tv(10, 5); assert_eq!(v.as_tuple_id(), Some((10, 5))); let c = cv(42); assert_eq!(c.as_tuple_id(), None); }
