//! BTree full coverage — 30 additional tests

use std::sync::Arc;
use std::sync::atomic::Ordering;
use ferrisdb_storage::{BTree, BTreeKey, BTreeValue, BTreeItem, BufferPool, BufferPoolConfig};

fn bp(n: usize) -> Arc<BufferPool> { Arc::new(BufferPool::new(BufferPoolConfig::new(n)).unwrap()) }
fn k(s: &str) -> BTreeKey { BTreeKey::new(s.as_bytes().to_vec()) }
fn tv(b: u32, o: u16) -> BTreeValue { BTreeValue::Tuple { block: b, offset: o } }

// ===== Insert edge cases =====
#[test] fn test_insert_same_prefix() { let t = BTree::new(1, bp(1000)); t.init().unwrap(); for i in 0..100 { t.insert(k(&format!("prefix_{:04}", i)), tv(i,1)).unwrap(); } let r = t.scan_prefix(b"prefix_").unwrap(); assert_eq!(r.len(), 100); }
#[test] fn test_insert_long_keys() { let t = BTree::new(2, bp(1000)); t.init().unwrap(); for i in 0..50 { let key = format!("{}{:04}", "x".repeat(200), i); t.insert(k(&key), tv(i,1)).unwrap(); } }
#[test] fn test_insert_1_byte_keys() { let t = BTree::new(3, bp(500)); t.init().unwrap(); for b in 0..=255u8 { t.insert(BTreeKey::new(vec![b]), tv(b as u32,1)).unwrap(); } }
#[test] fn test_insert_and_lookup_all() { let t = BTree::new(4, bp(2000)); t.init().unwrap(); for i in 0..500 { t.insert(k(&format!("{:06}",i)), tv(i,1)).unwrap(); } for i in 0..500 { assert!(t.lookup(&k(&format!("{:06}",i))).unwrap().is_some()); } }

// ===== Delete edge cases =====
#[test] fn test_delete_first_key() { let t = BTree::new(10, bp(500)); t.init().unwrap(); t.insert(k("a"), tv(1,1)).unwrap(); t.insert(k("b"), tv(2,1)).unwrap(); t.insert(k("c"), tv(3,1)).unwrap(); t.delete(&k("a")).unwrap(); assert!(t.lookup(&k("a")).unwrap().is_none()); assert!(t.lookup(&k("b")).unwrap().is_some()); }
#[test] fn test_delete_last_key() { let t = BTree::new(11, bp(500)); t.init().unwrap(); t.insert(k("a"), tv(1,1)).unwrap(); t.insert(k("b"), tv(2,1)).unwrap(); t.insert(k("c"), tv(3,1)).unwrap(); t.delete(&k("c")).unwrap(); assert!(t.lookup(&k("c")).unwrap().is_none()); assert!(t.lookup(&k("b")).unwrap().is_some()); }
#[test] fn test_delete_middle_key() { let t = BTree::new(12, bp(500)); t.init().unwrap(); t.insert(k("a"), tv(1,1)).unwrap(); t.insert(k("b"), tv(2,1)).unwrap(); t.insert(k("c"), tv(3,1)).unwrap(); t.delete(&k("b")).unwrap(); assert!(t.lookup(&k("b")).unwrap().is_none()); }
#[test] fn test_delete_all_and_reinsert() { let t = BTree::new(13, bp(1000)); t.init().unwrap(); for i in 0..100 { t.insert(k(&format!("k{:03}",i)), tv(i,1)).unwrap(); } for i in 0..100 { t.delete(&k(&format!("k{:03}",i))).unwrap(); } for i in 0..100 { t.insert(k(&format!("k{:03}",i)), tv(i+1000,1)).unwrap(); } for i in 0..100 { assert!(t.lookup(&k(&format!("k{:03}",i))).unwrap().is_some()); } }

// ===== Scan edge cases =====
#[test] fn test_scan_single_key() { let t = BTree::new(20, bp(500)); t.init().unwrap(); t.insert(k("only"), tv(1,1)).unwrap(); let r = t.scan_prefix(b"only").unwrap(); assert_eq!(r.len(), 1); }
#[test] fn test_scan_no_match() { let t = BTree::new(21, bp(500)); t.init().unwrap(); t.insert(k("abc"), tv(1,1)).unwrap(); assert_eq!(t.scan_prefix(b"xyz").unwrap().len(), 0); }
#[test] fn test_scan_all_match() { let t = BTree::new(22, bp(500)); t.init().unwrap(); for i in 0..10 { t.insert(k(&format!("a{}", i)), tv(i,1)).unwrap(); } assert_eq!(t.scan_prefix(b"a").unwrap().len(), 10); }
#[test] fn test_scan_range_empty() { let t = BTree::new(23, bp(500)); t.init().unwrap(); t.insert(k("a"), tv(1,1)).unwrap(); assert_eq!(t.scan_range(b"m", b"z").unwrap().len(), 0); }
#[test] fn test_scan_reverse_order() { let t = BTree::new(24, bp(500)); t.init().unwrap(); for c in b"abcdef" { t.insert(BTreeKey::new(vec![*c]), tv(*c as u32,1)).unwrap(); } let r = t.scan_prefix_reverse(b"").unwrap(); assert_eq!(r[0].0, vec![b'f']); }

// ===== Unique =====
#[test] fn test_unique_many() { let t = BTree::new(30, bp(1000)); t.init().unwrap(); for i in 0..100 { t.insert_unique(k(&format!("u{:03}",i)), tv(i,1)).unwrap(); } }
#[test] fn test_unique_dup_after_many() { let t = BTree::new(31, bp(1000)); t.init().unwrap(); for i in 0..100 { t.insert_unique(k(&format!("u{:03}",i)), tv(i,1)).unwrap(); } assert!(t.insert_unique(k("u050"), tv(999,1)).is_err()); }

// ===== Concurrent =====
#[test] fn test_conc_scan_only() { let t = Arc::new(BTree::new(40, bp(2000))); t.init().unwrap(); for i in 0..200 { t.insert(k(&format!("cs{:04}",i)), tv(i,1)).unwrap(); } let mut h = vec![]; for _ in 0..4 { let t = t.clone(); h.push(std::thread::spawn(move || { for _ in 0..50 { let _ = t.scan_prefix(b"cs").unwrap(); } })); } for j in h { j.join().unwrap(); } }
#[test] fn test_conc_lookup_only() { let t = Arc::new(BTree::new(41, bp(2000))); t.init().unwrap(); for i in 0..500 { t.insert(k(&format!("cl{:06}",i)), tv(i,1)).unwrap(); } let mut h = vec![]; for _ in 0..8 { let t = t.clone(); h.push(std::thread::spawn(move || { for i in 0..500 { let _ = t.lookup(&k(&format!("cl{:06}",i))); } })); } for j in h { j.join().unwrap(); } }

// ===== Stats =====
#[test] fn test_stats_splits_counted() { let t = BTree::new(50, bp(5000)); t.init().unwrap(); for i in 0..3000u32 { t.insert(k(&format!("{:08}",i)), tv(i,1)).unwrap(); } assert!(t.stats().splits.load(Ordering::Relaxed) > 0); }
#[test] fn test_stats_all() { let t = BTree::new(51, bp(500)); t.init().unwrap(); t.insert(k("x"), tv(1,1)).unwrap(); t.lookup(&k("x")).unwrap(); t.delete(&k("x")).unwrap(); assert_eq!(t.stats().inserts.load(Ordering::Relaxed), 1); assert!(t.stats().lookups.load(Ordering::Relaxed) >= 1); assert_eq!(t.stats().deletes.load(Ordering::Relaxed), 1); }

// ===== Key operations =====
#[test] fn test_key_eq() { assert_eq!(k("abc"), k("abc")); assert_ne!(k("abc"), k("abd")); }
#[test] fn test_key_ord() { assert!(k("abc") < k("abd")); assert!(k("z") > k("a")); }
#[test] fn test_key_starts_with() { assert!(k("hello").starts_with(b"hel")); assert!(!k("hello").starts_with(b"world")); }
#[test] fn test_key_empty() { let e = k(""); assert_eq!(e.len(), 0); assert!(e.is_empty()); }
#[test] fn test_item_size() { let item = BTreeItem { key: k("test"), value: tv(1,1) }; assert!(item.serialized_size() > 0); }
