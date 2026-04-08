//! 序列化/反序列化 + 内部方法覆盖

use std::sync::Arc;
use ferrisdb_core::{BufferTag, PdbId, Xid, Lsn, Csn};
use ferrisdb_storage::*;
use ferrisdb_storage::wal::*;
use ferrisdb_storage::page::PAGE_SIZE;

// ============================================================
// BTree types 覆盖
// ============================================================

#[test]
fn test_btree_key_ordering() {
    let k1 = BTreeKey::new(b"aaa".to_vec());
    let k2 = BTreeKey::new(b"bbb".to_vec());
    let k3 = BTreeKey::new(b"aaa".to_vec());
    assert!(k1 < k2);
    assert_eq!(k1, k3);
    assert!(k2 > k1);
    assert_eq!(k1.compare(&k3), std::cmp::Ordering::Equal);
}

#[test]
fn test_btree_value_types() {
    let v1 = BTreeValue::Tuple { block: 1, offset: 2 };
    let v2 = BTreeValue::Child(42);
    // 验证 Debug/Clone
    let _ = format!("{:?}", v1);
    let _ = format!("{:?}", v2);
    let v1c = v1;
    let _ = v1c;
}

#[test]
fn test_btree_item_serialize_deserialize() {
    let item = BTreeItem {
        key: BTreeKey::new(b"test_key".to_vec()),
        value: BTreeValue::Tuple { block: 100, offset: 5 },
    };
    let bytes = item.serialize();
    let restored = BTreeItem::deserialize(&bytes);
    assert!(restored.is_some());
    let r = restored.unwrap();
    assert_eq!(r.key.data, b"test_key");
}

#[test]
fn test_btree_page_from_bytes() {
    #[repr(C, align(8192))]
    struct A { data: [u8; PAGE_SIZE] }
    let mut a = A { data: [0u8; PAGE_SIZE] };
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Leaf, 0);
    assert!(page.is_leaf());
    assert_eq!(page.header().nkeys, 0);
    assert_eq!(page.level(), 0);
}

#[test]
fn test_btree_page_internal() {
    #[repr(C, align(8192))]
    struct A { data: [u8; PAGE_SIZE] }
    let mut a = A { data: [0u8; PAGE_SIZE] };
    let page = BTreePage::from_bytes(&mut a.data);
    page.init(BTreePageType::Internal, 1);
    assert!(!page.is_leaf());
    assert_eq!(page.level(), 1);
    page.header_mut().first_child = 42;
    assert_eq!(page.header().first_child, 42);
}

// ============================================================
// WAL record types 覆盖
// ============================================================

#[test]
fn test_wal_record_header_new() {
    let h = WalRecordHeader::new(WalRecordType::HeapInsert, 100);
    assert_eq!({ h.size }, 100);
    assert_eq!(h.record_type(), Some(WalRecordType::HeapInsert));
    assert_eq!(WalRecordHeader::header_size(), 8);
}

#[test]
fn test_wal_record_for_page() {
    let page_id = PageId::new(0, 0, 1, 0);
    let rec = WalRecordForPage::new(WalRecordType::HeapInsert, page_id, 50);
    assert_eq!(rec.header.record_type(), Some(WalRecordType::HeapInsert));
}

#[test]
fn test_wal_heap_insert_serialize() {
    let page_id = PageId::new(0, 0, 1, 5);
    let data = vec![0xAB; 100];
    let rec = WalHeapInsert::new(page_id, 3, &data);
    let bytes = rec.serialize_with_data(&data);
    assert!(!bytes.is_empty());
}

#[test]
fn test_wal_heap_delete_serialize() {
    let page_id = PageId::new(0, 0, 1, 0);
    let rec = WalHeapDelete::new(page_id, 5);
    let bytes = rec.to_bytes();
    assert!(!bytes.is_empty());
}

#[test]
fn test_wal_heap_inplace_update_serialize() {
    let page_id = PageId::new(0, 0, 2, 0);
    let data = vec![0xCD; 64];
    let rec = WalHeapInplaceUpdate::new(page_id, 1, &data);
    let bytes = rec.serialize_with_data(&data);
    assert!(!bytes.is_empty());
}

#[test]
fn test_wal_txn_commit_abort() {
    let xid = Xid::new(0, 42);
    let commit = WalTxnCommit::new(xid, 100);
    let bytes = commit.to_bytes();
    assert!(!bytes.is_empty());

    let abort = WalTxnCommit::new_abort(xid);
    let abytes = abort.to_bytes();
    assert!(!abytes.is_empty());
}

#[test]
fn test_wal_checkpoint_record() {
    let ckpt = WalCheckpoint::new(12345, false);
    let bytes = ckpt.to_bytes();
    assert!(!bytes.is_empty());

    let ckpt_shutdown = WalCheckpoint::new(99999, true);
    let bytes2 = ckpt_shutdown.to_bytes();
    assert!(!bytes2.is_empty());
}

#[test]
fn test_wal_record_type_roundtrip() {
    for rtype in [WalRecordType::HeapInsert, WalRecordType::HeapDelete,
                  WalRecordType::HeapInplaceUpdate, WalRecordType::TxnCommit,
                  WalRecordType::TxnAbort, WalRecordType::CheckpointOnline,
                  WalRecordType::UndoInsertRecord, WalRecordType::UndoDeleteRecord,
                  WalRecordType::BtreeInsertOnLeaf, WalRecordType::DdlCreate] {
        let val = rtype as u16;
        assert!(WalRecordType::try_from(val).is_ok(), "Failed for {:?}", rtype);
    }
    // 无效值
    assert!(WalRecordType::try_from(9999u16).is_err());
}

// TupleHeader tests are in ferrisdb-transaction crate

// ============================================================
// Core types 覆盖
// ============================================================

#[test]
fn test_xid_operations() {
    let x = Xid::new(0, 42);
    assert_eq!(x.slot_id(), 42);
    assert!(x.is_valid());
    assert!(!Xid::INVALID.is_valid());
    assert_eq!(Xid::from_raw(x.raw()).raw(), x.raw());
}

#[test]
fn test_lsn_operations() {
    let l = Lsn::from_parts(1, 1000);
    assert!(l.is_valid());
    let (file, offset) = l.parts();
    assert_eq!(file, 1);
    assert_eq!(offset, 1000);
    assert!(!Lsn::INVALID.is_valid());
}

#[test]
fn test_csn_operations() {
    let c = Csn::from_raw(100);
    assert!(c.is_valid());
    assert!(!Csn::INVALID.is_valid());
    assert_eq!(c.raw(), 100);
}

#[test]
fn test_buffer_tag_operations() {
    let t1 = BufferTag::new(PdbId::new(0), 1, 0);
    let t2 = BufferTag::new(PdbId::new(0), 1, 0);
    let t3 = BufferTag::new(PdbId::new(0), 2, 0);
    assert_eq!(t1, t2);
    assert_ne!(t1, t3);
    assert!(t1.is_valid());
    assert!(!BufferTag::INVALID.is_valid());
    let _hash = t1.hash_value();
}

#[test]
fn test_page_id_operations() {
    let p = PageId::new(0, 0, 100, 5);
    let _ = format!("{:?}", p);
}

// ============================================================
// Snapshot 覆盖
// ============================================================

#[test]
fn test_snapshot_empty() {
    use ferrisdb_core::Snapshot;
    let s = Snapshot::new(Csn::from_raw(50), vec![]);
    assert!(!s.is_active(Xid::new(0, 1)));
}

#[test]
fn test_snapshot_with_active_xids() {
    use ferrisdb_core::Snapshot;
    let s = Snapshot::new(Csn::from_raw(50), vec![Xid::new(0, 3), Xid::new(0, 7)]);
    assert!(s.is_active(Xid::new(0, 3)));
    assert!(s.is_active(Xid::new(0, 7)));
    assert!(!s.is_active(Xid::new(0, 5)));
}

// ============================================================
// Config GUC 覆盖
// ============================================================

#[test]
fn test_config_all_fields() {
    let cfg = ferrisdb_core::config::config();
    let _ = cfg.shared_buffers.load(std::sync::atomic::Ordering::Relaxed);
    let _ = cfg.wal_buffers.load(std::sync::atomic::Ordering::Relaxed);
    let _ = cfg.max_connections.load(std::sync::atomic::Ordering::Relaxed);
    let _ = cfg.bgwriter_delay_ms.load(std::sync::atomic::Ordering::Relaxed);
    let _ = cfg.bgwriter_max_pages.load(std::sync::atomic::Ordering::Relaxed);
    let _ = cfg.wal_flusher_delay_ms.load(std::sync::atomic::Ordering::Relaxed);
    let _ = cfg.deadlock_timeout_ms.load(std::sync::atomic::Ordering::Relaxed);
    let _ = cfg.log_level.load(std::sync::atomic::Ordering::Relaxed);
    let _ = cfg.synchronous_commit.load(std::sync::atomic::Ordering::Relaxed);
    let _ = cfg.checkpoint_interval_ms.load(std::sync::atomic::Ordering::Relaxed);
    let _ = cfg.transaction_timeout_ms.load(std::sync::atomic::Ordering::Relaxed);
}

// ============================================================
// LWLock 覆盖
// ============================================================

#[test]
fn test_lwlock_shared_exclusive() {
    use ferrisdb_core::lock::LWLock;
    let lock = LWLock::new();
    lock.acquire_shared();
    lock.release_shared();
    lock.acquire_exclusive();
    lock.release_exclusive();
}

#[test]
fn test_lwlock_multiple_shared() {
    use ferrisdb_core::lock::LWLock;
    let lock = LWLock::new();
    lock.acquire_shared();
    lock.acquire_shared(); // 多个 shared 不冲突
    lock.release_shared();
    lock.release_shared();
}
