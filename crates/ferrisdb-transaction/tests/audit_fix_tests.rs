//! Tests for audit report fixes (transaction-layer)
//!
//! Covers: H3 (Undo WAL types), H14 (Transaction Drop safety)

use std::sync::Arc;
use ferrisdb_core::Xid;
use ferrisdb_transaction::TransactionManager;

// ============================================================
// H3: Undo WAL record type differentiation
// ============================================================

#[test]
fn test_undo_wal_types_differentiated() {
    use ferrisdb_transaction::transaction::UndoAction;

    let xid = Xid::new(0, 1);

    let actions: Vec<(UndoAction, u16)> = vec![
        (UndoAction::Insert { table_oid: 1, page_no: 0, tuple_offset: 0 }, 160),
        (UndoAction::Delete { table_oid: 1, page_no: 0, tuple_offset: 0, old_data: vec![1, 2] }, 161),
        (UndoAction::InplaceUpdate { table_oid: 1, page_no: 0, tuple_offset: 0, old_data: vec![3, 4] }, 162),
        (UndoAction::UpdateOldPage { table_oid: 1, page_no: 0, tuple_offset: 0, old_header: [0u8; 32] }, 163),
        (UndoAction::UpdateNewPage { table_oid: 1, page_no: 0, tuple_offset: 0 }, 164),
    ];

    for (action, expected_type) in actions {
        let bytes = action.serialize_for_wal(xid);
        // WalRecordHeader is packed: [size:2][rtype:2][crc:4]
        let rtype = u16::from_le_bytes([bytes[2], bytes[3]]);
        assert_eq!(rtype, expected_type, "Undo action {:?} should have rtype {}", bytes[10], expected_type);
    }
}

// ============================================================
// H14: Transaction Drop safety
// ============================================================

#[test]
fn test_commit_then_drop_no_double_abort() {
    let mgr = Arc::new(TransactionManager::new(16));
    let mut txn = mgr.begin().unwrap();
    assert!(txn.is_active());
    txn.commit().unwrap();
    assert!(!txn.is_active());
    drop(txn); // Should NOT call abort
}

#[test]
fn test_drop_uncommitted_txn_releases_slot() {
    let mgr = Arc::new(TransactionManager::new(4));
    // Fill 3 slots (slots 1, 2, 3; slot 0 is reserved)
    {
        let _t1 = mgr.begin().unwrap();
        let _t2 = mgr.begin().unwrap();
        let _t3 = mgr.begin().unwrap();
        // Dropping all 3 — slots should be released
    }
    // Should be able to begin 3 new transactions
    let _t4 = mgr.begin().unwrap();
    let _t5 = mgr.begin().unwrap();
    let _t6 = mgr.begin().unwrap();
}

#[test]
fn test_abort_then_drop_no_panic() {
    let mgr = Arc::new(TransactionManager::new(16));
    let mut txn = mgr.begin().unwrap();
    txn.abort().unwrap();
    assert!(!txn.is_active());
    drop(txn); // Should NOT call abort again
}
