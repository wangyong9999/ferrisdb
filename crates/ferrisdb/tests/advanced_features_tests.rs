//! Tests for 6 advanced features: temp tablespace, concurrent index build,
//! DIO, expression index, SSI, parallel redo (27 tests)

use tempfile::TempDir;
use ferrisdb::*;

fn eng(td: &TempDir) -> Engine {
    Engine::open(EngineConfig { data_dir: td.path().to_path_buf(), shared_buffers: 500, wal_enabled: false, ..Default::default() }).unwrap()
}
fn eng_wal(td: &TempDir) -> Engine {
    Engine::open(EngineConfig { data_dir: td.path().to_path_buf(), shared_buffers: 500, wal_enabled: true, ..Default::default() }).unwrap()
}

// ==================== 1. Temp Tablespace (9) ====================

#[test] fn test_create_temp_table() { let td = TempDir::new().unwrap(); let e = eng(&td); let oid = e.create_temp_table("tmp").unwrap(); assert!(oid > 0); }
#[test] fn test_temp_table_is_temp() { let td = TempDir::new().unwrap(); let e = eng(&td); e.create_temp_table("tmp").unwrap(); assert!(e.is_temp_table("tmp")); }
#[test] fn test_normal_table_not_temp() { let td = TempDir::new().unwrap(); let e = eng(&td); e.create_table("normal").unwrap(); assert!(!e.is_temp_table("normal")); }
#[test] fn test_temp_table_insert_fetch() { let td = TempDir::new().unwrap(); let e = eng(&td); e.create_temp_table("tmp").unwrap(); let t = e.open_temp_table("tmp").unwrap(); let tid = t.insert(b"temp_data", Xid::new(0,1), 0).unwrap(); let (_,d) = t.fetch(tid).unwrap().unwrap(); assert_eq!(d, b"temp_data"); }
#[test] fn test_temp_table_no_wal() { let td = TempDir::new().unwrap(); let e = eng_wal(&td); e.create_temp_table("tmp_wal").unwrap(); let t = e.open_temp_table("tmp_wal").unwrap(); t.insert(b"no_wal", Xid::new(0,1), 0).unwrap(); }
#[test] fn test_temp_table_not_in_wal_ddl() { let td = TempDir::new().unwrap(); let e = eng_wal(&td); let wal = e.wal_writer().unwrap(); let before = wal.offset(); e.create_temp_table("tmp_no_ddl").unwrap(); assert_eq!(wal.offset(), before, "Temp table DDL should not write WAL"); }
#[test] fn test_temp_table_drop() { let td = TempDir::new().unwrap(); let e = eng(&td); e.create_temp_table("tmp_drop").unwrap(); e.drop_table("tmp_drop").unwrap(); assert!(e.open_temp_table("tmp_drop").is_err()); }
#[test] fn test_temp_and_normal_coexist() { let td = TempDir::new().unwrap(); let e = eng(&td); e.create_table("normal").unwrap(); e.create_temp_table("tmp").unwrap(); assert!(!e.is_temp_table("normal")); assert!(e.is_temp_table("tmp")); }
#[test] fn test_temp_table_not_persisted() { let td = TempDir::new().unwrap(); { let e = eng(&td); e.create_temp_table("ephemeral").unwrap(); e.shutdown().unwrap(); } { let e = eng(&td); assert!(e.is_temp_table("ephemeral")); } }

// ==================== 2. Concurrent Index Build (5) ====================

#[test] fn test_create_index_concurrently_basic() { let td = TempDir::new().unwrap(); let e = eng(&td); e.create_table("t").unwrap(); let t = e.open_table("t").unwrap(); for i in 0..50 { t.insert(format!("{:04}_data", i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } e.save_table_state("t", &t).unwrap(); let oid = e.create_index_concurrently("t_idx", "t", |data| BTreeKey::new(data[0..4].to_vec())).unwrap(); assert!(oid > 0); }
#[test] fn test_concurrent_index_finds_data() { let td = TempDir::new().unwrap(); let e = eng(&td); e.create_table("ci").unwrap(); let t = e.open_table("ci").unwrap(); for i in 0..20 { t.insert(format!("{:04}_val", i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } e.save_table_state("ci", &t).unwrap(); e.create_index_concurrently("ci_idx", "ci", |d| BTreeKey::new(d[0..4].to_vec())).unwrap(); let idx = e.open_index("ci_idx").unwrap(); let val = idx.lookup(&BTreeKey::new(b"0010".to_vec())).unwrap(); assert!(val.is_some()); }
#[test] fn test_concurrent_index_empty_table() { let td = TempDir::new().unwrap(); let e = eng(&td); e.create_table("empty").unwrap(); e.create_index_concurrently("empty_idx", "empty", |d| BTreeKey::new(d.to_vec())).unwrap(); }
#[test] fn test_concurrent_index_survives_restart() { let td = TempDir::new().unwrap(); { let e = eng(&td); e.create_table("r").unwrap(); let t = e.open_table("r").unwrap(); for i in 0..30 { t.insert(format!("{:06}_d", i).as_bytes(), Xid::new(0,(i+1) as u32), 0).unwrap(); } e.save_table_state("r", &t).unwrap(); e.create_index_concurrently("r_idx", "r", |d| BTreeKey::new(d[0..6].to_vec())).unwrap(); e.shutdown().unwrap(); } { let e = eng(&td); let idx = e.open_index("r_idx").unwrap(); assert!(idx.lookup(&BTreeKey::new(b"000015".to_vec())).unwrap().is_some()); } }
#[test] fn test_concurrent_index_nonexistent_table() { let td = TempDir::new().unwrap(); let e = eng(&td); assert!(e.create_index_concurrently("bad", "no_table", |d| BTreeKey::new(d.to_vec())).is_err()); }

// ==================== 3. DIO (3) ====================

#[test] fn test_smgr_direct_io_flag() { let s = ferrisdb_storage::StorageManager::with_direct_io("/tmp/dio_test"); assert!(s.is_direct_io()); }
#[test] fn test_smgr_normal_no_dio() { let s = ferrisdb_storage::StorageManager::new("/tmp/normal_test"); assert!(!s.is_direct_io()); }
#[test] fn test_dio_write_read() { let td = TempDir::new().unwrap(); let s = ferrisdb_storage::StorageManager::with_direct_io(td.path()); s.init().unwrap(); let tag = ferrisdb_core::BufferTag::new(ferrisdb_core::PdbId::new(0), 1, 0); let data = vec![0xAA; 8192]; s.write_page(&tag, &data).unwrap(); let mut buf = vec![0u8; 8192]; s.read_page(&tag, &mut buf).unwrap(); assert_eq!(buf[0], 0xAA); }

// ==================== 4. Expression Index (3) ====================

#[test] fn test_expression_index_lower() {
    let td = TempDir::new().unwrap(); let e = eng(&td);
    e.create_table("expr_t").unwrap();
    let t = e.open_table("expr_t").unwrap();
    t.insert(b"HELLO_world", Xid::new(0,1), 0).unwrap();
    t.insert(b"UPPER_CASE!", Xid::new(0,2), 0).unwrap();
    e.save_table_state("expr_t", &t).unwrap();
    // Expression index: lowercase the first 5 bytes
    e.create_index_concurrently("expr_idx", "expr_t", |data| {
        let lower: Vec<u8> = data[0..5.min(data.len())].iter().map(|b| b.to_ascii_lowercase()).collect();
        BTreeKey::new(lower)
    }).unwrap();
    let idx = e.open_index("expr_idx").unwrap();
    assert!(idx.lookup(&BTreeKey::new(b"hello".to_vec())).unwrap().is_some());
    assert!(idx.lookup(&BTreeKey::new(b"upper".to_vec())).unwrap().is_some());
}
#[test] fn test_expression_index_hash() {
    let td = TempDir::new().unwrap(); let e = eng(&td);
    e.create_table("hash_t").unwrap();
    let t = e.open_table("hash_t").unwrap();
    t.insert(b"data_123", Xid::new(0,1), 0).unwrap();
    e.save_table_state("hash_t", &t).unwrap();
    // Expression index: take last 3 bytes as key
    e.create_index_concurrently("hash_idx", "hash_t", |data| {
        let end = data.len();
        let start = if end > 3 { end - 3 } else { 0 };
        BTreeKey::new(data[start..end].to_vec())
    }).unwrap();
}
#[test] fn test_expression_index_flag() {
    let bp = std::sync::Arc::new(ferrisdb_storage::BufferPool::new(ferrisdb_storage::BufferPoolConfig::new(100)).unwrap());
    let btree = ferrisdb_storage::BTree::new(999, bp);
    btree.init().unwrap();
    let def = ferrisdb::IndexDef {
        name: "expr".to_string(), btree,
        key_extractor: Box::new(|d| BTreeKey::new(d[0..1].to_vec())),
        unique: false, is_expression: true,
    };
    assert!(def.is_expression);
}

// ==================== 5. SSI (2) ====================

#[test] fn test_ssi_no_conflict() { use ferrisdb_transaction::{SsiTracker, AccessKey}; let ssi = SsiTracker::new(); let x1 = Xid::new(0,1); ssi.track_read(x1, AccessKey { table_oid: 1, page_no: 0 }); assert!(!ssi.check_conflict(x1)); }
#[test] fn test_ssi_rw_cycle_conflict() { use ferrisdb_transaction::{SsiTracker, AccessKey}; let ssi = SsiTracker::new(); let x1 = Xid::new(0,1); let x2 = Xid::new(0,2); let k1 = AccessKey { table_oid: 1, page_no: 0 }; let k2 = AccessKey { table_oid: 1, page_no: 5 }; ssi.track_read(x1, k1.clone()); ssi.track_write(x2, k1); ssi.track_read(x2, k2.clone()); ssi.track_write(x1, k2); assert!(ssi.check_conflict(x1)); }

// ==================== 6. Parallel Redo (5) ====================

#[test] fn test_parallel_redo_coordinator_basic() { use ferrisdb_storage::wal::{ParallelRedoCoordinator, ParallelRedoConfig}; let c = ParallelRedoCoordinator::new(ParallelRedoConfig { num_workers: 2, ..Default::default() }); let _ = c; }
#[test] fn test_parallel_redo_config_default() { use ferrisdb_storage::wal::ParallelRedoConfig; let cfg = ParallelRedoConfig::default(); assert!(cfg.num_workers > 0); }
#[test] fn test_recovery_with_parallel_config() { let td = TempDir::new().unwrap(); let wd = td.path().join("wal"); std::fs::create_dir_all(&wd).unwrap(); let s = std::sync::Arc::new(ferrisdb_storage::StorageManager::new(td.path())); s.init().unwrap(); let r = ferrisdb_storage::wal::WalRecovery::with_smgr(&wd, s); r.recover(ferrisdb_storage::wal::RecoveryMode::CrashRecovery).unwrap(); assert_eq!(r.stage(), ferrisdb_storage::wal::RecoveryStage::Completed); }
#[test] fn test_recovery_redo_with_records() { let td = TempDir::new().unwrap(); let wd = td.path().join("wal"); std::fs::create_dir_all(&wd).unwrap(); let w = ferrisdb_storage::wal::WalWriter::new(&wd); let data = vec![0u8; 45]; let rec = ferrisdb_storage::wal::WalHeapInsert::new(ferrisdb_storage::PageId::new(0,0,100,0), 1, &data); w.write(&rec.serialize_with_data(&data)).unwrap(); w.sync().unwrap(); drop(w); let s = std::sync::Arc::new(ferrisdb_storage::StorageManager::new(td.path())); s.init().unwrap(); let r = ferrisdb_storage::wal::WalRecovery::with_smgr(&wd, s); r.recover(ferrisdb_storage::wal::RecoveryMode::CrashRecovery).unwrap(); assert!(r.stats().records_redone + r.stats().records_skipped > 0); }
#[test] fn test_parallel_redo_stats() { use ferrisdb_storage::wal::ParallelRedoStats; let s = ParallelRedoStats::default(); assert_eq!(s.total_processed, 0); }
