//! Engine lifecycle 覆盖

use tempfile::TempDir;
use ferrisdb::{Engine, EngineConfig};

fn eng(td: &TempDir, wal: bool) -> Engine {
    Engine::open(EngineConfig {
        data_dir: td.path().to_path_buf(),
        shared_buffers: 100,
        wal_enabled: wal,
        ..Default::default()
    }).unwrap()
}

#[test]
fn test_engine_open_close_cycle() {
    let td = TempDir::new().unwrap();
    { let e = eng(&td, false); e.shutdown().unwrap(); }
    { let e = eng(&td, false); e.shutdown().unwrap(); }
    { let e = eng(&td, false); e.shutdown().unwrap(); }
}

#[test]
fn test_engine_with_wal() {
    let td = TempDir::new().unwrap();
    let e = eng(&td, true);
    e.create_table("t1").unwrap();
    let t = e.open_table("t1").unwrap();
    t.insert(b"wal_data", ferrisdb::Xid::new(0, 1), 0).unwrap();
    e.save_table_state("t1", &t).unwrap();
    e.shutdown().unwrap();
}

#[test]
fn test_engine_stats() {
    let td = TempDir::new().unwrap();
    let e = eng(&td, true);
    let s = e.stats();
    assert!(!s.is_shutdown);
    assert_eq!(s.active_transactions, 0);
    e.shutdown().unwrap();
    let s2 = e.stats();
    assert!(s2.is_shutdown);
}

#[test]
fn test_engine_create_drop_table() {
    let td = TempDir::new().unwrap();
    let e = eng(&td, false);
    e.create_table("drop_me").unwrap();
    assert!(e.open_table("drop_me").is_ok());
    e.drop_table("drop_me").unwrap();
    e.shutdown().unwrap();
}

#[test]
fn test_engine_create_index() {
    let td = TempDir::new().unwrap();
    let e = eng(&td, false);
    e.create_table("idx_table").unwrap();
    e.create_index("idx_pk", "idx_table").unwrap();
    let idx = e.open_index("idx_pk").unwrap();
    idx.insert(
        ferrisdb::BTreeKey::new(b"k1".to_vec()),
        ferrisdb::BTreeValue::Tuple { block: 1, offset: 0 },
    ).unwrap();
    e.save_index_state("idx_pk", &idx).unwrap();
    e.shutdown().unwrap();
}

#[test]
fn test_engine_register_table_autovacuum() {
    let td = TempDir::new().unwrap();
    let e = eng(&td, false);
    e.create_table("vac_t").unwrap();
    let t = std::sync::Arc::new(e.open_table("vac_t").unwrap());
    e.register_table(t);
    // autovacuum 后台线程应该能处理
    std::thread::sleep(std::time::Duration::from_millis(50));
    e.shutdown().unwrap();
}

#[test]
fn test_engine_begin_after_shutdown() {
    let td = TempDir::new().unwrap();
    let e = eng(&td, false);
    e.shutdown().unwrap();
    let r = e.begin();
    assert!(r.is_err(), "begin after shutdown should fail");
}

#[test]
fn test_engine_lockfile_prevents_double_open() {
    let td = TempDir::new().unwrap();
    let _e1 = eng(&td, false);
    let r = Engine::open(EngineConfig {
        data_dir: td.path().to_path_buf(),
        shared_buffers: 100,
        wal_enabled: false,
        ..Default::default()
    });
    assert!(r.is_err(), "Double open should fail");
}

#[test]
fn test_engine_tablespace() {
    let td = TempDir::new().unwrap();
    let e = eng(&td, false);
    let ts_oid = e.create_tablespace("myts", "/tmp/ts").unwrap();
    assert!(ts_oid > 0);
    let t_oid = e.create_table_in("ts_table", ts_oid).unwrap();
    assert!(t_oid > 0);
    e.shutdown().unwrap();
}

#[test]
fn test_engine_temp_table() {
    let td = TempDir::new().unwrap();
    let e = eng(&td, false);
    let oid = e.create_temp_table("tmp_t").unwrap();
    assert!(oid > 0);
    e.shutdown().unwrap();
}
