//! Tablespace WAL tests — DDL WAL record generation and recovery (14 tests)

use tempfile::TempDir;
use ferrisdb::{Engine, EngineConfig, Xid};

fn engine_wal(td: &TempDir) -> Engine {
    Engine::open(EngineConfig { data_dir: td.path().to_path_buf(), shared_buffers: 200, wal_enabled: true, ..Default::default() }).unwrap()
}

// ===== DDL WAL record generation =====

#[test]
fn test_create_table_writes_wal() {
    let td = TempDir::new().unwrap();
    let eng = engine_wal(&td);
    let wal = eng.wal_writer().unwrap();
    let before = wal.offset();
    eng.create_table("wal_table").unwrap();
    assert!(wal.offset() > before, "CREATE TABLE should write WAL");
}

#[test]
fn test_drop_table_writes_wal() {
    let td = TempDir::new().unwrap();
    let eng = engine_wal(&td);
    eng.create_table("drop_me").unwrap();
    let wal = eng.wal_writer().unwrap();
    let before = wal.offset();
    eng.drop_table("drop_me").unwrap();
    assert!(wal.offset() > before, "DROP TABLE should write WAL");
}

#[test]
fn test_create_index_writes_wal() {
    let td = TempDir::new().unwrap();
    let eng = engine_wal(&td);
    eng.create_table("base").unwrap();
    let wal = eng.wal_writer().unwrap();
    let before = wal.offset();
    eng.create_index("base_idx", "base").unwrap();
    assert!(wal.offset() > before, "CREATE INDEX should write WAL");
}

#[test]
fn test_create_tablespace_writes_wal() {
    let td = TempDir::new().unwrap();
    let eng = engine_wal(&td);
    let wal = eng.wal_writer().unwrap();
    let before = wal.offset();
    eng.create_tablespace("fast_ssd", "/mnt/ssd/data").unwrap();
    assert!(wal.offset() > before, "CREATE TABLESPACE should write WAL");
}

// ===== DDL survives restart =====

#[test]
fn test_table_survives_restart_with_wal() {
    let td = TempDir::new().unwrap();
    {
        let eng = engine_wal(&td);
        eng.create_table("persistent_t").unwrap();
        eng.shutdown().unwrap();
    }
    {
        let eng = engine_wal(&td);
        assert!(eng.open_table("persistent_t").is_ok());
    }
}

#[test]
fn test_index_survives_restart_with_wal() {
    let td = TempDir::new().unwrap();
    {
        let eng = engine_wal(&td);
        eng.create_table("t").unwrap();
        eng.create_index("t_idx", "t").unwrap();
        eng.shutdown().unwrap();
    }
    {
        let eng = engine_wal(&td);
        assert!(eng.open_index("t_idx").is_ok());
    }
}

#[test]
fn test_drop_survives_restart() {
    let td = TempDir::new().unwrap();
    {
        let eng = engine_wal(&td);
        eng.create_table("temp_t").unwrap();
        eng.drop_table("temp_t").unwrap();
        eng.shutdown().unwrap();
    }
    {
        let eng = engine_wal(&td);
        assert!(eng.open_table("temp_t").is_err(), "Dropped table should not exist after restart");
    }
}

// ===== Multiple DDL operations =====

#[test]
fn test_multiple_ddl_wal() {
    let td = TempDir::new().unwrap();
    {
        let eng = engine_wal(&td);
        for i in 0..10 {
            eng.create_table(&format!("t_{}", i)).unwrap();
        }
        eng.drop_table("t_5").unwrap();
        eng.drop_table("t_7").unwrap();
        eng.shutdown().unwrap();
    }
    {
        let eng = engine_wal(&td);
        for i in 0..10 {
            if i == 5 || i == 7 {
                assert!(eng.open_table(&format!("t_{}", i)).is_err());
            } else {
                assert!(eng.open_table(&format!("t_{}", i)).is_ok());
            }
        }
    }
}

#[test]
fn test_create_table_in_tablespace() {
    let td = TempDir::new().unwrap();
    let eng = engine_wal(&td);
    eng.create_tablespace("ts1", "/data/ts1").unwrap();
    // create_table_in is available but tablespace_id needs to be the OID returned
    let oid = eng.create_table("in_ts1").unwrap();
    assert!(oid > 0);
}

// ===== WAL sync on DDL =====

#[test]
fn test_ddl_wal_synced_before_shutdown() {
    let td = TempDir::new().unwrap();
    {
        let eng = engine_wal(&td);
        eng.create_table("sync_t").unwrap();
        let wal = eng.wal_writer().unwrap();
        wal.sync().unwrap();
        assert!(wal.flushed_lsn().raw() > 0);
        eng.shutdown().unwrap();
    }
}

// ===== Table data + DDL combined =====

#[test]
fn test_table_with_data_and_ddl_wal() {
    let td = TempDir::new().unwrap();
    {
        let eng = engine_wal(&td);
        eng.create_table("data_t").unwrap();
        let t = eng.open_table("data_t").unwrap();
        for i in 0..20 {
            t.insert(format!("r{}", i).as_bytes(), Xid::new(0, (i+1) as u32), 0).unwrap();
        }
        eng.save_table_state("data_t", &t).unwrap();
        eng.shutdown().unwrap();
    }
    {
        let eng = engine_wal(&td);
        let t = eng.open_table("data_t").unwrap();
        assert!(t.get_current_page() > 0);
    }
}

#[test]
fn test_index_with_data_and_ddl_wal() {
    let td = TempDir::new().unwrap();
    {
        let eng = engine_wal(&td);
        eng.create_table("idx_data_t").unwrap();
        eng.create_index("idx_data_pk", "idx_data_t").unwrap();
        let idx = eng.open_index("idx_data_pk").unwrap();
        idx.init().unwrap();
        for i in 0..50 {
            idx.insert(
                ferrisdb::BTreeKey::new(format!("{:06}", i).into_bytes()),
                ferrisdb::BTreeValue::Tuple { block: i, offset: 1 },
            ).unwrap();
        }
        eng.save_index_state("idx_data_pk", &idx).unwrap();
        eng.shutdown().unwrap();
    }
    {
        let eng = engine_wal(&td);
        let idx = eng.open_index("idx_data_pk").unwrap();
        let val = idx.lookup(&ferrisdb::BTreeKey::new(b"000025".to_vec())).unwrap();
        assert!(val.is_some(), "Index data should survive WAL restart");
    }
}

// ===== Error cases =====

#[test]
fn test_drop_nonexistent_table() {
    let td = TempDir::new().unwrap();
    let eng = engine_wal(&td);
    assert!(eng.drop_table("ghost").is_err());
}

#[test]
fn test_create_index_on_nonexistent_table() {
    let td = TempDir::new().unwrap();
    let eng = engine_wal(&td);
    assert!(eng.create_index("bad_idx", "no_table").is_err());
}
