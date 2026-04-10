//! Step 4 验证：DataFusion + FerrisDB 集成测试
//!
//! 测试 SQL 查询通过 DataFusion 引擎执行，数据存储在 FerrisDB 中。

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb::{Engine, EngineConfig};
use ferrisdb::{ColumnDef, DataType};
use ferrisdb::{encode_row, Value};
use ferrisdb_sql::session::create_session;

/// 创建测试 Engine 并插入数据
fn setup_engine_with_data(td: &TempDir) -> Arc<Engine> {
    let engine = Engine::open(EngineConfig {
        data_dir: td.path().to_path_buf(),
        shared_buffers: 500,
        wal_enabled: false,
        ..Default::default()
    }).unwrap();

    // CREATE TABLE users (id INT, name TEXT, score FLOAT)
    let columns = vec![
        ColumnDef::new("id", DataType::Int32, false, 0),
        ColumnDef::new("name", DataType::Text, true, 1),
        ColumnDef::new("score", DataType::Float64, true, 2),
    ];
    engine.create_table("users").unwrap();
    // 更新 catalog 加 Schema
    let meta = engine.catalog().lookup_by_name("users").unwrap();
    // 用 create_table_with_schema 重新创建（先 drop 再 create）
    engine.drop_table("users").unwrap();
    engine.catalog().create_table_with_schema("users", 0, columns.clone()).unwrap();

    // 用 Engine 的完整流程创建表和插入数据
    let table = engine.open_table("users").unwrap();
    let col_types: Vec<DataType> = columns.iter().map(|c| c.data_type).collect();

    // 使用 Engine::begin 获取事务
    let mut txn = engine.begin().unwrap();
    let xid = txn.xid();

    let rows = vec![
        vec![Value::Int32(1), Value::Text("Alice".into()), Value::Float64(95.5)],
        vec![Value::Int32(2), Value::Text("Bob".into()), Value::Float64(87.0)],
        vec![Value::Int32(3), Value::Text("Charlie".into()), Value::Null],
    ];

    for (i, row) in rows.iter().enumerate() {
        let encoded = encode_row(&col_types, row);
        table.insert(&encoded, xid, i as u32).unwrap();
    }
    txn.commit().unwrap();

    // 保存表状态
    engine.save_table_state("users", &table).unwrap();

    Arc::new(engine)
}

#[tokio::test]
async fn test_select_all() {
    let td = TempDir::new().unwrap();
    let engine = setup_engine_with_data(&td);
    let ctx = create_session(engine.clone());

    let df = ctx.sql("SELECT * FROM ferrisdb.public.users").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total_rows >= 3, "Should have at least 3 rows, got {}", total_rows);

    engine.shutdown().unwrap();
}

#[tokio::test]
async fn test_select_with_projection() {
    let td = TempDir::new().unwrap();
    let engine = setup_engine_with_data(&td);
    let ctx = create_session(engine.clone());

    let df = ctx.sql("SELECT name, score FROM ferrisdb.public.users").await.unwrap();
    let batches = df.collect().await.unwrap();

    assert!(!batches.is_empty());
    // 只有 2 列
    assert_eq!(batches[0].num_columns(), 2);

    engine.shutdown().unwrap();
}

#[tokio::test]
async fn test_table_not_found() {
    let td = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig {
        data_dir: td.path().to_path_buf(),
        shared_buffers: 100,
        wal_enabled: false,
        ..Default::default()
    }).unwrap());

    let ctx = create_session(engine.clone());
    let result = ctx.sql("SELECT * FROM ferrisdb.public.nonexistent").await;
    assert!(result.is_err());

    engine.shutdown().unwrap();
}

#[tokio::test]
async fn test_empty_table() {
    let td = TempDir::new().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig {
        data_dir: td.path().to_path_buf(),
        shared_buffers: 100,
        wal_enabled: false,
        ..Default::default()
    }).unwrap());

    engine.catalog().create_table_with_schema("empty_t", 0, vec![
        ColumnDef::new("x", DataType::Int32, false, 0),
    ]).unwrap();

    let ctx = create_session(engine.clone());
    let df = ctx.sql("SELECT * FROM ferrisdb.public.empty_t").await.unwrap();
    let batches = df.collect().await.unwrap();

    let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total_rows, 0);

    engine.shutdown().unwrap();
}
