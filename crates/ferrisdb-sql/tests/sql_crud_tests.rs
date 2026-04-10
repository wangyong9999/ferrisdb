//! Step 6-7: 完整 SQL CRUD + 事务集成测试
//!
//! 通过 FerrisQueryHandler 直接调用验证 SQL 处理，无需 TCP 连接。

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb::{Engine, EngineConfig};
use ferrisdb_storage::catalog::{ColumnDef, DataType};
use ferrisdb_storage::row_codec::{encode_row, Value};
use ferrisdb_sql::session::create_session;

fn make_engine(td: &TempDir) -> Arc<Engine> {
    Arc::new(Engine::open(EngineConfig {
        data_dir: td.path().to_path_buf(),
        shared_buffers: 500,
        wal_enabled: false,
        ..Default::default()
    }).unwrap())
}

fn create_users_table(engine: &Engine) {
    engine.create_table("users").unwrap();
    engine.drop_table("users").unwrap();
    engine.catalog().create_table_with_schema("users", 0, vec![
        ColumnDef::new("id", DataType::Int32, false, 0),
        ColumnDef::new("name", DataType::Text, true, 1),
        ColumnDef::new("age", DataType::Int32, true, 2),
    ]).unwrap();
}

fn insert_users(engine: &Engine, rows: &[(i32, &str, i32)]) {
    let table = engine.open_table("users").unwrap();
    let types = vec![DataType::Int32, DataType::Text, DataType::Int32];
    let mut txn = engine.begin().unwrap();
    for (i, (id, name, age)) in rows.iter().enumerate() {
        let vals = vec![Value::Int32(*id), Value::Text(name.to_string()), Value::Int32(*age)];
        let encoded = encode_row(&types, &vals);
        table.insert(&encoded, txn.xid(), i as u32).unwrap();
    }
    txn.commit().unwrap();
    // 保存表状态（current_page 等），让后续 open_table 能看到数据
    engine.save_table_state("users", &table).unwrap();
}

// ============================================================
// SELECT 测试
// ============================================================

#[tokio::test]
async fn test_select_all_rows() {
    let td = TempDir::new().unwrap();
    let engine = make_engine(&td);
    create_users_table(&engine);
    insert_users(&engine, &[(1, "Alice", 30), (2, "Bob", 25), (3, "Charlie", 35)]);

    let ctx = create_session(engine.clone());
    let df = ctx.sql("SELECT * FROM ferrisdb.public.users").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
    engine.shutdown().unwrap();
}

#[tokio::test]
async fn test_select_with_count() {
    let td = TempDir::new().unwrap();
    let engine = make_engine(&td);
    create_users_table(&engine);
    insert_users(&engine, &[(1, "A", 20), (2, "B", 30)]);

    let ctx = create_session(engine.clone());
    let df = ctx.sql("SELECT COUNT(*) FROM ferrisdb.public.users").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert!(!batches.is_empty());
    assert_eq!(batches[0].num_rows(), 1);
    engine.shutdown().unwrap();
}

#[tokio::test]
async fn test_select_column_projection() {
    let td = TempDir::new().unwrap();
    let engine = make_engine(&td);
    create_users_table(&engine);
    insert_users(&engine, &[(1, "Alice", 30)]);

    let ctx = create_session(engine.clone());
    let df = ctx.sql("SELECT name FROM ferrisdb.public.users").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_columns(), 1);
    engine.shutdown().unwrap();
}

// ============================================================
// INSERT 测试
// ============================================================

#[tokio::test]
async fn test_insert_and_read_back() {
    let td = TempDir::new().unwrap();
    let engine = make_engine(&td);
    create_users_table(&engine);
    insert_users(&engine, &[(10, "Test", 99)]);

    let ctx = create_session(engine.clone());
    let df = ctx.sql("SELECT * FROM ferrisdb.public.users").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert!(total >= 1);
    engine.shutdown().unwrap();
}

// ============================================================
// 空表 + 表不存在
// ============================================================

#[tokio::test]
async fn test_select_empty_table() {
    let td = TempDir::new().unwrap();
    let engine = make_engine(&td);
    create_users_table(&engine);

    let ctx = create_session(engine.clone());
    let df = ctx.sql("SELECT * FROM ferrisdb.public.users").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 0);
    engine.shutdown().unwrap();
}

#[tokio::test]
async fn test_select_nonexistent_table() {
    let td = TempDir::new().unwrap();
    let engine = make_engine(&td);
    let ctx = create_session(engine.clone());
    let result = ctx.sql("SELECT * FROM ferrisdb.public.ghost").await;
    assert!(result.is_err());
    engine.shutdown().unwrap();
}

// ============================================================
// 多类型测试
// ============================================================

#[tokio::test]
async fn test_all_data_types() {
    let td = TempDir::new().unwrap();
    let engine = make_engine(&td);

    engine.create_table("all_types").unwrap();
    engine.drop_table("all_types").unwrap();
    engine.catalog().create_table_with_schema("all_types", 0, vec![
        ColumnDef::new("i32_col", DataType::Int32, true, 0),
        ColumnDef::new("i64_col", DataType::Int64, true, 1),
        ColumnDef::new("f64_col", DataType::Float64, true, 2),
        ColumnDef::new("text_col", DataType::Text, true, 3),
        ColumnDef::new("bool_col", DataType::Boolean, true, 4),
    ]).unwrap();

    let table = engine.open_table("all_types").unwrap();
    let types = vec![DataType::Int32, DataType::Int64, DataType::Float64, DataType::Text, DataType::Boolean];
    let vals = vec![
        Value::Int32(42), Value::Int64(i64::MAX), Value::Float64(3.14),
        Value::Text("hello".into()), Value::Boolean(true),
    ];
    let mut txn = engine.begin().unwrap();
    table.insert(&encode_row(&types, &vals), txn.xid(), 0).unwrap();
    txn.commit().unwrap();
    engine.save_table_state("all_types", &table).unwrap();

    let ctx = create_session(engine.clone());
    let df = ctx.sql("SELECT * FROM ferrisdb.public.all_types").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 5);
    engine.shutdown().unwrap();
}

// ============================================================
// 多行插入 + 大量数据
// ============================================================

#[tokio::test]
async fn test_many_rows() {
    let td = TempDir::new().unwrap();
    let engine = make_engine(&td);
    create_users_table(&engine);

    let rows: Vec<(i32, &str, i32)> = (0..100).map(|i| (i, "user", 20 + i % 50)).collect();
    insert_users(&engine, &rows);

    let ctx = create_session(engine.clone());
    let df = ctx.sql("SELECT * FROM ferrisdb.public.users").await.unwrap();
    let batches = df.collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 100);
    engine.shutdown().unwrap();
}

// ============================================================
// NULL 值
// ============================================================

#[tokio::test]
async fn test_null_values() {
    let td = TempDir::new().unwrap();
    let engine = make_engine(&td);
    create_users_table(&engine);

    let table = engine.open_table("users").unwrap();
    let types = vec![DataType::Int32, DataType::Text, DataType::Int32];
    let vals = vec![Value::Int32(1), Value::Null, Value::Null];
    let mut txn = engine.begin().unwrap();
    table.insert(&encode_row(&types, &vals), txn.xid(), 0).unwrap();
    txn.commit().unwrap();
    engine.save_table_state("users", &table).unwrap();

    let ctx = create_session(engine.clone());
    let df = ctx.sql("SELECT * FROM ferrisdb.public.users").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    // name 和 age 应该是 null
    assert!(batches[0].column(1).is_null(0));
    assert!(batches[0].column(2).is_null(0));
    engine.shutdown().unwrap();
}
