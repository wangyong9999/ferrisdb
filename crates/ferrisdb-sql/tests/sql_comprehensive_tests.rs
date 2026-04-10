//! SQL 能力系统性测试 — 39 个测试点覆盖 DDL/DML/WHERE/JOIN/聚合/事务/错误处理
//!
//! 通过 DataFusion SessionContext 直接执行，验证 FerrisDB 存储后端的完整 SQL 能力。

use std::sync::Arc;
use tempfile::TempDir;
use ferrisdb::{Engine, EngineConfig};
use ferrisdb::{ColumnDef, DataType};
use ferrisdb::{encode_row, Value};
use ferrisdb_sql::session::create_session;
use arrow::array::*;

// ============================================================
// Helper
// ============================================================

fn engine(td: &TempDir) -> Arc<Engine> {
    Arc::new(Engine::open(EngineConfig {
        data_dir: td.path().to_path_buf(),
        shared_buffers: 500,
        wal_enabled: false,
        ..Default::default()
    }).unwrap())
}

fn create_table(eng: &Engine, name: &str, cols: Vec<ColumnDef>) {
    eng.create_table(name).unwrap();
    eng.drop_table(name).unwrap();
    eng.catalog().create_table_with_schema(name, 0, cols).unwrap();
}

fn insert_rows(eng: &Engine, name: &str, types: &[DataType], rows: &[Vec<Value>]) {
    let table = eng.open_table(name).unwrap();
    let mut txn = eng.begin().unwrap();
    for (i, row) in rows.iter().enumerate() {
        table.insert(&encode_row(types, row), txn.xid(), i as u32).unwrap();
    }
    txn.commit().unwrap();
    eng.save_table_state(name, &table).unwrap();
}

fn users_schema() -> Vec<ColumnDef> {
    vec![
        ColumnDef::new("id", DataType::Int32, false, 0),
        ColumnDef::new("name", DataType::Text, true, 1),
    ]
}

fn users_types() -> Vec<DataType> { vec![DataType::Int32, DataType::Text] }

fn fqn(table: &str) -> String { format!("ferrisdb.public.{}", table) }

async fn query_count(eng: &Arc<Engine>, sql: &str) -> usize {
    let ctx = create_session(eng.clone());
    let df = ctx.sql(sql).await.unwrap();
    let batches = df.collect().await.unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

// ============================================================
// 1. DDL
// ============================================================

#[tokio::test]
async fn test_ddl_create_table() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t1", users_schema());
    assert!(eng.catalog().lookup_by_name("t1").is_some());
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_ddl_create_all_types() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t2", vec![
        ColumnDef::new("a", DataType::Int32, true, 0),
        ColumnDef::new("b", DataType::Int64, true, 1),
        ColumnDef::new("c", DataType::Float64, true, 2),
        ColumnDef::new("d", DataType::Text, true, 3),
        ColumnDef::new("e", DataType::Boolean, true, 4),
    ]);
    let meta = eng.catalog().lookup_by_name("t2").unwrap();
    assert_eq!(meta.columns.len(), 5);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_ddl_drop_table() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "drop_me", users_schema());
    eng.drop_table("drop_me").unwrap();
    assert!(eng.catalog().lookup_by_name("drop_me").is_none());
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_ddl_duplicate_table_rejected() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "dup", users_schema());
    let result = eng.catalog().create_table_with_schema("dup", 0, users_schema());
    assert!(result.is_err());
    eng.shutdown().unwrap();
}

// ============================================================
// 2. INSERT
// ============================================================

#[tokio::test]
async fn test_insert_single_row() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("hello".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {}", fqn("t"))).await, 1);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_insert_multiple_rows() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("b".into())],
        vec![Value::Int32(3), Value::Text("c".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {}", fqn("t"))).await, 3);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_insert_null_value() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Null],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT * FROM {}", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    assert!(batches[0].column(1).is_null(0));
    eng.shutdown().unwrap();
}

// ============================================================
// 3. SELECT 基础
// ============================================================

#[tokio::test]
async fn test_select_all() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Bob".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {}", fqn("t"))).await, 2);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_select_projection() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT name FROM {}", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    assert_eq!(batches[0].num_columns(), 1);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_select_count() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("b".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT COUNT(*) FROM {}", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    let count = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(count, 2);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_select_nonexistent_table_error() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    let ctx = create_session(eng.clone());
    let result = ctx.sql(&format!("SELECT * FROM {}", fqn("ghost"))).await;
    assert!(result.is_err());
    eng.shutdown().unwrap();
}

// ============================================================
// 4. WHERE 过滤 (DataFusion 在 Arrow batch 上过滤)
// ============================================================

#[tokio::test]
async fn test_where_equals() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Bob".into())],
        vec![Value::Int32(3), Value::Text("Charlie".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE id = 2", fqn("t"))).await, 1);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_where_range() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(5), Value::Text("b".into())],
        vec![Value::Int32(10), Value::Text("c".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE id > 3", fqn("t"))).await, 2);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_where_text() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Bob".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE name = 'Alice'", fqn("t"))).await, 1);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_where_and() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Bob".into())],
        vec![Value::Int32(3), Value::Text("Alice".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE id > 1 AND name = 'Alice'", fqn("t"))).await, 1);
    eng.shutdown().unwrap();
}

// ============================================================
// 7. ORDER BY + LIMIT
// ============================================================

#[tokio::test]
async fn test_order_by() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(3), Value::Text("c".into())],
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("b".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT id FROM {} ORDER BY id", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    let ids = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(ids.value(1), 2);
    assert_eq!(ids.value(2), 3);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_limit() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("b".into())],
        vec![Value::Int32(3), Value::Text("c".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} LIMIT 2", fqn("t"))).await, 2);
    eng.shutdown().unwrap();
}

// ============================================================
// 9. 聚合
// ============================================================

#[tokio::test]
async fn test_aggregates() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(10), Value::Text("a".into())],
        vec![Value::Int32(20), Value::Text("b".into())],
        vec![Value::Int32(30), Value::Text("c".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT SUM(id), AVG(id) FROM {}", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_group_by() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("a".into())],
        vec![Value::Int32(3), Value::Text("b".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT name, COUNT(*) FROM {} GROUP BY name", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2); // 两组: a, b
    eng.shutdown().unwrap();
}

// ============================================================
// 10. 子查询
// ============================================================

#[tokio::test]
async fn test_subquery_from() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT * FROM (SELECT id FROM {}) sub", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    assert_eq!(batches[0].num_columns(), 1);
    eng.shutdown().unwrap();
}

// ============================================================
// 12. 错误处理
// ============================================================

#[tokio::test]
async fn test_error_invalid_syntax() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    let ctx = create_session(eng.clone());
    let result = ctx.sql("INVALID SQL SYNTAX").await;
    assert!(result.is_err());
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_error_incomplete_sql() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    let ctx = create_session(eng.clone());
    let result = ctx.sql("SELECT * FROM").await;
    assert!(result.is_err());
    eng.shutdown().unwrap();
}

// ============================================================
// 大量数据
// ============================================================

#[tokio::test]
async fn test_bulk_insert_and_query() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "big", users_schema());
    let rows: Vec<Vec<Value>> = (0..500).map(|i| {
        vec![Value::Int32(i), Value::Text(format!("user_{}", i))]
    }).collect();
    insert_rows(&eng, "big", &users_types(), &rows);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {}", fqn("big"))).await, 500);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE id >= 400", fqn("big"))).await, 100);
    eng.shutdown().unwrap();
}

// ============================================================
// 空结果
// ============================================================

#[tokio::test]
async fn test_where_no_match() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE id = 999", fqn("t"))).await, 0);
    eng.shutdown().unwrap();
}

// ============================================================
// 5. WHERE 高级: OR / NOT / IN / LIKE / IS NULL
// ============================================================

#[tokio::test]
async fn test_where_or() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Bob".into())],
        vec![Value::Int32(3), Value::Text("Charlie".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE id = 1 OR id = 3", fqn("t"))).await, 2);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_where_not() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("b".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE NOT id = 1", fqn("t"))).await, 1);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_where_in() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("b".into())],
        vec![Value::Int32(3), Value::Text("c".into())],
        vec![Value::Int32(4), Value::Text("d".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE id IN (1, 3, 4)", fqn("t"))).await, 3);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_where_like() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Alfred".into())],
        vec![Value::Int32(3), Value::Text("Bob".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE name LIKE 'Al%'", fqn("t"))).await, 2);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_where_is_null() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Null],
        vec![Value::Int32(2), Value::Text("Bob".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE name IS NULL", fqn("t"))).await, 1);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE name IS NOT NULL", fqn("t"))).await, 1);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_where_between() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(5), Value::Text("b".into())],
        vec![Value::Int32(10), Value::Text("c".into())],
        vec![Value::Int32(15), Value::Text("d".into())],
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE id BETWEEN 5 AND 10", fqn("t"))).await, 2);
    eng.shutdown().unwrap();
}

// ============================================================
// 6. DISTINCT
// ============================================================

#[tokio::test]
async fn test_distinct() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("a".into())],
        vec![Value::Int32(3), Value::Text("b".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT DISTINCT name FROM {}", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2);
    eng.shutdown().unwrap();
}

// ============================================================
// 7b. ORDER BY DESC + OFFSET
// ============================================================

#[tokio::test]
async fn test_order_by_desc() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("b".into())],
        vec![Value::Int32(3), Value::Text("c".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT id FROM {} ORDER BY id DESC", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    let ids = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ids.value(0), 3);
    assert_eq!(ids.value(2), 1);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_limit_offset() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("b".into())],
        vec![Value::Int32(3), Value::Text("c".into())],
        vec![Value::Int32(4), Value::Text("d".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT id FROM {} ORDER BY id LIMIT 2 OFFSET 1", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    let ids = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ids.len(), 2);
    assert_eq!(ids.value(0), 2);
    assert_eq!(ids.value(1), 3);
    eng.shutdown().unwrap();
}

// ============================================================
// 8. JOIN (两表)
// ============================================================

fn orders_schema() -> Vec<ColumnDef> {
    vec![
        ColumnDef::new("order_id", DataType::Int32, false, 0),
        ColumnDef::new("user_id", DataType::Int32, false, 1),
        ColumnDef::new("amount", DataType::Int64, true, 2),
    ]
}

fn orders_types() -> Vec<DataType> { vec![DataType::Int32, DataType::Int32, DataType::Int64] }

#[tokio::test]
async fn test_inner_join() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "users", users_schema());
    create_table(&eng, "orders", orders_schema());
    insert_rows(&eng, "users", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Bob".into())],
    ]);
    insert_rows(&eng, "orders", &orders_types(), &[
        vec![Value::Int32(100), Value::Int32(1), Value::Int64(500)],
        vec![Value::Int32(101), Value::Int32(1), Value::Int64(300)],
        vec![Value::Int32(102), Value::Int32(3), Value::Int64(100)], // user_id=3 不存在
    ]);
    let sql = format!(
        "SELECT u.name, o.amount FROM {} u JOIN {} o ON u.id = o.user_id",
        fqn("users"), fqn("orders")
    );
    assert_eq!(query_count(&eng, &sql).await, 2); // 只有 user_id=1 匹配
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_left_join() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "users", users_schema());
    create_table(&eng, "orders", orders_schema());
    insert_rows(&eng, "users", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Bob".into())],
    ]);
    insert_rows(&eng, "orders", &orders_types(), &[
        vec![Value::Int32(100), Value::Int32(1), Value::Int64(500)],
    ]);
    let sql = format!(
        "SELECT u.name, o.amount FROM {} u LEFT JOIN {} o ON u.id = o.user_id ORDER BY u.id",
        fqn("users"), fqn("orders")
    );
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2); // Alice 有 order, Bob 的 amount 为 NULL
    // 验证 Bob 的 amount 为 NULL
    let amount_col = batches[0].column(1);
    assert!(amount_col.is_null(1)); // Bob 行
    eng.shutdown().unwrap();
}

// ============================================================
// 9b. 聚合 + HAVING
// ============================================================

#[tokio::test]
async fn test_having() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("a".into())],
        vec![Value::Int32(3), Value::Text("a".into())],
        vec![Value::Int32(4), Value::Text("b".into())],
    ]);
    let sql = format!("SELECT name, COUNT(*) as cnt FROM {} GROUP BY name HAVING COUNT(*) > 1", fqn("t"));
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 1); // 只有 a (count=3)
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_aggregate_min_max() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(10), Value::Text("a".into())],
        vec![Value::Int32(20), Value::Text("b".into())],
        vec![Value::Int32(30), Value::Text("c".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT MIN(id), MAX(id) FROM {}", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    let min_val = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap().value(0);
    let max_val = batches[0].column(1).as_any().downcast_ref::<Int32Array>().unwrap().value(0);
    assert_eq!(min_val, 10);
    assert_eq!(max_val, 30);
    eng.shutdown().unwrap();
}

// ============================================================
// 10b. 子查询高级: WHERE IN 子查询
// ============================================================

#[tokio::test]
async fn test_subquery_where_in() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "users", users_schema());
    create_table(&eng, "orders", orders_schema());
    insert_rows(&eng, "users", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Bob".into())],
        vec![Value::Int32(3), Value::Text("Charlie".into())],
    ]);
    insert_rows(&eng, "orders", &orders_types(), &[
        vec![Value::Int32(100), Value::Int32(1), Value::Int64(500)],
        vec![Value::Int32(101), Value::Int32(3), Value::Int64(300)],
    ]);
    let sql = format!(
        "SELECT name FROM {} WHERE id IN (SELECT user_id FROM {})",
        fqn("users"), fqn("orders")
    );
    assert_eq!(query_count(&eng, &sql).await, 2); // Alice, Charlie
    eng.shutdown().unwrap();
}

// ============================================================
// 11. 表达式计算
// ============================================================

#[tokio::test]
async fn test_expression_arithmetic() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(10), Value::Text("a".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT id * 2 + 1 as calc FROM {}", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_expression_case_when() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(5), Value::Text("b".into())],
        vec![Value::Int32(10), Value::Text("c".into())],
    ]);
    let ctx = create_session(eng.clone());
    let sql = format!(
        "SELECT id, CASE WHEN id < 5 THEN 'low' ELSE 'high' END as tier FROM {}",
        fqn("t")
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 3);
    eng.shutdown().unwrap();
}

// ============================================================
// 12b. 错误处理扩展
// ============================================================

#[tokio::test]
async fn test_error_type_mismatch_in_where() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
    ]);
    let ctx = create_session(eng.clone());
    // 字符串与 int 比较 — DataFusion 可能会自动转换或报错
    let result = ctx.sql(&format!("SELECT * FROM {} WHERE id = 'not_a_number'", fqn("t"))).await;
    // 无论成功（0行）还是报错，引擎都不应 panic
    match result {
        Ok(df) => { let _ = df.collect().await; }
        Err(_) => {} // 报错也是合理行为
    }
    eng.shutdown().unwrap();
}

// ============================================================
// 13. UNION
// ============================================================

#[tokio::test]
async fn test_union_all() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t1", users_schema());
    create_table(&eng, "t2", users_schema());
    insert_rows(&eng, "t1", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
    ]);
    insert_rows(&eng, "t2", &users_types(), &[
        vec![Value::Int32(2), Value::Text("b".into())],
    ]);
    let sql = format!(
        "SELECT * FROM {} UNION ALL SELECT * FROM {}",
        fqn("t1"), fqn("t2")
    );
    assert_eq!(query_count(&eng, &sql).await, 2);
    eng.shutdown().unwrap();
}

// ============================================================
// 14. 多列排序
// ============================================================

#[tokio::test]
async fn test_order_by_multi_column() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("b".into())],
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("a".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT * FROM {} ORDER BY id ASC, name ASC", fqn("t"))).await.unwrap()
        .collect().await.unwrap();
    let names = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.value(0), "a"); // id=1, name=a 排第一
    assert_eq!(names.value(1), "b"); // id=1, name=b 排第二
    eng.shutdown().unwrap();
}

// ============================================================
// 15. 跨页大数据量验证
// ============================================================

#[tokio::test]
async fn test_large_dataset_with_filter() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "big", users_schema());
    let rows: Vec<Vec<Value>> = (0..2000).map(|i| {
        vec![Value::Int32(i), Value::Text(format!("user_{}", i))]
    }).collect();
    insert_rows(&eng, "big", &users_types(), &rows);
    // 全量
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {}", fqn("big"))).await, 2000);
    // 范围过滤
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {} WHERE id >= 1000 AND id < 1500", fqn("big"))).await, 500);
    // 聚合
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT COUNT(*), MIN(id), MAX(id) FROM {}", fqn("big"))).await.unwrap()
        .collect().await.unwrap();
    let max_val = batches[0].column(2).as_any().downcast_ref::<Int32Array>().unwrap().value(0);
    assert_eq!(max_val, 1999);
    eng.shutdown().unwrap();
}

// ============================================================
// 16. Window Functions (DataFusion 天然能力验证)
// ============================================================

#[tokio::test]
async fn test_window_row_number() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(3), Value::Text("c".into())],
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("b".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id) as rn FROM {}", fqn("t")
    )).await.unwrap().collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 3);
    // ROW_NUMBER 返回 UInt64
    let rn = batches[0].column(1).as_any().downcast_ref::<UInt64Array>().unwrap();
    assert_eq!(rn.value(0), 1);
    assert_eq!(rn.value(2), 3);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_window_rank_dense_rank() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(1), Value::Text("b".into())],
        vec![Value::Int32(2), Value::Text("c".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT id, RANK() OVER (ORDER BY id) as rnk, DENSE_RANK() OVER (ORDER BY id) as drnk FROM {}",
        fqn("t")
    )).await.unwrap().collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 3);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_window_lag_lead() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(10), Value::Text("a".into())],
        vec![Value::Int32(20), Value::Text("b".into())],
        vec![Value::Int32(30), Value::Text("c".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT id, LAG(id, 1) OVER (ORDER BY id) as prev, LEAD(id, 1) OVER (ORDER BY id) as next FROM {}",
        fqn("t")
    )).await.unwrap().collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3);
    // 合并所有 batch 验证 LAG/LEAD 存在 NULL（结果可能跨 batch）
    let has_lag_null = batches.iter().any(|b| (0..b.num_rows()).any(|i| b.column(1).is_null(i)));
    let has_lead_null = batches.iter().any(|b| (0..b.num_rows()).any(|i| b.column(2).is_null(i)));
    assert!(has_lag_null, "LAG should produce at least one NULL");
    assert!(has_lead_null, "LEAD should produce at least one NULL");
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_window_partition_by() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("a".into())],
        vec![Value::Int32(3), Value::Text("b".into())],
        vec![Value::Int32(4), Value::Text("b".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT id, name, SUM(id) OVER (PARTITION BY name) as group_sum FROM {} ORDER BY id",
        fqn("t")
    )).await.unwrap().collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 4);
    eng.shutdown().unwrap();
}

// ============================================================
// 17. CTE (Common Table Expressions)
// ============================================================

#[tokio::test]
async fn test_cte_basic() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Bob".into())],
        vec![Value::Int32(3), Value::Text("Charlie".into())],
    ]);
    let ctx = create_session(eng.clone());
    let sql = format!(
        "WITH high_id AS (SELECT * FROM {} WHERE id > 1) SELECT COUNT(*) as cnt FROM high_id",
        fqn("t")
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let cnt = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(cnt, 2);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_cte_multi_level() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("a".into())],
        vec![Value::Int32(2), Value::Text("b".into())],
        vec![Value::Int32(3), Value::Text("c".into())],
    ]);
    let ctx = create_session(eng.clone());
    let sql = format!(
        "WITH step1 AS (SELECT id, name FROM {} WHERE id >= 2), \
         step2 AS (SELECT id * 10 as big_id FROM step1) \
         SELECT COUNT(*) as cnt FROM step2",
        fqn("t")
    );
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let cnt = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);
    assert_eq!(cnt, 2);
    eng.shutdown().unwrap();
}

// ============================================================
// 18. JOIN 补全: RIGHT / FULL OUTER / CROSS
// ============================================================

#[tokio::test]
async fn test_right_join() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "users", users_schema());
    create_table(&eng, "orders", orders_schema());
    insert_rows(&eng, "users", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
    ]);
    insert_rows(&eng, "orders", &orders_types(), &[
        vec![Value::Int32(100), Value::Int32(1), Value::Int64(500)],
        vec![Value::Int32(101), Value::Int32(99), Value::Int64(200)], // user 99 不存在
    ]);
    let sql = format!(
        "SELECT u.name, o.order_id FROM {} u RIGHT JOIN {} o ON u.id = o.user_id ORDER BY o.order_id",
        fqn("users"), fqn("orders")
    );
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2); // 两行: Alice+100, NULL+101
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_full_outer_join() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "users", users_schema());
    create_table(&eng, "orders", orders_schema());
    insert_rows(&eng, "users", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(2), Value::Text("Bob".into())],
    ]);
    insert_rows(&eng, "orders", &orders_types(), &[
        vec![Value::Int32(100), Value::Int32(1), Value::Int64(500)],
        vec![Value::Int32(101), Value::Int32(99), Value::Int64(200)],
    ]);
    let sql = format!(
        "SELECT u.name, o.order_id FROM {} u FULL OUTER JOIN {} o ON u.id = o.user_id",
        fqn("users"), fqn("orders")
    );
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&sql).await.unwrap().collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 3); // Alice+100, Bob+NULL, NULL+101
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_cross_join() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t1", vec![ColumnDef::new("a", DataType::Int32, false, 0)]);
    create_table(&eng, "t2", vec![ColumnDef::new("b", DataType::Int32, false, 0)]);
    insert_rows(&eng, "t1", &[DataType::Int32], &[
        vec![Value::Int32(1)], vec![Value::Int32(2)],
    ]);
    insert_rows(&eng, "t2", &[DataType::Int32], &[
        vec![Value::Int32(10)], vec![Value::Int32(20)], vec![Value::Int32(30)],
    ]);
    let sql = format!("SELECT a, b FROM {} CROSS JOIN {}", fqn("t1"), fqn("t2"));
    assert_eq!(query_count(&eng, &sql).await, 6); // 2 × 3
    eng.shutdown().unwrap();
}

// ============================================================
// 19. Set Operations: INTERSECT / EXCEPT
// ============================================================

#[tokio::test]
async fn test_intersect() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t1", vec![ColumnDef::new("id", DataType::Int32, false, 0)]);
    create_table(&eng, "t2", vec![ColumnDef::new("id", DataType::Int32, false, 0)]);
    insert_rows(&eng, "t1", &[DataType::Int32], &[
        vec![Value::Int32(1)], vec![Value::Int32(2)], vec![Value::Int32(3)],
    ]);
    insert_rows(&eng, "t2", &[DataType::Int32], &[
        vec![Value::Int32(2)], vec![Value::Int32(3)], vec![Value::Int32(4)],
    ]);
    let sql = format!(
        "SELECT id FROM {} INTERSECT SELECT id FROM {}", fqn("t1"), fqn("t2")
    );
    assert_eq!(query_count(&eng, &sql).await, 2); // 2, 3
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_except() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t1", vec![ColumnDef::new("id", DataType::Int32, false, 0)]);
    create_table(&eng, "t2", vec![ColumnDef::new("id", DataType::Int32, false, 0)]);
    insert_rows(&eng, "t1", &[DataType::Int32], &[
        vec![Value::Int32(1)], vec![Value::Int32(2)], vec![Value::Int32(3)],
    ]);
    insert_rows(&eng, "t2", &[DataType::Int32], &[
        vec![Value::Int32(2)],
    ]);
    let sql = format!(
        "SELECT id FROM {} EXCEPT SELECT id FROM {}", fqn("t1"), fqn("t2")
    );
    assert_eq!(query_count(&eng, &sql).await, 2); // 1, 3
    eng.shutdown().unwrap();
}

// ============================================================
// 20. NULL 函数: COALESCE / NULLIF
// ============================================================

#[tokio::test]
async fn test_coalesce() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Null],
        vec![Value::Int32(2), Value::Text("Bob".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT id, COALESCE(name, 'unknown') as name FROM {} ORDER BY id", fqn("t")
    )).await.unwrap().collect().await.unwrap();
    let names = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(names.value(0), "unknown");
    assert_eq!(names.value(1), "Bob");
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_nullif() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("Alice".into())],
        vec![Value::Int32(0), Value::Text("Bob".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT NULLIF(id, 0) as safe_id FROM {} ORDER BY id", fqn("t")
    )).await.unwrap().collect().await.unwrap();
    // NULLIF(0, 0) = NULL
    assert!(batches[0].column(0).is_null(0));
    eng.shutdown().unwrap();
}

// ============================================================
// 21. 标量字符串函数
// ============================================================

#[tokio::test]
async fn test_string_functions() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("  Hello World  ".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT UPPER(name), LOWER(name), LENGTH(TRIM(name)), REPLACE(name, 'World', 'Rust') FROM {}",
        fqn("t")
    )).await.unwrap().collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    let upper = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(upper.value(0), "  HELLO WORLD  ");
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_substring_concat() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(1), Value::Text("FerrisDB".into())],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT SUBSTRING(name, 1, 6) as sub, CONCAT(name, '-v2') as cat FROM {}",
        fqn("t")
    )).await.unwrap().collect().await.unwrap();
    let sub = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(sub.value(0), "Ferris");
    let cat = batches[0].column(1).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(cat.value(0), "FerrisDB-v2");
    eng.shutdown().unwrap();
}

// ============================================================
// 22. 数学函数
// ============================================================

#[tokio::test]
async fn test_math_functions() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "nums", vec![
        ColumnDef::new("val", DataType::Float64, false, 0),
    ]);
    insert_rows(&eng, "nums", &[DataType::Float64], &[
        vec![Value::Float64(-3.7)],
        vec![Value::Float64(16.0)],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT ABS(val), CEIL(val), FLOOR(val), ROUND(val) FROM {} ORDER BY val",
        fqn("nums")
    )).await.unwrap().collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 2);
    // ABS(-3.7) = 3.7
    let abs_col = batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap();
    assert!((abs_col.value(0) - 3.7).abs() < 0.001);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_math_power_sqrt_mod() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "nums", vec![
        ColumnDef::new("val", DataType::Float64, false, 0),
    ]);
    insert_rows(&eng, "nums", &[DataType::Float64], &[
        vec![Value::Float64(9.0)],
    ]);
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT POWER(val, 2), SQRT(val) FROM {}", fqn("nums")
    )).await.unwrap().collect().await.unwrap();
    let pow = batches[0].column(0).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    let sqrt = batches[0].column(1).as_any().downcast_ref::<Float64Array>().unwrap().value(0);
    assert!((pow - 81.0).abs() < 0.001);
    assert!((sqrt - 3.0).abs() < 0.001);
    eng.shutdown().unwrap();
}

// ============================================================
// 23. CAST 类型转换
// ============================================================

#[tokio::test]
async fn test_cast() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    insert_rows(&eng, "t", &users_types(), &[
        vec![Value::Int32(42), Value::Text("hello".into())],
    ]);
    let ctx = create_session(eng.clone());
    // INT → TEXT
    let batches = ctx.sql(&format!(
        "SELECT CAST(id AS VARCHAR) as id_str FROM {}", fqn("t")
    )).await.unwrap().collect().await.unwrap();
    let s = batches[0].column(0).as_any().downcast_ref::<StringArray>().unwrap();
    assert_eq!(s.value(0), "42");
    // TEXT → 应报错或返回空（不可转为 INT）
    eng.shutdown().unwrap();
}

// ============================================================
// 24. Timestamp / Date 类型
// ============================================================

#[tokio::test]
async fn test_timestamp_type() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "events", vec![
        ColumnDef::new("id", DataType::Int32, false, 0),
        ColumnDef::new("ts", DataType::Timestamp, true, 1),
    ]);
    // 1704067200000000 = 2024-01-01 00:00:00 UTC in microseconds
    // 1704153600000000 = 2024-01-02 00:00:00 UTC
    insert_rows(&eng, "events", &[DataType::Int32, DataType::Timestamp], &[
        vec![Value::Int32(1), Value::Timestamp(1704067200_000_000)],
        vec![Value::Int32(2), Value::Timestamp(1704153600_000_000)],
        vec![Value::Int32(3), Value::Null],
    ]);
    // 基础查询
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {}", fqn("events"))).await, 3);
    // WHERE 过滤时间戳 — 用 arrow_cast 转为 Timestamp 类型
    assert_eq!(query_count(&eng, &format!(
        "SELECT * FROM {} WHERE ts > arrow_cast(1704100000000000, 'Timestamp(Microsecond, None)')", fqn("events")
    )).await, 1);
    // NULL 时间戳
    assert_eq!(query_count(&eng, &format!(
        "SELECT * FROM {} WHERE ts IS NULL", fqn("events")
    )).await, 1);
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_date_type() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "logs", vec![
        ColumnDef::new("id", DataType::Int32, false, 0),
        ColumnDef::new("day", DataType::Date, true, 1),
    ]);
    // Date32: days since epoch. 2024-01-01 = 19723
    insert_rows(&eng, "logs", &[DataType::Int32, DataType::Date], &[
        vec![Value::Int32(1), Value::Date(19723)],  // 2024-01-01
        vec![Value::Int32(2), Value::Date(19724)],  // 2024-01-02
        vec![Value::Int32(3), Value::Date(19725)],  // 2024-01-03
    ]);
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {}", fqn("logs"))).await, 3);
    // ORDER BY date
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("SELECT id FROM {} ORDER BY day DESC", fqn("logs"))).await.unwrap()
        .collect().await.unwrap();
    let ids = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    assert_eq!(ids.value(0), 3); // 最新日期排第一
    eng.shutdown().unwrap();
}

#[tokio::test]
async fn test_timestamp_date_mixed_query() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "audit", vec![
        ColumnDef::new("user_id", DataType::Int32, false, 0),
        ColumnDef::new("action", DataType::Text, false, 1),
        ColumnDef::new("created_at", DataType::Timestamp, false, 2),
    ]);
    insert_rows(&eng, "audit", &[DataType::Int32, DataType::Text, DataType::Timestamp], &[
        vec![Value::Int32(1), Value::Text("login".into()), Value::Timestamp(1704067200_000_000)],
        vec![Value::Int32(1), Value::Text("logout".into()), Value::Timestamp(1704070800_000_000)],
        vec![Value::Int32(2), Value::Text("login".into()), Value::Timestamp(1704067200_000_000)],
    ]);
    // GROUP BY + 聚合 with timestamp
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT user_id, COUNT(*) as cnt FROM {} GROUP BY user_id ORDER BY user_id", fqn("audit")
    )).await.unwrap().collect().await.unwrap();
    let ids = batches[0].column(0).as_any().downcast_ref::<Int32Array>().unwrap();
    let cnts = batches[0].column(1).as_any().downcast_ref::<Int64Array>().unwrap();
    assert_eq!(ids.value(0), 1);
    assert_eq!(cnts.value(0), 2);
    assert_eq!(ids.value(1), 2);
    assert_eq!(cnts.value(1), 1);
    eng.shutdown().unwrap();
}

// ============================================================
// 25. Float32 + Int16 类型
// ============================================================

#[tokio::test]
async fn test_float32_int16_types() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "sensors", vec![
        ColumnDef::new("id", DataType::Int16, false, 0),
        ColumnDef::new("temp", DataType::Float32, false, 1),
        ColumnDef::new("label", DataType::Text, true, 2),
    ]);
    insert_rows(&eng, "sensors", &[DataType::Int16, DataType::Float32, DataType::Text], &[
        vec![Value::Int16(1), Value::Float32(23.5), Value::Text("indoor".into())],
        vec![Value::Int16(2), Value::Float32(35.2), Value::Text("outdoor".into())],
        vec![Value::Int16(3), Value::Float32(18.0), Value::Text("basement".into())],
    ]);
    // 全量查询
    assert_eq!(query_count(&eng, &format!("SELECT * FROM {}", fqn("sensors"))).await, 3);
    // 过滤
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!(
        "SELECT id, temp FROM {} WHERE temp > 20.0 ORDER BY temp", fqn("sensors")
    )).await.unwrap().collect().await.unwrap();
    let total: usize = batches.iter().map(|b| b.num_rows()).sum();
    assert_eq!(total, 2);
    // 聚合
    let batches = ctx.sql(&format!(
        "SELECT AVG(temp) as avg_temp FROM {}", fqn("sensors")
    )).await.unwrap().collect().await.unwrap();
    assert_eq!(batches[0].num_rows(), 1);
    eng.shutdown().unwrap();
}

// ============================================================
// 26. EXPLAIN (DataFusion 内建)
// ============================================================

#[tokio::test]
async fn test_explain() {
    let td = TempDir::new().unwrap();
    let eng = engine(&td);
    create_table(&eng, "t", users_schema());
    let ctx = create_session(eng.clone());
    let batches = ctx.sql(&format!("EXPLAIN SELECT * FROM {}", fqn("t")))
        .await.unwrap().collect().await.unwrap();
    assert!(batches[0].num_rows() > 0); // 应该有执行计划输出
    eng.shutdown().unwrap();
}
