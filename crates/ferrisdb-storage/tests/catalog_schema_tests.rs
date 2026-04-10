//! SystemCatalog Schema 扩展测试
//! 覆盖 ColumnDef + DataType + 持久化 + 向后兼容

use tempfile::TempDir;
use ferrisdb_storage::{SystemCatalog, ColumnDef, DataType, RelationType};

fn col(name: &str, dt: DataType) -> ColumnDef {
    ColumnDef::new(name, dt, true, 0)
}

fn col_nn(name: &str, dt: DataType, pos: u16) -> ColumnDef {
    ColumnDef::new(name, dt, false, pos)
}

// ============================================================
// DataType 基础
// ============================================================

#[test]
fn test_data_type_from_u8() {
    assert_eq!(DataType::from_u8(0), Some(DataType::Int32));
    assert_eq!(DataType::from_u8(1), Some(DataType::Int64));
    assert_eq!(DataType::from_u8(2), Some(DataType::Float64));
    assert_eq!(DataType::from_u8(3), Some(DataType::Text));
    assert_eq!(DataType::from_u8(4), Some(DataType::Boolean));
    assert_eq!(DataType::from_u8(5), Some(DataType::Bytes));
    assert_eq!(DataType::from_u8(99), None);
}

#[test]
fn test_data_type_fixed_size() {
    assert_eq!(DataType::Int32.fixed_size(), Some(4));
    assert_eq!(DataType::Int64.fixed_size(), Some(8));
    assert_eq!(DataType::Float64.fixed_size(), Some(8));
    assert_eq!(DataType::Boolean.fixed_size(), Some(1));
    assert_eq!(DataType::Text.fixed_size(), None);
    assert_eq!(DataType::Bytes.fixed_size(), None);
}

#[test]
fn test_data_type_sql_name() {
    assert_eq!(DataType::Int32.sql_name(), "INT");
    assert_eq!(DataType::Int64.sql_name(), "BIGINT");
    assert_eq!(DataType::Text.sql_name(), "TEXT");
    assert_eq!(DataType::Boolean.sql_name(), "BOOLEAN");
}

// ============================================================
// ColumnDef 序列化
// ============================================================

/// ColumnDef roundtrip 通过 catalog persist+reload 间接验证
#[test]
fn test_column_def_roundtrip_via_persist() {
    let td = TempDir::new().unwrap();
    let columns = vec![
        ColumnDef::new("user_name", DataType::Text, true, 2),
        ColumnDef::new("age", DataType::Int32, false, 0),
    ];
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        cat.create_table_with_schema("rt_test", 0, columns).unwrap();
    }
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        let meta = cat.lookup_by_name("rt_test").unwrap();
        assert_eq!(meta.columns[0].name, "user_name");
        assert_eq!(meta.columns[0].data_type, DataType::Text);
        assert!(meta.columns[0].nullable);
        assert_eq!(meta.columns[0].position, 2);
        assert_eq!(meta.columns[1].name, "age");
        assert!(!meta.columns[1].nullable);
    }
}

#[test]
fn test_column_def_all_types_via_persist() {
    let td = TempDir::new().unwrap();
    let columns: Vec<ColumnDef> = [DataType::Int32, DataType::Int64, DataType::Float64,
               DataType::Text, DataType::Boolean, DataType::Bytes]
        .iter().enumerate()
        .map(|(i, dt)| ColumnDef::new(&format!("col_{}", i), *dt, false, i as u16))
        .collect();
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        cat.create_table_with_schema("all_types", 0, columns).unwrap();
    }
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        let meta = cat.lookup_by_name("all_types").unwrap();
        assert_eq!(meta.columns.len(), 6);
        assert_eq!(meta.columns[0].data_type, DataType::Int32);
        assert_eq!(meta.columns[3].data_type, DataType::Text);
        assert_eq!(meta.columns[5].data_type, DataType::Bytes);
    }
}

// ============================================================
// CREATE TABLE with Schema
// ============================================================

#[test]
fn test_create_table_with_schema() {
    let cat = SystemCatalog::new();
    let columns = vec![
        col_nn("id", DataType::Int32, 0),
        col("name", DataType::Text),
        col("email", DataType::Text),
        col_nn("age", DataType::Int32, 3),
    ];
    let oid = cat.create_table_with_schema("users", 0, columns).unwrap();
    let meta = cat.lookup_by_oid(oid).unwrap();
    assert_eq!(meta.columns.len(), 4);
    assert_eq!(meta.columns[0].name, "id");
    assert_eq!(meta.columns[0].data_type, DataType::Int32);
    assert!(!meta.columns[0].nullable);
    assert_eq!(meta.columns[1].name, "name");
    assert_eq!(meta.columns[1].data_type, DataType::Text);
    assert!(meta.columns[1].nullable);
}

#[test]
fn test_create_table_without_schema_backward_compat() {
    let cat = SystemCatalog::new();
    let oid = cat.create_table("old_table", 0).unwrap();
    let meta = cat.lookup_by_oid(oid).unwrap();
    assert!(meta.columns.is_empty()); // 旧接口无列定义
}

#[test]
fn test_create_table_duplicate_name_rejected() {
    let cat = SystemCatalog::new();
    cat.create_table_with_schema("users", 0, vec![col("id", DataType::Int32)]).unwrap();
    let result = cat.create_table_with_schema("users", 0, vec![]);
    assert!(result.is_err());
}

// ============================================================
// Schema 持久化 + 重启恢复
// ============================================================

#[test]
fn test_schema_persist_and_reload() {
    let td = TempDir::new().unwrap();
    let columns = vec![
        col_nn("id", DataType::Int64, 0),
        col("title", DataType::Text),
        col("price", DataType::Float64),
        col("active", DataType::Boolean),
        col("data", DataType::Bytes),
    ];

    // 创建 + 持久化
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        cat.create_table_with_schema("products", 0, columns.clone()).unwrap();
    }

    // 重启 + 验证
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        let meta = cat.lookup_by_name("products").unwrap();
        assert_eq!(meta.columns.len(), 5);
        assert_eq!(meta.columns[0].name, "id");
        assert_eq!(meta.columns[0].data_type, DataType::Int64);
        assert!(!meta.columns[0].nullable);
        assert_eq!(meta.columns[1].name, "title");
        assert_eq!(meta.columns[1].data_type, DataType::Text);
        assert!(meta.columns[1].nullable);
        assert_eq!(meta.columns[2].data_type, DataType::Float64);
        assert_eq!(meta.columns[3].data_type, DataType::Boolean);
        assert_eq!(meta.columns[4].data_type, DataType::Bytes);
    }
}

#[test]
fn test_schema_backward_compat_old_format() {
    let td = TempDir::new().unwrap();
    // 先用旧接口创建（无 columns）
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        cat.create_table("legacy", 0).unwrap();
    }
    // 重启 → 旧格式仍可读
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        let meta = cat.lookup_by_name("legacy").unwrap();
        assert_eq!(meta.name, "legacy");
        assert!(meta.columns.is_empty()); // 旧格式无 columns
    }
}

#[test]
fn test_schema_mixed_old_new_format() {
    let td = TempDir::new().unwrap();
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        cat.create_table("old_t", 0).unwrap(); // 无 schema
        cat.create_table_with_schema("new_t", 0, vec![
            col("x", DataType::Int32),
            col("y", DataType::Float64),
        ]).unwrap(); // 有 schema
    }
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        assert!(cat.lookup_by_name("old_t").unwrap().columns.is_empty());
        assert_eq!(cat.lookup_by_name("new_t").unwrap().columns.len(), 2);
    }
}

// ============================================================
// Schema + Index 组合
// ============================================================

#[test]
fn test_schema_with_index() {
    let td = TempDir::new().unwrap();
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        let t_oid = cat.create_table_with_schema("orders", 0, vec![
            col_nn("order_id", DataType::Int64, 0),
            col_nn("customer_id", DataType::Int32, 1),
            col("amount", DataType::Float64),
        ]).unwrap();
        cat.create_index("orders_pk", t_oid, 0).unwrap();
        cat.create_index("orders_cust_idx", t_oid, 0).unwrap();
    }
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        let t = cat.lookup_by_name("orders").unwrap();
        assert_eq!(t.columns.len(), 3);
        let indexes = cat.list_indexes(t.oid);
        assert_eq!(indexes.len(), 2);
        // 索引无 columns
        assert!(indexes[0].columns.is_empty());
    }
}

// ============================================================
// 大量列 + 特殊字符
// ============================================================

#[test]
fn test_many_columns() {
    let cat = SystemCatalog::new();
    let columns: Vec<ColumnDef> = (0..100).map(|i| {
        ColumnDef::new(&format!("col_{}", i), DataType::Int32, i % 2 == 0, i as u16)
    }).collect();
    let oid = cat.create_table_with_schema("wide_table", 0, columns).unwrap();
    let meta = cat.lookup_by_oid(oid).unwrap();
    assert_eq!(meta.columns.len(), 100);
    assert_eq!(meta.columns[50].name, "col_50");
    assert!(meta.columns[50].nullable);
    assert!(!meta.columns[51].nullable);
}

#[test]
fn test_unicode_column_names() {
    let cat = SystemCatalog::new();
    let columns = vec![
        col("用户名", DataType::Text),
        col("年龄", DataType::Int32),
        col("メール", DataType::Text),
    ];
    let oid = cat.create_table_with_schema("用户表", 0, columns).unwrap();
    let meta = cat.lookup_by_oid(oid).unwrap();
    assert_eq!(meta.columns[0].name, "用户名");
    assert_eq!(meta.name, "用户表");
}

// ============================================================
// Drop 表后 Schema 清理
// ============================================================

#[test]
fn test_drop_table_clears_schema() {
    let td = TempDir::new().unwrap();
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        let oid = cat.create_table_with_schema("temp", 0, vec![
            col("a", DataType::Int32),
        ]).unwrap();
        cat.drop_relation(oid).unwrap();
    }
    {
        let cat = SystemCatalog::open(td.path()).unwrap();
        assert!(cat.lookup_by_name("temp").is_none());
        assert_eq!(cat.count(), 0);
    }
}
