//! sqllogictest 驱动器 — 运行 .slt 文件验证 FerrisDB SQL 能力
//!
//! 实现 sqllogictest::AsyncDB trait，将 SQL 转发给 DataFusion SessionContext 执行。

use std::sync::Arc;

use arrow::array::*;
use arrow::datatypes::DataType as ArrowDataType;
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use sqllogictest::{AsyncDB, DBOutput, DefaultColumnType, Runner};
use tempfile::TempDir;

use ferrisdb::{Engine, EngineConfig};
use ferrisdb_sql::session::create_session;

/// 错误类型
#[derive(Debug)]
struct FerrisError(String);

impl std::fmt::Display for FerrisError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for FerrisError {}

/// FerrisDB 的 sqllogictest 适配器
struct FerrisDB {
    ctx: SessionContext,
    _engine: Arc<Engine>,
    _td: Arc<TempDir>,
}

impl FerrisDB {
    fn new() -> Self {
        let td = TempDir::new().unwrap();
        let engine = Arc::new(
            Engine::open(EngineConfig {
                data_dir: td.path().to_path_buf(),
                shared_buffers: 500,
                wal_enabled: false,
                ..Default::default()
            })
            .unwrap(),
        );
        let ctx = create_session(engine.clone());
        Self {
            ctx,
            _engine: engine,
            _td: Arc::new(td),
        }
    }
}

#[async_trait]
impl AsyncDB for FerrisDB {
    type Error = FerrisError;
    type ColumnType = DefaultColumnType;

    async fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
        let sql_trimmed = sql.trim();

        // DDL: CREATE TABLE — 需要特殊处理，因为 DataFusion 不直接支持写 FerrisDB
        if sql_trimmed.to_uppercase().starts_with("CREATE TABLE") {
            self.handle_create_table(sql_trimmed).map_err(|e| FerrisError(e.to_string()))?;
            return Ok(DBOutput::StatementComplete(0));
        }

        // DML: INSERT INTO — 需要特殊处理
        if sql_trimmed.to_uppercase().starts_with("INSERT INTO") {
            let count = self.handle_insert(sql_trimmed).map_err(|e| FerrisError(e.to_string()))?;
            return Ok(DBOutput::StatementComplete(count));
        }

        // 其他 SQL 走 DataFusion
        let df = self.ctx.sql(sql_trimmed).await.map_err(|e| FerrisError(e.to_string()))?;
        let batches = df.collect().await.map_err(|e| FerrisError(e.to_string()))?;

        if batches.is_empty() || batches.iter().all(|b| b.num_columns() == 0) {
            return Ok(DBOutput::StatementComplete(0));
        }

        // 收集列类型
        let schema = batches[0].schema();
        let types: Vec<DefaultColumnType> = schema
            .fields()
            .iter()
            .map(|f| match f.data_type() {
                ArrowDataType::Int8
                | ArrowDataType::Int16
                | ArrowDataType::Int32
                | ArrowDataType::Int64
                | ArrowDataType::UInt8
                | ArrowDataType::UInt16
                | ArrowDataType::UInt32
                | ArrowDataType::UInt64 => DefaultColumnType::Integer,
                ArrowDataType::Float16
                | ArrowDataType::Float32
                | ArrowDataType::Float64 => DefaultColumnType::FloatingPoint,
                _ => DefaultColumnType::Text,
            })
            .collect();

        // 收集所有行为字符串
        let mut rows = Vec::new();
        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let row: Vec<String> = (0..batch.num_columns())
                    .map(|col_idx| {
                        let col = batch.column(col_idx);
                        if col.is_null(row_idx) {
                            "NULL".to_string()
                        } else {
                            array_value_to_string(col, row_idx)
                        }
                    })
                    .collect();
                rows.push(row);
            }
        }

        Ok(DBOutput::Rows { types, rows })
    }

    async fn shutdown(&mut self) {}

    fn engine_name(&self) -> &str {
        "ferrisdb"
    }

    async fn sleep(dur: std::time::Duration) {
        tokio::time::sleep(dur).await;
    }
}

impl FerrisDB {
    fn handle_create_table(&mut self, sql: &str) -> Result<(), String> {
        use ferrisdb_storage::catalog::{ColumnDef, DataType};

        // 简单解析: CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
        let upper = sql.to_uppercase();
        let after_table = &sql[upper.find("TABLE").unwrap() + 5..].trim();
        let paren_pos = after_table.find('(').ok_or_else(|| "missing (".to_string())?;
        let table_name = after_table[..paren_pos].trim().to_string();
        let cols_str = &after_table[paren_pos + 1..after_table.rfind(')').ok_or_else(|| "missing )".to_string())?];

        let mut columns = Vec::new();
        for (pos, col_def) in cols_str.split(',').enumerate() {
            let parts: Vec<&str> = col_def.trim().split_whitespace().collect();
            if parts.len() < 2 {
                continue;
            }
            let col_name = parts[0].to_lowercase();
            let col_type = match parts[1].to_uppercase().as_str() {
                "INT" | "INTEGER" | "INT4" => DataType::Int32,
                "BIGINT" | "INT8" => DataType::Int64,
                "SMALLINT" | "INT2" => DataType::Int16,
                "REAL" | "FLOAT4" | "FLOAT" => DataType::Float32,
                "DOUBLE" | "FLOAT8" => DataType::Float64,
                "TEXT" | "VARCHAR" | "STRING" => DataType::Text,
                "BOOLEAN" | "BOOL" => DataType::Boolean,
                "BYTEA" | "BLOB" => DataType::Bytes,
                "TIMESTAMP" => DataType::Timestamp,
                "DATE" => DataType::Date,
                other => return Err(format!("unsupported type: {}", other)),
            };
            let nullable = !col_def.to_uppercase().contains("NOT NULL");
            columns.push(ColumnDef::new(&col_name, col_type, nullable, pos as u16));
        }

        // 创建表
        self._engine.create_table(&table_name).map_err(|e| e.to_string())?;
        self._engine.drop_table(&table_name).map_err(|e| e.to_string())?;
        self._engine
            .catalog()
            .create_table_with_schema(&table_name, 0, columns)
            .map_err(|e| e.to_string())?;

        // 重建 session 使新表可见
        self.ctx = create_session(self._engine.clone());

        Ok(())
    }

    fn handle_insert(&self, sql: &str) -> Result<u64, String> {
        use ferrisdb_storage::catalog::DataType;
        use ferrisdb_storage::row_codec::{encode_row, Value};

        // 简单解析: INSERT INTO table VALUES (v1, v2), (v3, v4)
        let upper = sql.to_uppercase();
        let into_pos = upper.find("INTO").ok_or_else(|| "missing INTO".to_string())? + 4;
        let rest = &sql[into_pos..].trim();
        let values_pos = rest.to_uppercase().find("VALUES").ok_or_else(|| "missing VALUES".to_string())?;
        let table_name = rest[..values_pos].trim().to_string();
        let values_str = &rest[values_pos + 6..].trim();

        let meta = self
            ._engine
            .catalog()
            .lookup_by_name(&table_name)
            .ok_or_else(|| format!("table '{}' not found", table_name))?;
        // 确保 table_name 不带引号
        let table_name = table_name.trim_matches('"');
        let col_types: Vec<DataType> = meta.columns.iter().map(|c| c.data_type).collect();

        let table = self._engine.open_table(&table_name).map_err(|e| e.to_string())?;
        let mut count = 0u64;

        for row_str in values_str.split("),(") {
            let cleaned = row_str.trim_matches(|c: char| c == '(' || c == ')' || c == ' ');
            let values: Vec<Value> = cleaned
                .split(',')
                .zip(col_types.iter())
                .map(|(val_str, dt)| {
                    let v = val_str.trim().trim_matches('\'');
                    if v.to_uppercase() == "NULL" {
                        return Value::Null;
                    }
                    match dt {
                        DataType::Int16 => v.parse::<i16>().map(Value::Int16).unwrap_or(Value::Null),
                        DataType::Int32 => v.parse::<i32>().map(Value::Int32).unwrap_or(Value::Null),
                        DataType::Int64 => v.parse::<i64>().map(Value::Int64).unwrap_or(Value::Null),
                        DataType::Float32 => v.parse::<f32>().map(Value::Float32).unwrap_or(Value::Null),
                        DataType::Float64 => v.parse::<f64>().map(Value::Float64).unwrap_or(Value::Null),
                        DataType::Text => Value::Text(v.to_string()),
                        DataType::Boolean => Value::Boolean(v.to_uppercase() == "TRUE" || v == "1"),
                        DataType::Bytes => Value::Bytes(v.as_bytes().to_vec()),
                        DataType::Timestamp => v.parse::<i64>().map(Value::Timestamp).unwrap_or(Value::Null),
                        DataType::Date => v.parse::<i32>().map(Value::Date).unwrap_or(Value::Null),
                    }
                })
                .collect();

            let encoded = encode_row(&col_types, &values);
            let mut txn = self._engine.begin().map_err(|e| e.to_string())?;
            table
                .insert(&encoded, txn.xid(), count as u32)
                .map_err(|e| e.to_string())?;
            txn.commit().map_err(|e| e.to_string())?;
            count += 1;
        }

        self._engine
            .save_table_state(&table_name, &table)
            .map_err(|e| e.to_string())?;

        Ok(count)
    }
}

/// 将 Arrow Array 的单个值转换为字符串
fn array_value_to_string(col: &dyn Array, idx: usize) -> String {
    use arrow::util::display::ArrayFormatter;
    let formatter = ArrayFormatter::try_new(col, &Default::default()).unwrap();
    formatter.value(idx).to_string()
}

// ============================================================
// 测试入口：运行 test_files/ 下的 .slt 文件
// ============================================================

#[tokio::test]
async fn run_slt_select() {
    run_slt_file("select.slt").await;
}

#[tokio::test]
async fn run_slt_aggregate() {
    run_slt_file("aggregate.slt").await;
}

#[tokio::test]
async fn run_slt_join() {
    run_slt_file("join.slt").await;
}

#[tokio::test]
async fn run_slt_window() {
    run_slt_file("window.slt").await;
}

#[tokio::test]
async fn run_slt_cte() {
    run_slt_file("cte.slt").await;
}

#[tokio::test]
async fn run_slt_functions() {
    run_slt_file("functions.slt").await;
}

async fn run_slt_file(filename: &str) {
    let path = format!(
        "{}/test_files/{}",
        env!("CARGO_MANIFEST_DIR"),
        filename
    );
    if !std::path::Path::new(&path).exists() {
        eprintln!("SKIP: {} not found", path);
        return;
    }
    let db = FerrisDB::new();
    let mut runner = Runner::new(|| async { Ok(FerrisDB::new()) });
    runner.run_file_async(&path).await.unwrap();
}
