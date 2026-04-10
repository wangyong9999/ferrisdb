//! pgwire 服务端 — PostgreSQL 协议处理
//!
//! 接收 psql/JDBC 连接，将 SQL 转发给 DataFusion 执行，结果返回给客户端。

use std::fmt::Debug;
use std::sync::Arc;

use async_trait::async_trait;
use futures::{stream, Sink, StreamExt};
use tokio::net::TcpListener;

use pgwire::api::auth::noop::NoopStartupHandler;
use pgwire::api::copy::NoopCopyHandler;
use pgwire::api::query::{PlaceholderExtendedQueryHandler, SimpleQueryHandler};
use pgwire::api::results::{DataRowEncoder, FieldFormat, FieldInfo, QueryResponse, Response, Tag};
use pgwire::api::{ClientInfo, NoopErrorHandler, PgWireServerHandlers, Type};
use pgwire::error::{PgWireError, PgWireResult};
use pgwire::messages::PgWireBackendMessage;
use pgwire::tokio::process_socket;

use arrow::array::*;
use arrow::datatypes::DataType as ArrowDataType;

use ferrisdb::Engine;
use ferrisdb_storage::catalog::{ColumnDef, DataType};
use ferrisdb_storage::row_codec::{encode_row, Value};

use crate::session::create_session;

/// FerrisDB SQL 查询处理器
pub struct FerrisQueryHandler {
    engine: Arc<Engine>,
}

impl FerrisQueryHandler {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

impl NoopStartupHandler for FerrisQueryHandler {}

/// DataType → pgwire Type 映射
fn to_pg_type(dt: &DataType) -> Type {
    match dt {
        DataType::Int32 => Type::INT4,
        DataType::Int64 => Type::INT8,
        DataType::Float64 => Type::FLOAT8,
        DataType::Text => Type::VARCHAR,
        DataType::Boolean => Type::BOOL,
        DataType::Bytes => Type::BYTEA,
        DataType::Timestamp => Type::TIMESTAMP,
        DataType::Date => Type::DATE,
        DataType::Float32 => Type::FLOAT4,
        DataType::Int16 => Type::INT2,
    }
}

/// Arrow DataType → pgwire Type
fn arrow_to_pg_type(dt: &ArrowDataType) -> Type {
    match dt {
        ArrowDataType::Int32 => Type::INT4,
        ArrowDataType::Int64 => Type::INT8,
        ArrowDataType::Float64 => Type::FLOAT8,
        ArrowDataType::Utf8 => Type::VARCHAR,
        ArrowDataType::Boolean => Type::BOOL,
        ArrowDataType::Binary => Type::BYTEA,
        ArrowDataType::UInt64 => Type::INT8,
        _ => Type::VARCHAR, // fallback
    }
}

#[async_trait]
impl SimpleQueryHandler for FerrisQueryHandler {
    async fn do_query<'a, C>(
        &self,
        _client: &mut C,
        query: &'a str,
    ) -> PgWireResult<Vec<Response<'a>>>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let trimmed = query.trim().trim_end_matches(';').trim();

        // CREATE TABLE 特殊处理（DataFusion 不直接支持 FerrisDB 的 CREATE TABLE）
        if trimmed.to_uppercase().starts_with("CREATE TABLE") {
            return self.handle_create_table(trimmed).await;
        }

        // INSERT 特殊处理
        if trimmed.to_uppercase().starts_with("INSERT") {
            return self.handle_insert(trimmed).await;
        }

        // DELETE
        if trimmed.to_uppercase().starts_with("DELETE") {
            return self.handle_delete(trimmed).await;
        }

        // UPDATE
        if trimmed.to_uppercase().starts_with("UPDATE") {
            return self.handle_update(trimmed).await;
        }

        // DROP TABLE
        if trimmed.to_uppercase().starts_with("DROP TABLE") {
            return self.handle_drop_table(trimmed).await;
        }

        // BEGIN/COMMIT/ROLLBACK (暂时返回 OK，事务在每条语句级别自动管理)
        let upper = trimmed.to_uppercase();
        if upper == "BEGIN" || upper == "START TRANSACTION" {
            return Ok(vec![Response::Execution(Tag::new("BEGIN"))]);
        }
        if upper == "COMMIT" || upper == "END" {
            return Ok(vec![Response::Execution(Tag::new("COMMIT"))]);
        }
        if upper == "ROLLBACK" {
            return Ok(vec![Response::Execution(Tag::new("ROLLBACK"))]);
        }

        // SELECT 和其他查询走 DataFusion
        let ctx = create_session(Arc::clone(&self.engine));

        let df = ctx.sql(query).await.map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(),
                "42601".to_owned(),
                format!("{}", e),
            )))
        })?;

        let batches = df.collect().await.map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(),
                "XX000".to_owned(),
                format!("{}", e),
            )))
        })?;

        if batches.is_empty() {
            return Ok(vec![Response::Execution(Tag::new("SELECT").with_rows(0))]);
        }

        // 构建 pgwire schema
        let arrow_schema = batches[0].schema();
        let pg_fields: Vec<FieldInfo> = arrow_schema
            .fields()
            .iter()
            .map(|f| {
                FieldInfo::new(
                    f.name().clone(),
                    None,
                    None,
                    arrow_to_pg_type(f.data_type()),
                    FieldFormat::Text,
                )
            })
            .collect();
        let pg_schema = Arc::new(pg_fields);

        // 转换 Arrow batches → pgwire data rows
        let mut all_rows = Vec::new();
        for batch in &batches {
            for row_idx in 0..batch.num_rows() {
                let mut row_values: Vec<Option<String>> = Vec::new();
                for col_idx in 0..batch.num_columns() {
                    let col = batch.column(col_idx);
                    if col.is_null(row_idx) {
                        row_values.push(None);
                    } else {
                        let s = arrow::util::display::array_value_to_string(col, row_idx)
                            .unwrap_or_else(|_| "NULL".to_string());
                        row_values.push(Some(s));
                    }
                }
                all_rows.push(row_values);
            }
        }

        let total_rows = all_rows.len();
        let schema_ref = pg_schema.clone();
        let data_stream = stream::iter(all_rows.into_iter()).map(move |row| {
            let mut encoder = DataRowEncoder::new(schema_ref.clone());
            for val in &row {
                encoder.encode_field(val)?;
            }
            encoder.finish()
        });

        Ok(vec![Response::Query(QueryResponse::new(pg_schema, data_stream))])
    }
}

impl FerrisQueryHandler {
    /// 处理 CREATE TABLE
    async fn handle_create_table<'a>(&self, sql: &'a str) -> PgWireResult<Vec<Response<'a>>> {
        // 简单解析: CREATE TABLE name (col1 TYPE, col2 TYPE, ...)
        let sql_upper = sql.to_uppercase();
        let after_table = sql_upper.find("TABLE").map(|i| i + 5).unwrap_or(0);
        let rest = sql[after_table..].trim();

        let paren_pos = rest.find('(').ok_or_else(|| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "42601".to_owned(),
                "Invalid CREATE TABLE syntax".to_owned(),
            )))
        })?;

        let table_name = rest[..paren_pos].trim().to_lowercase();
        let cols_str = rest[paren_pos+1..].trim_end_matches(')').trim();

        let mut columns = Vec::new();
        for (pos, col_def) in cols_str.split(',').enumerate() {
            let parts: Vec<&str> = col_def.trim().split_whitespace().collect();
            if parts.len() < 2 { continue; }
            let col_name = parts[0].to_lowercase();
            let col_type = match parts[1].to_uppercase().as_str() {
                "INT" | "INT4" | "INTEGER" => DataType::Int32,
                "BIGINT" | "INT8" => DataType::Int64,
                "FLOAT" | "FLOAT8" | "DOUBLE" | "REAL" => DataType::Float64,
                "TEXT" | "VARCHAR" | "STRING" => DataType::Text,
                "BOOL" | "BOOLEAN" => DataType::Boolean,
                "BYTEA" | "BYTES" | "BLOB" => DataType::Bytes,
                _ => DataType::Text,
            };
            let nullable = !parts.iter().any(|p| p.to_uppercase() == "NOT")
                || !parts.iter().any(|p| p.to_uppercase() == "NULL");
            columns.push(ColumnDef::new(&col_name, col_type, nullable, pos as u16));
        }

        // 在 FerrisDB 中创建表
        self.engine.create_table(&table_name).map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "42P07".to_owned(), format!("{}", e),
            )))
        })?;

        // 更新 catalog 加 Schema（先 drop 无 schema 版本再重建）
        self.engine.drop_table(&table_name).map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
            )))
        })?;
        self.engine.catalog().create_table_with_schema(&table_name, 0, columns).map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
            )))
        })?;

        Ok(vec![Response::Execution(Tag::new("CREATE TABLE"))])
    }

    /// 处理 INSERT INTO
    async fn handle_insert<'a>(&self, sql: &'a str) -> PgWireResult<Vec<Response<'a>>> {
        // 简单解析: INSERT INTO table VALUES (v1, v2, ...)
        let sql_upper = sql.to_uppercase();
        let into_pos = sql_upper.find("INTO").map(|i| i + 4).unwrap_or(0);
        let rest = sql[into_pos..].trim();

        let values_pos = rest.to_uppercase().find("VALUES").ok_or_else(|| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "42601".to_owned(),
                "Invalid INSERT syntax, expected VALUES".to_owned(),
            )))
        })?;

        let table_name = rest[..values_pos].trim().to_lowercase();
        let values_str = rest[values_pos+6..].trim();

        // 查表 Schema
        let meta = self.engine.catalog().lookup_by_name(&table_name).ok_or_else(|| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "42P01".to_owned(),
                format!("Table '{}' not found", table_name),
            )))
        })?;

        let col_types: Vec<DataType> = meta.columns.iter().map(|c| c.data_type).collect();

        // 解析值列表: (v1, v2, ...) 可能有多行
        let mut inserted = 0usize;
        for row_str in values_str.split("),(") {
            let cleaned = row_str.trim_matches(|c| c == '(' || c == ')' || c == ' ');
            let values: Vec<Value> = cleaned.split(',')
                .zip(col_types.iter())
                .map(|(val_str, dt)| {
                    let v = val_str.trim().trim_matches('\'');
                    if v.to_uppercase() == "NULL" {
                        return Value::Null;
                    }
                    match dt {
                        DataType::Int32 => v.parse::<i32>().map(Value::Int32).unwrap_or(Value::Null),
                        DataType::Int64 => v.parse::<i64>().map(Value::Int64).unwrap_or(Value::Null),
                        DataType::Float64 => v.parse::<f64>().map(Value::Float64).unwrap_or(Value::Null),
                        DataType::Text => Value::Text(v.to_string()),
                        DataType::Boolean => Value::Boolean(v.to_uppercase() == "TRUE" || v == "1"),
                        DataType::Bytes => Value::Bytes(v.as_bytes().to_vec()),
                        DataType::Timestamp => v.parse::<i64>().map(Value::Timestamp).unwrap_or(Value::Null),
                        DataType::Date => v.parse::<i32>().map(Value::Date).unwrap_or(Value::Null),
                        DataType::Float32 => v.parse::<f32>().map(Value::Float32).unwrap_or(Value::Null),
                        DataType::Int16 => v.parse::<i16>().map(Value::Int16).unwrap_or(Value::Null),
                    }
                })
                .collect();

            let encoded = encode_row(&col_types, &values);

            // 使用 Engine 事务插入
            let table = self.engine.open_table(&table_name).map_err(|e| {
                PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
                )))
            })?;
            let mut txn = self.engine.begin().map_err(|e| {
                PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
                )))
            })?;
            table.insert(&encoded, txn.xid(), 0).map_err(|e| {
                PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
                )))
            })?;
            txn.commit().map_err(|e| {
                PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                    "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
                )))
            })?;
            inserted += 1;
        }

        Ok(vec![Response::Execution(Tag::new("INSERT").with_rows(inserted))])
    }

    /// 处理 DELETE FROM table WHERE ...
    async fn handle_delete<'a>(&self, sql: &'a str) -> PgWireResult<Vec<Response<'a>>> {
        // 简单解析: DELETE FROM table [WHERE condition]
        let sql_upper = sql.to_uppercase();
        let from_pos = sql_upper.find("FROM").map(|i| i + 4).unwrap_or(0);
        let rest = sql[from_pos..].trim();

        let (table_name, _where_clause) = if let Some(w) = rest.to_uppercase().find("WHERE") {
            (rest[..w].trim().to_lowercase(), Some(&rest[w+5..]))
        } else {
            (rest.trim().to_lowercase(), None)
        };

        // 打开表
        let table = self.engine.open_table(&table_name).map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "42P01".to_owned(), format!("{}", e),
            )))
        })?;

        let meta = self.engine.catalog().lookup_by_name(&table_name).ok_or_else(|| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "42P01".to_owned(),
                format!("Table '{}' not found", table_name),
            )))
        })?;

        // 扫描找到所有行，然后删除
        // TODO: WHERE 条件过滤（当前删除所有行）
        let mut txn = self.engine.begin().map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
            )))
        })?;
        let xid = txn.xid();

        let mut scan = ferrisdb_transaction::HeapScan::new(&table);
        let mut tids = Vec::new();
        while let Ok(Some((tid, _hdr, _data))) = scan.next() {
            tids.push(tid);
        }

        let mut deleted = 0usize;
        for (i, tid) in tids.iter().enumerate() {
            if table.delete(*tid, xid, i as u32).is_ok() {
                deleted += 1;
            }
        }
        txn.commit().map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
            )))
        })?;

        Ok(vec![Response::Execution(Tag::new("DELETE").with_rows(deleted))])
    }

    /// 处理 UPDATE table SET col=val WHERE ...
    async fn handle_update<'a>(&self, sql: &'a str) -> PgWireResult<Vec<Response<'a>>> {
        // 简单解析: UPDATE table SET col1=val1, col2=val2 [WHERE ...]
        let sql_upper = sql.to_uppercase();
        let set_pos = sql_upper.find("SET").ok_or_else(|| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "42601".to_owned(),
                "Invalid UPDATE syntax, expected SET".to_owned(),
            )))
        })?;

        let table_name = sql[6..set_pos].trim().to_lowercase(); // skip "UPDATE "
        let after_set = &sql[set_pos+3..];

        let (set_clause, _where_clause) = if let Some(w) = after_set.to_uppercase().find("WHERE") {
            (&after_set[..w], Some(&after_set[w+5..]))
        } else {
            (after_set.trim(), None)
        };

        let meta = self.engine.catalog().lookup_by_name(&table_name).ok_or_else(|| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "42P01".to_owned(),
                format!("Table '{}' not found", table_name),
            )))
        })?;

        let col_types: Vec<DataType> = meta.columns.iter().map(|c| c.data_type).collect();

        // 解析 SET 子句: col1=val1, col2=val2
        let mut updates: Vec<(String, String)> = Vec::new();
        for assign in set_clause.split(',') {
            let parts: Vec<&str> = assign.splitn(2, '=').collect();
            if parts.len() == 2 {
                updates.push((parts[0].trim().to_lowercase(), parts[1].trim().trim_matches('\'').to_string()));
            }
        }

        let table = self.engine.open_table(&table_name).map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
            )))
        })?;

        let mut txn = self.engine.begin().map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
            )))
        })?;
        let xid = txn.xid();

        // 扫描所有行，修改后 update
        let mut scan = ferrisdb_transaction::HeapScan::new(&table);
        let mut rows_to_update = Vec::new();
        while let Ok(Some((tid, _hdr, data))) = scan.next() {
            let payload = if data.len() > 32 { &data[32..] } else { &data };
            if let Some(mut vals) = ferrisdb_storage::row_codec::decode_row(&col_types, payload) {
                // 应用 SET 更新
                for (col_name, new_val) in &updates {
                    if let Some(pos) = meta.columns.iter().position(|c| c.name == *col_name) {
                        vals[pos] = parse_value(&col_types[pos], new_val);
                    }
                }
                rows_to_update.push((tid, vals));
            }
        }

        let mut updated = 0usize;
        for (tid, vals) in &rows_to_update {
            let encoded = encode_row(&col_types, vals);
            if table.update(*tid, &encoded, xid, updated as u32).is_ok() {
                updated += 1;
            }
        }
        txn.commit().map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "XX000".to_owned(), format!("{}", e),
            )))
        })?;

        Ok(vec![Response::Execution(Tag::new("UPDATE").with_rows(updated))])
    }

    /// 处理 DROP TABLE
    async fn handle_drop_table<'a>(&self, sql: &'a str) -> PgWireResult<Vec<Response<'a>>> {
        let table_name = sql[10..].trim().trim_end_matches(';').trim().to_lowercase();
        self.engine.drop_table(&table_name).map_err(|e| {
            PgWireError::UserError(Box::new(pgwire::error::ErrorInfo::new(
                "ERROR".to_owned(), "42P01".to_owned(), format!("{}", e),
            )))
        })?;
        Ok(vec![Response::Execution(Tag::new("DROP TABLE"))])
    }
}

/// 解析值字符串为 Value
fn parse_value(dt: &DataType, s: &str) -> Value {
    if s.to_uppercase() == "NULL" { return Value::Null; }
    match dt {
        DataType::Int32 => s.parse::<i32>().map(Value::Int32).unwrap_or(Value::Null),
        DataType::Int64 => s.parse::<i64>().map(Value::Int64).unwrap_or(Value::Null),
        DataType::Float64 => s.parse::<f64>().map(Value::Float64).unwrap_or(Value::Null),
        DataType::Text => Value::Text(s.to_string()),
        DataType::Boolean => Value::Boolean(s.to_uppercase() == "TRUE" || s == "1"),
        DataType::Bytes => Value::Bytes(s.as_bytes().to_vec()),
        DataType::Timestamp => {
            // 支持 ISO 8601 格式: "2024-01-15 10:30:00" → 微秒 epoch
            // 简化实现：尝试解析常见格式
            s.parse::<i64>().map(Value::Timestamp).unwrap_or(Value::Null)
        }
        DataType::Date => {
            s.parse::<i32>().map(Value::Date).unwrap_or(Value::Null)
        }
        DataType::Float32 => s.parse::<f32>().map(Value::Float32).unwrap_or(Value::Null),
        DataType::Int16 => s.parse::<i16>().map(Value::Int16).unwrap_or(Value::Null),
    }
}

/// pgwire 服务端工厂
pub struct FerrisServerFactory {
    handler: Arc<FerrisQueryHandler>,
}

impl FerrisServerFactory {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self {
            handler: Arc::new(FerrisQueryHandler::new(engine)),
        }
    }
}

impl PgWireServerHandlers for FerrisServerFactory {
    type StartupHandler = FerrisQueryHandler;
    type SimpleQueryHandler = FerrisQueryHandler;
    type ExtendedQueryHandler = PlaceholderExtendedQueryHandler;
    type CopyHandler = NoopCopyHandler;
    type ErrorHandler = NoopErrorHandler;

    fn simple_query_handler(&self) -> Arc<Self::SimpleQueryHandler> {
        self.handler.clone()
    }

    fn extended_query_handler(&self) -> Arc<Self::ExtendedQueryHandler> {
        Arc::new(PlaceholderExtendedQueryHandler)
    }

    fn startup_handler(&self) -> Arc<Self::StartupHandler> {
        self.handler.clone()
    }

    fn copy_handler(&self) -> Arc<Self::CopyHandler> {
        Arc::new(NoopCopyHandler)
    }

    fn error_handler(&self) -> Arc<Self::ErrorHandler> {
        Arc::new(NoopErrorHandler)
    }
}

/// 启动 FerrisDB SQL Server
pub async fn start_server(engine: Arc<Engine>, addr: &str) -> std::io::Result<()> {
    let factory = Arc::new(FerrisServerFactory::new(engine));
    let listener = TcpListener::bind(addr).await?;
    println!("FerrisDB SQL Server listening on {}", addr);

    loop {
        let (socket, peer) = listener.accept().await?;
        println!("  New connection from {}", peer);
        let factory_ref = factory.clone();
        tokio::spawn(async move {
            if let Err(e) = process_socket(socket, None, factory_ref).await {
                eprintln!("  Connection error: {:?}", e);
            }
        });
    }
}
