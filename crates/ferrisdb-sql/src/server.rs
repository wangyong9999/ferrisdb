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
