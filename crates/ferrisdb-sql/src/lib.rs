//! FerrisDB SQL Server
//!
//! 将 FerrisDB 存储引擎接入 Apache DataFusion SQL 引擎，
//! 通过 pgwire 提供 PostgreSQL 协议兼容的网络服务。
//!
//! # 架构
//!
//! ```text
//! psql/JDBC → pgwire(TCP) → DataFusion(SQL) → FerrisDB(Storage)
//! ```

pub mod catalog;
pub mod table;
pub mod session;
pub mod server;
pub mod row_to_arrow;
