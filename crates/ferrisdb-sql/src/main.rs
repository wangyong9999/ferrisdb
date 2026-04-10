//! FerrisDB SQL Server 入口

use std::sync::Arc;
use std::path::PathBuf;
use ferrisdb::{Engine, EngineConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("FerrisDB SQL Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  Storage: FerrisDB (Rust)");
    println!("  SQL:     Apache DataFusion");
    println!("  Protocol: PostgreSQL wire protocol");
    println!();

    let data_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/ferrisdb_data".to_string());
    let port = std::env::args()
        .nth(2)
        .unwrap_or_else(|| "5433".to_string());

    let addr = format!("0.0.0.0:{}", port);

    // 启动存储引擎
    std::fs::create_dir_all(&data_dir)?;
    let engine = Engine::open(EngineConfig {
        data_dir: PathBuf::from(&data_dir),
        shared_buffers: 10000,
        wal_enabled: true,
        ..Default::default()
    })?;
    println!("Data directory: {}", data_dir);

    let engine = Arc::new(engine);

    // 启动 SQL Server
    println!();
    println!("Connect with: psql -h 127.0.0.1 -p {}", port);
    println!();

    ferrisdb_sql::server::start_server(engine, &addr).await?;

    Ok(())
}
