//! FerrisDB SQL Server 入口

use std::path::PathBuf;

#[tokio::main]
async fn main() {
    println!("FerrisDB SQL Server v{}", env!("CARGO_PKG_VERSION"));
    println!("  Storage engine: FerrisDB");
    println!("  SQL engine: Apache DataFusion");
    println!("  Protocol: PostgreSQL wire protocol (pgwire)");
    println!();

    // TODO: Step 5 — 启动 TCP 服务
    let data_dir = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "/tmp/ferrisdb_data".to_string());

    println!("Data directory: {}", data_dir);
    println!("Starting server on 0.0.0.0:5432...");
    println!();
    println!("NOTE: Server implementation pending (Step 5).");
    println!("Use `cargo test -p ferrisdb-sql` to run SQL integration tests.");
}
