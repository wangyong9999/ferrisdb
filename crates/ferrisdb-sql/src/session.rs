//! 会话管理 — DataFusion SessionContext + FerrisDB Engine

use std::sync::Arc;
use datafusion::prelude::*;
use ferrisdb::Engine;
use crate::catalog::FerrisCatalog;

/// 创建绑定到 FerrisDB Engine 的 DataFusion SessionContext
pub fn create_session(engine: Arc<Engine>) -> SessionContext {
    let ctx = SessionContext::new();

    // 注册 FerrisDB catalog 为默认 catalog
    let catalog = Arc::new(FerrisCatalog::new(engine));
    ctx.register_catalog("ferrisdb", catalog);

    // 设置默认 catalog 和 schema
    // DataFusion 默认用 "datafusion" catalog，我们覆盖它
    ctx
}
