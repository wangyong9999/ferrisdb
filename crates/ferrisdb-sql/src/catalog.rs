//! DataFusion CatalogProvider + SchemaProvider 实现
//!
//! 将 FerrisDB 的 SystemCatalog 映射到 DataFusion 的 catalog 体系。

use std::any::Any;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::TableProvider;
use datafusion::error::Result;

use ferrisdb::Engine;

use crate::table::FerrisTable;

// Engine 没有 derive(Debug)，手动实现
impl std::fmt::Debug for FerrisCatalog {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FerrisCatalog").finish()
    }
}

impl std::fmt::Debug for FerrisSchema {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FerrisSchema").finish()
    }
}

/// FerrisDB 的 DataFusion CatalogProvider
pub struct FerrisCatalog {
    engine: Arc<Engine>,
}

impl FerrisCatalog {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

impl CatalogProvider for FerrisCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        vec!["public".to_string()]
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        if name == "public" {
            Some(Arc::new(FerrisSchema::new(Arc::clone(&self.engine))))
        } else {
            None
        }
    }
}

/// FerrisDB 的 DataFusion SchemaProvider
pub struct FerrisSchema {
    engine: Arc<Engine>,
}

impl FerrisSchema {
    pub fn new(engine: Arc<Engine>) -> Self {
        Self { engine }
    }
}

#[async_trait]
impl SchemaProvider for FerrisSchema {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.engine.catalog().list_tables()
            .into_iter()
            .map(|m| m.name)
            .collect()
    }

    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>> {
        let meta = match self.engine.catalog().lookup_by_name(name) {
            Some(m) => m,
            None => return Ok(None),
        };

        if meta.columns.is_empty() {
            // 无 Schema 的表不能通过 SQL 访问
            return Ok(None);
        }

        let table = FerrisTable::new(Arc::clone(&self.engine), meta);
        Ok(Some(Arc::new(table)))
    }

    fn table_exist(&self, name: &str) -> bool {
        self.engine.catalog().lookup_by_name(name).is_some()
    }
}
