//! DataFusion TableProvider 实现
//!
//! 将 FerrisDB 的 HeapTable 映射为 DataFusion 的 TableProvider。

use std::any::Any;
use std::sync::Arc;

use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::memory::MemoryExec;

use ferrisdb::Engine;
use ferrisdb_storage::catalog::RelationMeta;
use ferrisdb_storage::row_codec::{decode_row, Value};

use crate::row_to_arrow;

impl std::fmt::Debug for FerrisTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FerrisTable")
            .field("name", &self.meta.name)
            .field("oid", &self.meta.oid)
            .finish()
    }
}

/// FerrisDB 表的 DataFusion 包装
pub struct FerrisTable {
    engine: Arc<Engine>,
    meta: RelationMeta,
    schema: SchemaRef,
}

impl FerrisTable {
    pub fn new(engine: Arc<Engine>, meta: RelationMeta) -> Self {
        let schema = row_to_arrow::to_arrow_schema(&meta.columns);
        Self { engine, meta, schema }
    }
}

#[async_trait]
impl TableProvider for FerrisTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // 同步扫描 FerrisDB HeapTable，收集所有可见行
        let engine = Arc::clone(&self.engine);
        let table_name = self.meta.name.clone();
        let columns = self.meta.columns.clone();
        let col_types: Vec<ferrisdb_storage::catalog::DataType> =
            columns.iter().map(|c| c.data_type).collect();

        // spawn_blocking 桥接同步存储 → async DataFusion
        let rows = tokio::task::spawn_blocking(move || -> Vec<Vec<Value>> {
            let table = match engine.open_table(&table_name) {
                Ok(t) => t,
                Err(_) => return vec![],
            };
            let txn_mgr = engine.txn_manager();
            let txn = match txn_mgr.begin() {
                Ok(t) => t,
                Err(_) => return vec![],
            };

            let mut result = Vec::new();
            let mut scan = ferrisdb_transaction::HeapScan::new(&table);
            while let Ok(Some((_tid, _hdr, data))) = scan.next() {
                // HeapTable::insert 构造 TupleHeader(32B) + data
                // scan 返回的 data = TupleHeader(32B) + 用户数据
                let payload = if data.len() > 32 { &data[32..] } else { &data };
                if let Some(vals) = decode_row(&col_types, payload) {
                    result.push(vals);
                }
            }
            result
        })
        .await
        .map_err(|e| datafusion::error::DataFusionError::Execution(format!("Scan error: {}", e)))?;

        // 转换为 Arrow RecordBatch
        let batch = row_to_arrow::rows_to_batch(&self.meta.columns, &rows)
            .map_err(|e| datafusion::error::DataFusionError::ArrowError(e, None))?;

        let schema = self.schema();
        let batches = if batch.num_rows() > 0 {
            vec![vec![batch]]
        } else {
            vec![vec![]]
        };

        // 用 MemoryExec 包装结果
        let exec = MemoryExec::try_new(&batches, schema, projection.cloned())?;
        Ok(Arc::new(exec))
    }
}
