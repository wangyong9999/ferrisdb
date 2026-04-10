//! FerrisDB Row ↔ Arrow RecordBatch 转换

use arrow::array::*;
use arrow::datatypes::{DataType as ArrowDataType, Field, Schema as ArrowSchema, SchemaRef};
use arrow::record_batch::RecordBatch;
use ferrisdb_storage::catalog::{ColumnDef, DataType};
use ferrisdb_storage::row_codec::Value;
use std::sync::Arc;

/// FerrisDB DataType → Arrow DataType
pub fn to_arrow_type(dt: &DataType) -> ArrowDataType {
    match dt {
        DataType::Int32 => ArrowDataType::Int32,
        DataType::Int64 => ArrowDataType::Int64,
        DataType::Float64 => ArrowDataType::Float64,
        DataType::Text => ArrowDataType::Utf8,
        DataType::Boolean => ArrowDataType::Boolean,
        DataType::Bytes => ArrowDataType::Binary,
        DataType::Timestamp => ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
        DataType::Date => ArrowDataType::Date32,
        DataType::Float32 => ArrowDataType::Float32,
        DataType::Int16 => ArrowDataType::Int16,
    }
}

/// FerrisDB ColumnDef 列表 → Arrow Schema
pub fn to_arrow_schema(columns: &[ColumnDef]) -> SchemaRef {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, to_arrow_type(&c.data_type), c.nullable))
        .collect();
    Arc::new(ArrowSchema::new(fields))
}

/// 多行 FerrisDB Value → Arrow RecordBatch
///
/// `rows`: 每行是 Vec<Value>，列数必须与 columns 一致
pub fn rows_to_batch(columns: &[ColumnDef], rows: &[Vec<Value>]) -> Result<RecordBatch, arrow::error::ArrowError> {
    let schema = to_arrow_schema(columns);
    let ncols = columns.len();

    if rows.is_empty() {
        return Ok(RecordBatch::new_empty(schema));
    }

    let arrays: Vec<Arc<dyn Array>> = (0..ncols)
        .map(|col_idx| {
            let dt = &columns[col_idx].data_type;
            build_array(dt, rows, col_idx)
        })
        .collect();

    RecordBatch::try_new(schema, arrays)
}

/// 从 Arrow RecordBatch 的一行提取 FerrisDB Value 列表
pub fn batch_row_to_values(batch: &RecordBatch, row_idx: usize, col_types: &[DataType]) -> Vec<Value> {
    let mut values = Vec::with_capacity(batch.num_columns());
    for (col_idx, dt) in col_types.iter().enumerate() {
        let col = batch.column(col_idx);
        if col.is_null(row_idx) {
            values.push(Value::Null);
            continue;
        }
        let v = match dt {
            DataType::Int32 => {
                Value::Int32(col.as_any().downcast_ref::<Int32Array>().unwrap().value(row_idx))
            }
            DataType::Int64 => {
                Value::Int64(col.as_any().downcast_ref::<Int64Array>().unwrap().value(row_idx))
            }
            DataType::Float64 => {
                Value::Float64(col.as_any().downcast_ref::<Float64Array>().unwrap().value(row_idx))
            }
            DataType::Text => {
                Value::Text(col.as_any().downcast_ref::<StringArray>().unwrap().value(row_idx).to_string())
            }
            DataType::Boolean => {
                Value::Boolean(col.as_any().downcast_ref::<BooleanArray>().unwrap().value(row_idx))
            }
            DataType::Bytes => {
                Value::Bytes(col.as_any().downcast_ref::<BinaryArray>().unwrap().value(row_idx).to_vec())
            }
            DataType::Timestamp => {
                Value::Timestamp(col.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap().value(row_idx))
            }
            DataType::Date => {
                Value::Date(col.as_any().downcast_ref::<Date32Array>().unwrap().value(row_idx))
            }
            DataType::Float32 => {
                Value::Float32(col.as_any().downcast_ref::<Float32Array>().unwrap().value(row_idx))
            }
            DataType::Int16 => {
                Value::Int16(col.as_any().downcast_ref::<Int16Array>().unwrap().value(row_idx))
            }
        };
        values.push(v);
    }
    values
}

/// 构建单列的 Arrow Array
fn build_array(dt: &DataType, rows: &[Vec<Value>], col_idx: usize) -> Arc<dyn Array> {
    match dt {
        DataType::Int32 => {
            let arr: Int32Array = rows.iter().map(|row| {
                match &row[col_idx] {
                    Value::Int32(v) => Some(*v),
                    _ => None,
                }
            }).collect();
            Arc::new(arr)
        }
        DataType::Int64 => {
            let arr: Int64Array = rows.iter().map(|row| {
                match &row[col_idx] {
                    Value::Int64(v) => Some(*v),
                    _ => None,
                }
            }).collect();
            Arc::new(arr)
        }
        DataType::Float64 => {
            let arr: Float64Array = rows.iter().map(|row| {
                match &row[col_idx] {
                    Value::Float64(v) => Some(*v),
                    _ => None,
                }
            }).collect();
            Arc::new(arr)
        }
        DataType::Text => {
            let arr: StringArray = rows.iter().map(|row| {
                match &row[col_idx] {
                    Value::Text(s) => Some(s.as_str()),
                    _ => None,
                }
            }).collect();
            Arc::new(arr)
        }
        DataType::Boolean => {
            let arr: BooleanArray = rows.iter().map(|row| {
                match &row[col_idx] {
                    Value::Boolean(v) => Some(*v),
                    _ => None,
                }
            }).collect();
            Arc::new(arr)
        }
        DataType::Bytes => {
            let arr: BinaryArray = rows.iter().map(|row| {
                match &row[col_idx] {
                    Value::Bytes(b) => Some(b.as_slice()),
                    _ => None,
                }
            }).collect();
            Arc::new(arr)
        }
        DataType::Timestamp => {
            let arr: TimestampMicrosecondArray = rows.iter().map(|row| {
                match &row[col_idx] {
                    Value::Timestamp(v) => Some(*v),
                    _ => None,
                }
            }).collect();
            Arc::new(arr)
        }
        DataType::Date => {
            let arr: Date32Array = rows.iter().map(|row| {
                match &row[col_idx] {
                    Value::Date(v) => Some(*v),
                    _ => None,
                }
            }).collect();
            Arc::new(arr)
        }
        DataType::Float32 => {
            let arr: Float32Array = rows.iter().map(|row| {
                match &row[col_idx] {
                    Value::Float32(v) => Some(*v),
                    _ => None,
                }
            }).collect();
            Arc::new(arr)
        }
        DataType::Int16 => {
            let arr: Int16Array = rows.iter().map(|row| {
                match &row[col_idx] {
                    Value::Int16(v) => Some(*v),
                    _ => None,
                }
            }).collect();
            Arc::new(arr)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols() -> Vec<ColumnDef> {
        vec![
            ColumnDef::new("id", DataType::Int32, false, 0),
            ColumnDef::new("name", DataType::Text, true, 1),
            ColumnDef::new("score", DataType::Float64, true, 2),
        ]
    }

    #[test]
    fn test_to_arrow_schema() {
        let schema = to_arrow_schema(&cols());
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "id");
        assert_eq!(*schema.field(0).data_type(), ArrowDataType::Int32);
        assert!(!schema.field(0).is_nullable());
        assert_eq!(schema.field(1).name(), "name");
        assert!(schema.field(1).is_nullable());
    }

    #[test]
    fn test_rows_to_batch() {
        let rows = vec![
            vec![Value::Int32(1), Value::Text("Alice".into()), Value::Float64(95.5)],
            vec![Value::Int32(2), Value::Text("Bob".into()), Value::Null],
        ];
        let batch = rows_to_batch(&cols(), &rows).unwrap();
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 3);

        let ids = batch.column(0).as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(ids.value(0), 1);
        assert_eq!(ids.value(1), 2);

        let names = batch.column(1).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(names.value(0), "Alice");

        assert!(batch.column(2).is_null(1)); // Bob's score is null
    }

    #[test]
    fn test_rows_to_batch_empty() {
        let batch = rows_to_batch(&cols(), &[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_batch_row_to_values() {
        let rows = vec![
            vec![Value::Int32(42), Value::Text("test".into()), Value::Float64(3.14)],
        ];
        let batch = rows_to_batch(&cols(), &rows).unwrap();
        let types: Vec<DataType> = cols().iter().map(|c| c.data_type).collect();
        let vals = batch_row_to_values(&batch, 0, &types);
        assert_eq!(vals[0], Value::Int32(42));
        assert_eq!(vals[1], Value::Text("test".into()));
        assert_eq!(vals[2], Value::Float64(3.14));
    }

    #[test]
    fn test_roundtrip_all_types() {
        let columns = vec![
            ColumnDef::new("a", DataType::Int32, true, 0),
            ColumnDef::new("b", DataType::Int64, true, 1),
            ColumnDef::new("c", DataType::Float64, true, 2),
            ColumnDef::new("d", DataType::Text, true, 3),
            ColumnDef::new("e", DataType::Boolean, true, 4),
            ColumnDef::new("f", DataType::Bytes, true, 5),
        ];
        let rows = vec![vec![
            Value::Int32(-1),
            Value::Int64(i64::MAX),
            Value::Float64(2.718),
            Value::Text("日本語".into()),
            Value::Boolean(true),
            Value::Bytes(vec![0xDE, 0xAD]),
        ]];
        let batch = rows_to_batch(&columns, &rows).unwrap();
        let types: Vec<DataType> = columns.iter().map(|c| c.data_type).collect();
        let vals = batch_row_to_values(&batch, 0, &types);
        assert_eq!(vals, rows[0]);
    }
}
