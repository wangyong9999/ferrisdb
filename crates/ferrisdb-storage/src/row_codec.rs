//! 行数据编解码器
//!
//! 将有类型的列值（Value）编码为字节序列（存入 HeapTable），
//! 并从字节序列解码回列值。
//!
//! 编码格式：
//! ```text
//! [null_bitmap: ceil(N/8) bytes][col0_data][col1_data]...
//! ```
//!
//! 固定长度类型直接存储，变长类型前置 4 字节长度。

use crate::catalog::DataType;

/// 列值
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    /// 空值
    Null,
    /// 32 位有符号整数
    Int32(i32),
    /// 64 位有符号整数
    Int64(i64),
    /// 64 位浮点数
    Float64(f64),
    /// UTF-8 文本
    Text(String),
    /// 布尔值
    Boolean(bool),
    /// 字节数组
    Bytes(Vec<u8>),
    /// 时间戳（微秒精度 UTC epoch）
    Timestamp(i64),
    /// 日期（天数，1970-01-01 起算）
    Date(i32),
    /// 32 位浮点数
    Float32(f32),
    /// 16 位有符号整数
    Int16(i16),
}

impl Value {
    /// 获取值的数据类型（Null 返回 None）
    pub fn data_type(&self) -> Option<DataType> {
        match self {
            Value::Null => None,
            Value::Int32(_) => Some(DataType::Int32),
            Value::Int64(_) => Some(DataType::Int64),
            Value::Float64(_) => Some(DataType::Float64),
            Value::Text(_) => Some(DataType::Text),
            Value::Boolean(_) => Some(DataType::Boolean),
            Value::Bytes(_) => Some(DataType::Bytes),
            Value::Timestamp(_) => Some(DataType::Timestamp),
            Value::Date(_) => Some(DataType::Date),
            Value::Float32(_) => Some(DataType::Float32),
            Value::Int16(_) => Some(DataType::Int16),
        }
    }

    /// 是否为 Null
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// 将一行有类型的值编码为字节序列
///
/// 格式: [null_bitmap][col0][col1]...
/// - null_bitmap: ceil(columns/8) 字节，每列 1 bit（1 = null）
/// - 固定类型: 直接 LE 编码
/// - 变长类型: [len:4 LE][data]
pub fn encode_row(types: &[DataType], values: &[Value]) -> Vec<u8> {
    let ncols = types.len();
    let bitmap_size = (ncols + 7) / 8;
    let mut buf = Vec::with_capacity(bitmap_size + ncols * 8);

    // 先预留 null bitmap 空间
    buf.resize(bitmap_size, 0u8);

    // 编码每列
    for (i, (dt, val)) in types.iter().zip(values.iter()).enumerate() {
        if val.is_null() {
            // 设置 null bit
            buf[i / 8] |= 1 << (i % 8);
            continue;
        }

        match (dt, val) {
            (DataType::Int32, Value::Int32(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            (DataType::Int64, Value::Int64(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            (DataType::Float64, Value::Float64(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            (DataType::Boolean, Value::Boolean(v)) => buf.push(if *v { 1 } else { 0 }),
            (DataType::Text, Value::Text(s)) => {
                buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
                buf.extend_from_slice(s.as_bytes());
            }
            (DataType::Bytes, Value::Bytes(b)) => {
                buf.extend_from_slice(&(b.len() as u32).to_le_bytes());
                buf.extend_from_slice(b);
            }
            (DataType::Timestamp, Value::Timestamp(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            (DataType::Date, Value::Date(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            (DataType::Float32, Value::Float32(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            (DataType::Int16, Value::Int16(v)) => buf.extend_from_slice(&v.to_le_bytes()),
            // 类型不匹配时按 null 处理
            _ => {
                buf[i / 8] |= 1 << (i % 8);
            }
        }
    }

    buf
}

/// 从字节序列解码一行值
///
/// 返回 None 如果数据格式不正确
pub fn decode_row(types: &[DataType], data: &[u8]) -> Option<Vec<Value>> {
    let ncols = types.len();
    if ncols == 0 {
        return Some(vec![]);
    }

    let bitmap_size = (ncols + 7) / 8;
    if data.len() < bitmap_size {
        return None;
    }

    let bitmap = &data[..bitmap_size];
    let mut offset = bitmap_size;
    let mut values = Vec::with_capacity(ncols);

    for (i, dt) in types.iter().enumerate() {
        // 检查 null bit
        if bitmap[i / 8] & (1 << (i % 8)) != 0 {
            values.push(Value::Null);
            continue;
        }

        match dt {
            DataType::Int32 => {
                if offset + 4 > data.len() { return None; }
                let v = i32::from_le_bytes(data[offset..offset+4].try_into().ok()?);
                values.push(Value::Int32(v));
                offset += 4;
            }
            DataType::Int64 => {
                if offset + 8 > data.len() { return None; }
                let v = i64::from_le_bytes(data[offset..offset+8].try_into().ok()?);
                values.push(Value::Int64(v));
                offset += 8;
            }
            DataType::Float64 => {
                if offset + 8 > data.len() { return None; }
                let v = f64::from_le_bytes(data[offset..offset+8].try_into().ok()?);
                values.push(Value::Float64(v));
                offset += 8;
            }
            DataType::Boolean => {
                if offset + 1 > data.len() { return None; }
                values.push(Value::Boolean(data[offset] != 0));
                offset += 1;
            }
            DataType::Text => {
                if offset + 4 > data.len() { return None; }
                let len = u32::from_le_bytes(data[offset..offset+4].try_into().ok()?) as usize;
                offset += 4;
                if offset + len > data.len() { return None; }
                let s = String::from_utf8(data[offset..offset+len].to_vec()).ok()?;
                values.push(Value::Text(s));
                offset += len;
            }
            DataType::Bytes => {
                if offset + 4 > data.len() { return None; }
                let len = u32::from_le_bytes(data[offset..offset+4].try_into().ok()?) as usize;
                offset += 4;
                if offset + len > data.len() { return None; }
                values.push(Value::Bytes(data[offset..offset+len].to_vec()));
                offset += len;
            }
            DataType::Timestamp => {
                if offset + 8 > data.len() { return None; }
                let v = i64::from_le_bytes(data[offset..offset+8].try_into().ok()?);
                values.push(Value::Timestamp(v));
                offset += 8;
            }
            DataType::Date => {
                if offset + 4 > data.len() { return None; }
                let v = i32::from_le_bytes(data[offset..offset+4].try_into().ok()?);
                values.push(Value::Date(v));
                offset += 4;
            }
            DataType::Float32 => {
                if offset + 4 > data.len() { return None; }
                let v = f32::from_le_bytes(data[offset..offset+4].try_into().ok()?);
                values.push(Value::Float32(v));
                offset += 4;
            }
            DataType::Int16 => {
                if offset + 2 > data.len() { return None; }
                let v = i16::from_le_bytes(data[offset..offset+2].try_into().ok()?);
                values.push(Value::Int16(v));
                offset += 2;
            }
        }
    }

    Some(values)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_basic() {
        let types = vec![DataType::Int32, DataType::Text, DataType::Boolean];
        let values = vec![
            Value::Int32(42),
            Value::Text("hello".to_string()),
            Value::Boolean(true),
        ];
        let encoded = encode_row(&types, &values);
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_encode_decode_with_nulls() {
        let types = vec![DataType::Int32, DataType::Text, DataType::Int64];
        let values = vec![Value::Int32(1), Value::Null, Value::Int64(999)];
        let encoded = encode_row(&types, &values);
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded[0], Value::Int32(1));
        assert_eq!(decoded[1], Value::Null);
        assert_eq!(decoded[2], Value::Int64(999));
    }

    #[test]
    fn test_encode_decode_all_types() {
        let types = vec![
            DataType::Int32, DataType::Int64, DataType::Float64,
            DataType::Text, DataType::Boolean, DataType::Bytes,
        ];
        let values = vec![
            Value::Int32(-100),
            Value::Int64(i64::MAX),
            Value::Float64(3.14159),
            Value::Text("日本語テスト".to_string()),
            Value::Boolean(false),
            Value::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        ];
        let encoded = encode_row(&types, &values);
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_encode_decode_all_nulls() {
        let types = vec![DataType::Int32, DataType::Text, DataType::Boolean];
        let values = vec![Value::Null, Value::Null, Value::Null];
        let encoded = encode_row(&types, &values);
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }

    #[test]
    fn test_encode_decode_empty() {
        let encoded = encode_row(&[], &[]);
        let decoded = decode_row(&[], &encoded).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn test_decode_truncated_data() {
        let types = vec![DataType::Int32, DataType::Text];
        let values = vec![Value::Int32(42), Value::Text("hello".to_string())];
        let encoded = encode_row(&types, &values);
        // 截断数据
        assert!(decode_row(&types, &encoded[..3]).is_none());
    }

    #[test]
    fn test_encode_decode_large_text() {
        let types = vec![DataType::Text];
        let big_string = "x".repeat(100_000);
        let values = vec![Value::Text(big_string.clone())];
        let encoded = encode_row(&types, &values);
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded[0], Value::Text(big_string));
    }

    #[test]
    fn test_value_data_type() {
        assert_eq!(Value::Int32(0).data_type(), Some(DataType::Int32));
        assert_eq!(Value::Text("".into()).data_type(), Some(DataType::Text));
        assert_eq!(Value::Null.data_type(), None);
        assert!(Value::Null.is_null());
        assert!(!Value::Int32(0).is_null());
    }

    #[test]
    fn test_many_columns_roundtrip() {
        let types: Vec<DataType> = (0..50).map(|i| {
            match i % 6 {
                0 => DataType::Int32,
                1 => DataType::Int64,
                2 => DataType::Float64,
                3 => DataType::Text,
                4 => DataType::Boolean,
                _ => DataType::Bytes,
            }
        }).collect();
        let values: Vec<Value> = types.iter().enumerate().map(|(i, dt)| {
            if i % 7 == 0 { return Value::Null; }
            match dt {
                DataType::Int32 => Value::Int32(i as i32),
                DataType::Int64 => Value::Int64(i as i64 * 1000),
                DataType::Float64 => Value::Float64(i as f64 * 0.1),
                DataType::Text => Value::Text(format!("val_{}", i)),
                DataType::Boolean => Value::Boolean(i % 2 == 0),
                DataType::Bytes => Value::Bytes(vec![i as u8; 3]),
                DataType::Timestamp => Value::Timestamp(i as i64 * 1_000_000),
                DataType::Date => Value::Date(i as i32 * 100),
                DataType::Float32 => Value::Float32(i as f32 * 0.5),
                DataType::Int16 => Value::Int16(i as i16),
            }
        }).collect();
        let encoded = encode_row(&types, &values);
        let decoded = decode_row(&types, &encoded).unwrap();
        assert_eq!(decoded, values);
    }
}
