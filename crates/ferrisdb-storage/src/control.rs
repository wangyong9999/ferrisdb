//! Control File — 持久化存储引擎元数据
//!
//! 记录 checkpoint LSN、timeline ID 等关键信息，确保崩溃后恢复从正确位置开始。

use std::fs;
use std::path::{Path, PathBuf};
use ferrisdb_core::{Lsn, Result, FerrisDBError};

/// Control file 数据
#[derive(Debug, Clone)]
pub struct ControlFileData {
    /// 最后一次 checkpoint 的 LSN
    pub checkpoint_lsn: u64,
    /// Timeline ID
    pub timeline_id: u64,
    /// 最后的 WAL 文件号
    pub last_wal_file: u32,
    /// 系统状态 (0=shut down clean, 1=in production, 2=in recovery)
    pub state: u32,
}

impl Default for ControlFileData {
    fn default() -> Self {
        Self {
            checkpoint_lsn: 0,
            timeline_id: 1,
            last_wal_file: 0,
            state: 0,
        }
    }
}

impl ControlFileData {
    /// 序列化为字节（固定 32 字节）
    pub fn to_bytes(&self) -> [u8; 32] {
        let mut buf = [0u8; 32];
        buf[0..8].copy_from_slice(&self.checkpoint_lsn.to_le_bytes());
        buf[8..16].copy_from_slice(&self.timeline_id.to_le_bytes());
        buf[16..20].copy_from_slice(&self.last_wal_file.to_le_bytes());
        buf[20..24].copy_from_slice(&self.state.to_le_bytes());
        // 24..32: reserved / CRC
        let crc = crc32fast::hash(&buf[0..24]);
        buf[24..28].copy_from_slice(&crc.to_le_bytes());
        buf
    }

    /// 从字节反序列化
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 32 { return None; }
        let crc_stored = u32::from_le_bytes([data[24], data[25], data[26], data[27]]);
        let crc_computed = crc32fast::hash(&data[0..24]);
        if crc_stored != crc_computed { return None; }

        Some(Self {
            checkpoint_lsn: u64::from_le_bytes(data[0..8].try_into().ok()?),
            timeline_id: u64::from_le_bytes(data[8..16].try_into().ok()?),
            last_wal_file: u32::from_le_bytes(data[16..20].try_into().ok()?),
            state: u32::from_le_bytes(data[20..24].try_into().ok()?),
        })
    }
}

/// Control File 管理器
pub struct ControlFile {
    path: PathBuf,
    data: parking_lot::RwLock<ControlFileData>,
}

impl ControlFile {
    /// 打开或创建 control file
    pub fn open<P: AsRef<Path>>(data_dir: P) -> Result<Self> {
        let path = data_dir.as_ref().join("dstore_control");
        let data = if path.exists() {
            let bytes = fs::read(&path)
                .map_err(|e| FerrisDBError::Internal(format!("Failed to read control file: {}", e)))?;
            ControlFileData::from_bytes(&bytes)
                .ok_or_else(|| FerrisDBError::Internal("Control file corrupted".to_string()))?
        } else {
            ControlFileData::default()
        };
        Ok(Self { path, data: parking_lot::RwLock::new(data) })
    }

    /// 获取 checkpoint LSN
    pub fn checkpoint_lsn(&self) -> Lsn {
        Lsn::from_raw(self.data.read().checkpoint_lsn)
    }

    /// 更新 checkpoint LSN 并持久化
    pub fn update_checkpoint(&self, lsn: Lsn, wal_file: u32) -> Result<()> {
        {
            let mut d = self.data.write();
            d.checkpoint_lsn = lsn.raw();
            d.last_wal_file = wal_file;
        }
        self.flush()
    }

    /// 标记系统状态
    pub fn set_state(&self, state: u32) -> Result<()> {
        self.data.write().state = state;
        self.flush()
    }

    /// 获取当前数据的拷贝
    pub fn get_data(&self) -> ControlFileData {
        self.data.read().clone()
    }

    /// 持久化到磁盘（原子写: 写临时文件 → rename）
    fn flush(&self) -> Result<()> {
        let bytes = self.data.read().to_bytes();
        let tmp_path = self.path.with_extension("tmp");
        fs::write(&tmp_path, bytes)
            .map_err(|e| FerrisDBError::Internal(format!("Failed to write control file: {}", e)))?;
        fs::rename(&tmp_path, &self.path)
            .map_err(|e| FerrisDBError::Internal(format!("Failed to rename control file: {}", e)))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_control_file_data_roundtrip() {
        let data = ControlFileData {
            checkpoint_lsn: 12345,
            timeline_id: 1,
            last_wal_file: 7,
            state: 1,
        };
        let bytes = data.to_bytes();
        let restored = ControlFileData::from_bytes(&bytes).unwrap();
        assert_eq!(restored.checkpoint_lsn, 12345);
        assert_eq!(restored.last_wal_file, 7);
    }

    #[test]
    fn test_control_file_data_crc_detects_corruption() {
        let data = ControlFileData::default();
        let mut bytes = data.to_bytes();
        bytes[5] ^= 0xFF; // corrupt
        assert!(ControlFileData::from_bytes(&bytes).is_none());
    }

    #[test]
    fn test_control_file_create_and_read() {
        let td = TempDir::new().unwrap();
        {
            let cf = ControlFile::open(td.path()).unwrap();
            cf.update_checkpoint(Lsn::from_raw(9999), 3).unwrap();
        }
        {
            let cf = ControlFile::open(td.path()).unwrap();
            assert_eq!(cf.checkpoint_lsn().raw(), 9999);
            assert_eq!(cf.get_data().last_wal_file, 3);
        }
    }

    #[test]
    fn test_control_file_state() {
        let td = TempDir::new().unwrap();
        let cf = ControlFile::open(td.path()).unwrap();
        cf.set_state(1).unwrap(); // in production
        assert_eq!(cf.get_data().state, 1);
    }
}
