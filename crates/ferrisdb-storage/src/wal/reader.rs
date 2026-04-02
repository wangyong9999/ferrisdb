//! WAL Reader
//!
//! 读取 WAL 记录。

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;

use ferrisdb_core::{Lsn, FerrisDBError, Result};
use ferrisdb_core::atomic::Ordering;

use super::record::{WalRecordHeader, WalRecordType};
use super::writer::WalFileHeader;

/// WAL Reader
///
/// 用于读取 WAL 文件中的记录。
pub struct WalReader {
    /// WAL 文件路径
    file_path: PathBuf,
    /// 文件句柄
    file: Option<File>,
    /// 当前文件号
    file_no: u32,
    /// 当前偏移量
    offset: u64,
    /// 文件结束位置
    end_offset: u64,
}

impl WalReader {
    /// 创建新的 Reader
    pub fn new<P: Into<PathBuf>>(path: P) -> Self {
        Self {
            file_path: path.into(),
            file: None,
            file_no: 0,
            offset: 0,
            end_offset: 0,
        }
    }

    /// 打开指定的 WAL 文件
    pub fn open(&mut self, file_no: u32) -> Result<()> {
        let file_name = format!("{:08X}.wal", file_no);
        let path = self.file_path.join(file_name);

        let mut file = File::open(&path)
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to open WAL file: {}", e)
            )))?;

        // 读取文件头部
        let mut header_buf = [0u8; WalFileHeader::size()];
        file.read_exact(&mut header_buf)
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to read WAL header: {}", e)
            )))?;

        let header = WalFileHeader::from_bytes(&header_buf);

        // 验证魔数
        if header.magic != super::writer::WAL_FILE_MAGIC {
            return Err(FerrisDBError::Wal(ferrisdb_core::error::WalError::Corruption));
        }

        // 获取文件大小
        let metadata = file.metadata()
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to get file metadata: {}", e)
            )))?;
        self.end_offset = metadata.len();

        self.file = Some(file);
        self.file_no = file_no;
        self.offset = WalFileHeader::size() as u64;

        Ok(())
    }

    /// 定位到指定偏移量
    pub fn seek(&mut self, offset: u64) -> Result<()> {
        if let Some(ref mut file) = self.file {
            file.seek(SeekFrom::Start(offset))
                .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                    format!("Failed to seek WAL file: {}", e)
                )))?;
            self.offset = offset;
            Ok(())
        } else {
            Err(FerrisDBError::InvalidState("WAL file not opened".to_string()))
        }
    }

    /// 读取下一条记录
    pub fn read_next(&mut self) -> Result<Option<(Lsn, Vec<u8>)>> {
        if self.file.is_none() {
            return Err(FerrisDBError::InvalidState("WAL file not opened".to_string()));
        }

        let file = self.file.as_mut().unwrap();

        // 检查是否到达文件末尾
        if self.offset >= self.end_offset {
            return Ok(None);
        }

        // 读取记录头部
        let mut header_buf = [0u8; std::mem::size_of::<WalRecordHeader>()];
        match file.read_exact(&mut header_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                return Ok(None);
            }
            Err(e) => {
                return Err(FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                    format!("Failed to read record header: {}", e)
                )));
            }
        }

        let header = unsafe {
            std::ptr::read(header_buf.as_ptr() as *const WalRecordHeader)
        };

        // 读取记录数据
        let data_size = header.size as usize;
        let mut data = vec![0u8; data_size];
        file.read_exact(&mut data)
            .map_err(|e| FerrisDBError::Wal(ferrisdb_core::error::WalError::ReadFailed(
                format!("Failed to read record data: {}", e)
            )))?;

        // 构造完整的记录
        let mut record = Vec::with_capacity(header_buf.len() + data_size);
        record.extend_from_slice(&header_buf);
        record.extend_from_slice(&data);

        // 计算 LSN
        let lsn = Lsn::from_parts(self.file_no, self.offset as u32);

        // 更新偏移量
        self.offset += (header_buf.len() + data_size) as u64;

        Ok(Some((lsn, record)))
    }

    /// 获取当前 LSN
    #[inline]
    pub fn current_lsn(&self) -> Lsn {
        Lsn::from_parts(self.file_no, self.offset as u32)
    }

    /// 获取当前偏移量
    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// 检查是否到达末尾
    #[inline]
    pub fn is_eof(&self) -> bool {
        self.offset >= self.end_offset
    }

    /// 关闭 Reader
    pub fn close(&mut self) {
        self.file = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_reader_open() {
        let temp_dir = TempDir::new().unwrap();
        let mut reader = WalReader::new(temp_dir.path());

        // 尝试打开不存在的文件应该失败
        assert!(reader.open(999).is_err());
    }
}
