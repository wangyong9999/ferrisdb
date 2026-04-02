//! Storage Manager
//!
//! 管理磁盘文件的 I/O 操作。

use ferrisdb_core::{BufferTag, FerrisDBError, Result, Lsn, PdbId};
use ferrisdb_core::PageId;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use parking_lot::RwLock;
use std::collections::HashMap;

use crate::page::PAGE_SIZE;

/// 文件句柄
struct FileHandle {
    /// 文件
    file: Mutex<File>,
    /// 文件路径
    path: PathBuf,
    /// 文件大小（页面数）
    num_pages: std::sync::atomic::AtomicU64,
}

impl FileHandle {
    fn new(path: PathBuf) -> Result<Self> {
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)
            .map_err(|e| FerrisDBError::Io(e))?;

        let metadata = file.metadata().map_err(|e| FerrisDBError::Io(e))?;
        let num_pages = metadata.len() / PAGE_SIZE as u64;

        Ok(Self {
            file: Mutex::new(file),
            path,
            num_pages: std::sync::atomic::AtomicU64::new(num_pages),
        })
    }

    fn read_page(&self, page_no: u32, buf: &mut [u8]) -> Result<()> {
        let offset = page_no as u64 * PAGE_SIZE as u64;
        let file = self.file.lock().map_err(|_| {
            FerrisDBError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to lock file",
            ))
        })?;

        // Use read_exact_at (pread) to avoid seek syscall
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
            .map_err(|e| FerrisDBError::Io(e))?;

        Ok(())
    }

    fn write_page(&self, page_no: u32, buf: &[u8]) -> Result<()> {
        let offset = page_no as u64 * PAGE_SIZE as u64;
        let file = self.file.lock().map_err(|_| {
            FerrisDBError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to lock file",
            ))
        })?;

        // Use write_all_at (pwrite) to avoid seek syscall
        use std::os::unix::fs::FileExt;
        file.write_all_at(buf, offset)
            .map_err(|e| FerrisDBError::Io(e))?;

        // 更新页面数
        let current = self.num_pages.load(std::sync::atomic::Ordering::Acquire);
        if page_no as u64 >= current {
            self.num_pages.store(page_no as u64 + 1, std::sync::atomic::Ordering::Release);
        }

        Ok(())
    }

    fn sync(&self) -> Result<()> {
        let file = self.file.lock().map_err(|_| {
            FerrisDBError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Failed to lock file",
            ))
        })?;
        file.sync_all().map_err(|e| FerrisDBError::Io(e))
    }

    fn num_pages(&self) -> u64 {
        self.num_pages.load(std::sync::atomic::Ordering::Acquire)
    }
}

/// Storage Manager
///
/// 管理磁盘上的存储文件。
pub struct StorageManager {
    /// 数据目录
    data_dir: PathBuf,
    /// 文件句柄缓存 (file_id -> FileHandle)
    files: RwLock<HashMap<u16, Arc<FileHandle>>>,
    /// PDB 目录映射
    pdb_dirs: RwLock<HashMap<u32, PathBuf>>,
    /// 最大打开文件数（FD LRU 限制）
    max_open_files: usize,
}

impl StorageManager {
    /// 创建新的 Storage Manager
    pub fn new<P: AsRef<Path>>(data_dir: P) -> Self {
        Self {
            data_dir: data_dir.as_ref().to_path_buf(),
            files: RwLock::new(HashMap::new()),
            pdb_dirs: RwLock::new(HashMap::new()),
            max_open_files: 256,
        }
    }

    /// 初始化数据目录
    pub fn init(&self) -> Result<()> {
        std::fs::create_dir_all(&self.data_dir)
            .map_err(|e| FerrisDBError::Io(e))?;
        Ok(())
    }

    /// 获取数据目录
    #[inline]
    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    /// 获取文件路径
    fn get_file_path(&self, pdb_id: PdbId, file_id: u16) -> PathBuf {
        let pdb_dir = if pdb_id.raw() == 0 {
            self.data_dir.clone()
        } else {
            self.data_dir.join(format!("pdb_{}", pdb_id.raw()))
        };
        pdb_dir.join(format!("{}.db", file_id))
    }

    /// 获取或打开文件
    fn get_or_open_file(&self, tag: &BufferTag) -> Result<Arc<FileHandle>> {
        let pdb_id = PdbId::new(pdb_id_from_tag(tag));
        let file_id = file_id_from_tag(tag);

        // 先检查缓存
        {
            let files = self.files.read();
            if let Some(handle) = files.get(&file_id) {
                return Ok(Arc::clone(handle));
            }
        }

        // 需要打开文件
        let mut files = self.files.write();

        // 双重检查
        if let Some(handle) = files.get(&file_id) {
            return Ok(Arc::clone(handle));
        }

        let path = self.get_file_path(pdb_id, file_id);

        // 确保目录存在
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|e| FerrisDBError::Io(e))?;
        }

        // FD LRU 淘汰：如果打开文件数超过限制，关闭最不常用的
        if files.len() >= self.max_open_files {
            // 简单策略：移除第一个非当前文件
            let evict_key = files.keys().find(|&&k| k != file_id).copied();
            if let Some(key) = evict_key {
                files.remove(&key);
            }
        }

        let handle = Arc::new(FileHandle::new(path)?);
        files.insert(file_id, Arc::clone(&handle));

        Ok(handle)
    }

    /// 读取页面
    pub fn read_page(&self, tag: &BufferTag, buf: &mut [u8]) -> Result<()> {
        debug_assert!(buf.len() == PAGE_SIZE);

        let handle = self.get_or_open_file(tag)?;
        let block_no = block_no_from_tag(tag);

        handle.read_page(block_no, buf)
    }

    /// 写入页面
    pub fn write_page(&self, tag: &BufferTag, buf: &[u8]) -> Result<()> {
        debug_assert!(buf.len() == PAGE_SIZE);

        let handle = self.get_or_open_file(tag)?;
        let block_no = block_no_from_tag(tag);

        handle.write_page(block_no, buf)
    }

    /// 同步文件
    pub fn sync(&self, tag: &BufferTag) -> Result<()> {
        let handle = self.get_or_open_file(tag)?;
        handle.sync()
    }

    /// 同步所有文件
    pub fn sync_all(&self) -> Result<()> {
        let files = self.files.read();
        for handle in files.values() {
            handle.sync()?;
        }
        Ok(())
    }

    /// 获取文件页面数
    pub fn num_pages(&self, tag: &BufferTag) -> Result<u64> {
        let handle = self.get_or_open_file(tag)?;
        Ok(handle.num_pages())
    }

    /// 扩展文件（分配新页面）
    pub fn extend(&self, tag: &BufferTag) -> Result<u32> {
        let handle = self.get_or_open_file(tag)?;
        let new_page = handle.num_pages() as u32;

        // 写入空页面
        let zero_page = vec![0u8; PAGE_SIZE];
        handle.write_page(new_page, &zero_page)?;

        Ok(new_page)
    }

    /// 通过 PageId 读取页面（用于 WAL recovery）
    ///
    /// PageId (tablespace, database, relation, block) 映射到文件路径：
    ///   data_dir/pdb_{database}/{relation}.db 的第 block 页
    pub fn read_page_by_id(&self, page_id: &PageId, buf: &mut [u8]) -> Result<()> {
        debug_assert!(buf.len() == PAGE_SIZE);

        let file_id = page_id.relation as u16;
        let block_no = page_id.block;
        let pdb_id = PdbId::new(page_id.database);

        // 获取或创建文件句柄
        let handle = {
            // 先检查缓存
            {
                let files = self.files.read();
                if let Some(h) = files.get(&file_id) {
                    let h = Arc::clone(h);
                    drop(files);
                    return h.read_page(block_no, buf);
                }
            }

            let mut files = self.files.write();
            if let Some(h) = files.get(&file_id) {
                Arc::clone(h)
            } else {
                let path = self.get_file_path(pdb_id, file_id);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| FerrisDBError::Io(e))?;
                }
                let handle = Arc::new(FileHandle::new(path)?);
                files.insert(file_id, Arc::clone(&handle));
                handle
            }
        };

        handle.read_page(block_no, buf)
    }

    /// 通过 PageId 写入页面（用于 WAL recovery）
    pub fn write_page_by_id(&self, page_id: &PageId, buf: &[u8]) -> Result<()> {
        debug_assert!(buf.len() == PAGE_SIZE);

        let file_id = page_id.relation as u16;
        let block_no = page_id.block;
        let pdb_id = PdbId::new(page_id.database);

        let handle = {
            {
                let files = self.files.read();
                if let Some(h) = files.get(&file_id) {
                    let h = Arc::clone(h);
                    drop(files);
                    return h.write_page(block_no, buf);
                }
            }

            let mut files = self.files.write();
            if let Some(h) = files.get(&file_id) {
                Arc::clone(h)
            } else {
                let path = self.get_file_path(pdb_id, file_id);
                if let Some(parent) = path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| FerrisDBError::Io(e))?;
                }
                let handle = Arc::new(FileHandle::new(path)?);
                files.insert(file_id, Arc::clone(&handle));
                handle
            }
        };

        handle.write_page(block_no, buf)
    }
}

/// 从 BufferTag 提取 PDB ID
fn pdb_id_from_tag(tag: &BufferTag) -> u32 {
    // BufferTag 包含 pdb_id 字段
    // 简化实现：返回 0
    0
}

/// 从 BufferTag 提取 File ID
fn file_id_from_tag(tag: &BufferTag) -> u16 {
    tag.file_id()
}

/// 从 BufferTag 提取 Block No
fn block_no_from_tag(tag: &BufferTag) -> u32 {
    tag.block_id()
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrisdb_core::{FileTag, PdbId};
    use tempfile::TempDir;

    #[test]
    fn test_storage_manager_new() {
        let temp_dir = TempDir::new().unwrap();
        let smgr = StorageManager::new(temp_dir.path());
        assert_eq!(smgr.data_dir(), temp_dir.path());
    }

    #[test]
    fn test_storage_manager_read_write() {
        let temp_dir = TempDir::new().unwrap();
        let smgr = StorageManager::new(temp_dir.path());
        smgr.init().unwrap();

        let tag = BufferTag::new(PdbId::new(0), 1, 0);

        // 写入页面
        let mut page_data = vec![0u8; PAGE_SIZE];
        for (i, byte) in page_data.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        smgr.write_page(&tag, &page_data).unwrap();

        // 读取页面
        let mut read_buf = vec![0u8; PAGE_SIZE];
        smgr.read_page(&tag, &mut read_buf).unwrap();

        assert_eq!(page_data, read_buf);
    }

    #[test]
    fn test_storage_manager_extend() {
        let temp_dir = TempDir::new().unwrap();
        let smgr = StorageManager::new(temp_dir.path());
        smgr.init().unwrap();

        let tag = BufferTag::new(PdbId::new(0), 1, 0);

        let new_page = smgr.extend(&tag).unwrap();
        assert_eq!(new_page, 0);

        let new_page2 = smgr.extend(&tag).unwrap();
        assert_eq!(new_page2, 1);
    }
}
