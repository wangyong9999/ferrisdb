//! WAL Writer
//!
//! 负责将 WAL 记录写入磁盘文件。

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use ferrisdb_core::{Lsn, FerrisDBError, Result};
use ferrisdb_core::atomic::{AtomicU32, AtomicU64, Ordering};
use ferrisdb_core::error::WalError;
use parking_lot::Mutex;

use super::record::{WalRecordHeader, WalRecordType};

/// WAL 文件大小（16MB）
pub const WAL_FILE_SIZE: u64 = 16 * 1024 * 1024;

/// WAL 文件头部魔数
pub const WAL_FILE_MAGIC: u32 = 0xD2A8F347;

/// WAL 文件头部
#[derive(Debug, Clone, Copy)]
#[repr(C, packed)]
pub struct WalFileHeader {
    /// CRC
    pub crc: u32,
    /// 版本号
    pub version: u32,
    /// 起始 PLSN
    pub start_plsn: u64,
    /// 文件大小
    pub file_size: u64,
    /// 时间线 ID
    pub timeline_id: u64,
    /// 最后记录剩余长度
    pub last_record_remain_len: u32,
    /// 魔数
    pub magic: u32,
}

impl WalFileHeader {
    /// 创建新的文件头部
    pub fn new(start_plsn: u64, timeline_id: u64) -> Self {
        Self {
            crc: 0,
            version: 1,
            start_plsn,
            file_size: WAL_FILE_SIZE,
            timeline_id,
            last_record_remain_len: 0,
            magic: WAL_FILE_MAGIC,
        }
    }

    /// 头部大小
    #[inline]
    pub const fn size() -> usize {
        std::mem::size_of::<Self>()
    }

    /// 序列化为字节
    pub fn to_bytes(&self) -> [u8; Self::size()] {
        unsafe { std::mem::transmute_copy(self) }
    }

    /// 从字节反序列化
    pub fn from_bytes(bytes: &[u8]) -> Self {
        assert!(bytes.len() >= Self::size());
        unsafe { std::ptr::read(bytes.as_ptr() as *const Self) }
    }
}

const _: () = assert!(std::mem::size_of::<WalFileHeader>() == 40);

/// WAL Writer
///
/// 负责将 WAL 记录写入磁盘文件。
pub struct WalWriter {
    /// WAL 目录
    wal_dir: PathBuf,
    /// 当前文件号
    file_no: AtomicU32,
    /// 当前偏移量（= 最新写入的 LSN offset）
    offset: AtomicU64,
    /// 时间线 ID
    timeline_id: AtomicU64,
    /// 当前文件（Mutex 仅保护打开/关闭/sync）
    current_file: Mutex<Option<File>>,
    /// 当前文件的 raw fd（缓存，用于无锁 pwrite）
    #[cfg(unix)]
    current_fd: std::sync::atomic::AtomicI32,
    /// 已 fsync 到磁盘的 LSN（由 sync() 更新）
    flushed_lsn: AtomicU64,
    /// Condvar: flusher 完成 sync 后通知等待的 committers
    flush_notify: (std::sync::Mutex<()>, std::sync::Condvar),
}

impl WalWriter {
    /// 创建新的 WAL Writer
    pub fn new<P: AsRef<Path>>(wal_dir: P) -> Self {
        Self {
            wal_dir: wal_dir.as_ref().to_path_buf(),
            file_no: AtomicU32::new(0),
            offset: AtomicU64::new(WalFileHeader::size() as u64),
            timeline_id: AtomicU64::new(1),
            current_file: Mutex::new(None),
            #[cfg(unix)]
            current_fd: std::sync::atomic::AtomicI32::new(-1),
            flushed_lsn: AtomicU64::new(0),
            flush_notify: (std::sync::Mutex::new(()), std::sync::Condvar::new()),
        }
    }

    /// 获取 WAL 目录路径
    pub fn wal_dir(&self) -> &Path {
        &self.wal_dir
    }

    /// 获取当前 LSN
    #[inline]
    pub fn current_lsn(&self) -> Lsn {
        let file_no = self.file_no.load(Ordering::Acquire);
        let offset = self.offset.load(Ordering::Acquire);
        Lsn::from_parts(file_no, offset as u32)
    }

    /// 获取当前文件号
    #[inline]
    pub fn file_no(&self) -> u32 {
        self.file_no.load(Ordering::Acquire)
    }

    /// 获取当前偏移量
    #[inline]
    pub fn offset(&self) -> u64 {
        self.offset.load(Ordering::Acquire)
    }

    /// 确保 WAL 目录存在
    fn ensure_dir(&self) -> Result<()> {
        if !self.wal_dir.exists() {
            std::fs::create_dir_all(&self.wal_dir)
                .map_err(|e| FerrisDBError::Wal(WalError::WriteFailed(
                    format!("Failed to create WAL dir: {}", e)
                )))?;
        }
        Ok(())
    }

    /// 获取 WAL 文件路径
    fn get_file_path(&self, file_no: u32) -> PathBuf {
        self.wal_dir.join(format!("{:08X}.wal", file_no))
    }

    /// 打开或创建当前 WAL 文件
    fn ensure_file(&self) -> Result<File> {
        self.ensure_dir()?;

        let file_no = self.file_no.load(Ordering::Acquire);
        let file_path = self.get_file_path(file_no);

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&file_path)
            .map_err(|e| FerrisDBError::Wal(WalError::WriteFailed(
                format!("Failed to open WAL file: {}", e)
            )))?;

        // 检查文件是否是新的
        let metadata = file.metadata()
            .map_err(|e| FerrisDBError::Wal(WalError::WriteFailed(
                format!("Failed to get file metadata: {}", e)
            )))?;

        if metadata.len() == 0 {
            // 新文件，写入头部
            let header = WalFileHeader::new(
                self.offset.load(Ordering::Acquire),
                self.timeline_id.load(Ordering::Acquire),
            );
            let bytes = header.to_bytes();
            file.write_all(&bytes)
                .map_err(|e| FerrisDBError::Wal(WalError::WriteFailed(
                    format!("Failed to write WAL header: {}", e)
                )))?;
        }

        // 定位到文件末尾
        let current_offset = self.offset.load(Ordering::Acquire);
        file.seek(SeekFrom::Start(current_offset))
            .map_err(|e| FerrisDBError::Wal(WalError::WriteFailed(
                format!("Failed to seek WAL file: {}", e)
            )))?;

        Ok(file)
    }

    /// 写入数据
    ///
    /// Mutex 保护文件 I/O（确保 seek+write 原子），但关键路径短。
    /// 使用 buffered write (OS page cache) 避免 syscall 开销。
    pub fn write(&self, data: &[u8]) -> Result<Lsn> {
        let data_len = data.len() as u64;

        // 获取写锁
        let mut file_guard = self.current_file.lock();

        let current_offset = self.offset.load(Ordering::Acquire);

        // 检查是否需要切换文件
        if current_offset + data_len > WAL_FILE_SIZE {
            drop(file_guard);
            self.switch_file()?;
            return self.write(data);
        }

        // LSN = 写入前的 offset
        let file_no = self.file_no.load(Ordering::Acquire);
        let lsn = Lsn::from_parts(file_no, current_offset as u32);

        // 确保文件已打开
        if file_guard.is_none() {
            *file_guard = Some(self.ensure_file()?);
        }
        let file = file_guard.as_mut().unwrap();

        // 计算 CRC 并单次 write_all（避免分段写入在 crash 间隙产生不一致记录）
        // WalRecordHeader 布局: [size:2][rtype:2][crc:4]
        let map_err = |e: std::io::Error| FerrisDBError::Wal(WalError::WriteFailed(
            format!("Failed to write WAL data: {}", e)
        ));
        if data.len() >= 8 {
            let mut hasher = crc32fast::Hasher::new();
            hasher.update(&data[0..4]);  // size + rtype
            hasher.update(&data[8..]);   // payload
            let crc = hasher.finalize();

            // 构造完整记录：[size:2][rtype:2][crc:4][payload...]
            // 栈上小缓冲区避免大多数情况的堆分配
            let total_len = data.len();
            if total_len <= 512 {
                // 小记录：栈上缓冲区
                let mut buf = [0u8; 512];
                buf[0..4].copy_from_slice(&data[0..4]);
                buf[4..8].copy_from_slice(&crc.to_le_bytes());
                buf[8..total_len].copy_from_slice(&data[8..]);
                file.write_all(&buf[..total_len]).map_err(map_err)?;
            } else {
                // 大记录：堆分配
                let mut buf = Vec::with_capacity(total_len);
                buf.extend_from_slice(&data[0..4]);
                buf.extend_from_slice(&crc.to_le_bytes());
                buf.extend_from_slice(&data[8..]);
                file.write_all(&buf).map_err(map_err)?;
            }
        } else {
            file.write_all(data).map_err(map_err)?;
        }

        // 更新偏移量
        self.offset.fetch_add(data_len, Ordering::AcqRel);

        Ok(lsn)
    }

    /// 切换到新的 WAL 文件
    pub fn switch_file(&self) -> Result<()> {
        // 同步当前文件
        self.sync()?;

        // 关闭当前文件
        {
            let mut file_guard = self.current_file.lock();
            *file_guard = None;
            #[cfg(unix)]
            self.current_fd.store(-1, std::sync::atomic::Ordering::Release);
        }

        // 增加文件号
        self.file_no.fetch_add(1, Ordering::AcqRel);

        // 重置偏移量
        self.offset.store(WalFileHeader::size() as u64, Ordering::Release);

        // 创建新文件
        let file = self.ensure_file()?;
        {
            let mut file_guard = self.current_file.lock();
            *file_guard = Some(file);
        }

        Ok(())
    }

    /// 同步刷新到磁盘，更新 flushed_lsn，通知等待者
    pub fn sync(&self) -> Result<()> {
        let file_guard = self.current_file.lock();
        if let Some(ref file) = *file_guard {
            file.sync_data()
                .map_err(|e| FerrisDBError::Wal(WalError::WriteFailed(
                    format!("Failed to sync WAL file: {}", e)
                )))?;
        }
        // 更新已刷盘 LSN
        let current = self.current_lsn().raw();
        self.flushed_lsn.store(current, Ordering::Release);
        // 通知所有等待 fsync 的 committer
        self.flush_notify.1.notify_all();
        Ok(())
    }

    /// 获取已 fsync 到磁盘的 LSN
    #[inline]
    pub fn flushed_lsn(&self) -> Lsn {
        Lsn::from_raw(self.flushed_lsn.load(Ordering::Acquire))
    }

    /// 等待指定 LSN 被 fsync 到磁盘（Durability 保证）
    ///
    /// 与 C++ WaitTargetPlsnPersist 等价：
    /// 1. 快速路径：检查 flushed_lsn
    /// 2. 唤醒 flusher 让它立即 sync
    /// 3. Condvar 等待 flusher 完成（group commit：多个 commit 共享一次 fsync）
    /// 4. Fallback: 自行 sync
    pub fn wait_for_lsn(&self, target_lsn: Lsn) -> Result<()> {
        // 快速路径
        if self.flushed_lsn.load(Ordering::Acquire) >= target_lsn.raw() {
            return Ok(());
        }

        // 唤醒 flusher
        self.notify_flusher();

        // Condvar 等待 flusher 完成 sync（最多 10ms 超时）
        let guard = self.flush_notify.0.lock().unwrap();
        let (_guard, timeout) = self.flush_notify.1
            .wait_timeout(guard, std::time::Duration::from_millis(10))
            .unwrap();

        if self.flushed_lsn.load(Ordering::Acquire) >= target_lsn.raw() {
            return Ok(());
        }

        // Fallback: 自行 sync（flusher 未及时完成）
        self.sync()
    }

    /// 唤醒 flusher 线程立即 sync
    fn notify_flusher(&self) {
        let key = &self.flushed_lsn as *const AtomicU64 as usize;
        unsafe {
            parking_lot_core::unpark_all(key, parking_lot_core::DEFAULT_UNPARK_TOKEN);
        }
    }

    /// 写入并同步
    pub fn write_and_sync(&self, data: &[u8]) -> Result<Lsn> {
        let lsn = self.write(data)?;
        self.sync()?;
        Ok(lsn)
    }

    /// 启动后台 WAL Flusher 线程
    ///
    /// 每 `interval_ms` 毫秒调用一次 sync()，实现 group commit 效果。
    /// 返回一个 handle，drop 时停止后台线程。
    pub fn start_flusher(self: &Arc<Self>, interval_ms: u64) -> WalFlusher {
        WalFlusher::start(Arc::clone(self), interval_ms)
    }

    /// 读取指定 LSN 的记录
    pub fn read(&self, lsn: Lsn) -> Result<Vec<u8>> {
        let (file_no, offset) = lsn.parts();

        // 检查是否是当前文件
        let current_file_no = self.file_no.load(Ordering::Acquire);
        if file_no != current_file_no {
            return Err(FerrisDBError::InvalidArgument(
                "Cannot read from old WAL file".to_string(),
            ));
        }

        let file_path = self.get_file_path(file_no);

        let mut file = OpenOptions::new()
            .read(true)
            .open(&file_path)
            .map_err(|e| FerrisDBError::Wal(WalError::ReadFailed(
                format!("Failed to open WAL file for read: {}", e)
            )))?;

        // 定位
        file.seek(SeekFrom::Start(offset as u64))
            .map_err(|e| FerrisDBError::Wal(WalError::ReadFailed(
                format!("Failed to seek WAL file: {}", e)
            )))?;

        // 读取记录头部
        let mut header_buf = [0u8; std::mem::size_of::<WalRecordHeader>()];
        file.read_exact(&mut header_buf)
            .map_err(|e| FerrisDBError::Wal(WalError::ReadFailed(
                format!("Failed to read WAL header: {}", e)
            )))?;

        let header = unsafe {
            std::ptr::read(header_buf.as_ptr() as *const WalRecordHeader)
        };

        // 读取数据
        let data_size = header.size as usize;
        let mut data = vec![0u8; data_size];
        file.read_exact(&mut data)
            .map_err(|e| FerrisDBError::Wal(WalError::ReadFailed(
                format!("Failed to read WAL data: {}", e)
            )))?;

        // 返回完整的记录（头部 + 数据）
        let mut result = Vec::with_capacity(header_buf.len() + data_size);
        result.extend_from_slice(&header_buf);
        result.extend_from_slice(&data);

        Ok(result)
    }

    /// 通知后台 flusher 尽快 sync（group commit 唤醒）
    ///
    /// 非阻塞：只 unpark flusher 线程，不等待 sync 完成。
    pub fn notify_sync(&self) {
        let key = &self.flushed_lsn as *const AtomicU64 as usize;
        unsafe { parking_lot_core::unpark_one(key, |_| parking_lot_core::DEFAULT_UNPARK_TOKEN); }
    }

    /// 关闭 Writer
    pub fn close(&self) -> Result<()> {
        self.sync()?;
        let mut file_guard = self.current_file.lock();
        *file_guard = None;
        Ok(())
    }
}

impl Default for WalWriter {
    fn default() -> Self {
        Self::new("/tmp/dstore_wal")
    }
}

/// 后台 WAL Flusher — 周期性 fsync WAL 文件（group commit）
///
/// C++ ferrisdb 的 BgWalWriter 每 ~100ms fsync 一次，实现 group commit。
/// 这里用同样策略：DML 只 write() 不 sync()，后台线程批量 sync。
pub struct WalFlusher {
    shutdown: Arc<std::sync::atomic::AtomicBool>,
}

impl WalFlusher {
    /// 启动后台 fsync 线程
    ///
    /// 使用 park/unpark 机制：
    /// - 默认每 interval_ms 周期 sync
    /// - commit 调用 `notify_sync()` 时立即唤醒 sync（group commit）
    pub fn start(writer: Arc<WalWriter>, interval_ms: u64) -> Self {
        let shutdown = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        std::thread::spawn(move || {
            // flushed_lsn 的地址作为 park key — notify_flusher 会 unpark 这个 key
            let key = &writer.flushed_lsn as *const AtomicU64 as usize;
            while !shutdown_clone.load(std::sync::atomic::Ordering::Acquire) {
                // 执行 sync（fsync WAL 文件到磁盘）
                let _ = writer.sync();
                // Park: 等待被 commit 唤醒或定时器超时
                unsafe {
                    parking_lot_core::park(
                        key,
                        || !shutdown_clone.load(std::sync::atomic::Ordering::Acquire),
                        || {},
                        |_, _| {},
                        parking_lot_core::DEFAULT_PARK_TOKEN,
                        Some(std::time::Instant::now() + std::time::Duration::from_millis(interval_ms)),
                    );
                }
            }
            let _ = writer.sync();
        });

        Self { shutdown }
    }

    /// 停止后台线程
    pub fn stop(&self) {
        self.shutdown.store(true, std::sync::atomic::Ordering::Release);
    }
}

impl Drop for WalFlusher {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_wal_file_header_size() {
        assert_eq!(std::mem::size_of::<WalFileHeader>(), 40);
    }

    #[test]
    fn test_wal_writer_new() {
        let writer = WalWriter::new("/tmp/test_wal");
        // Initially LSN is file_no=0, offset=40 (header size), which creates a non-zero LSN
        // LSN from parts: (0, 40) = 40, which is valid (non-zero)
        let lsn = writer.current_lsn();
        // LSN 40 is valid because it's non-zero
        assert!(lsn.raw() >= WalFileHeader::size() as u64);
    }

    #[test]
    fn test_wal_writer_write() {
        let temp_dir = TempDir::new().unwrap();
        let writer = WalWriter::new(temp_dir.path());

        let data = [1u8, 2, 3, 4, 5];
        let lsn = writer.write(&data).unwrap();
        assert!(lsn.is_valid());

        // 检查文件已创建
        let file_path = writer.get_file_path(0);
        assert!(file_path.exists());
    }

    #[test]
    fn test_wal_writer_sync() {
        let temp_dir = TempDir::new().unwrap();
        let writer = WalWriter::new(temp_dir.path());

        let data = [1u8, 2, 3, 4, 5];
        writer.write(&data).unwrap();
        writer.sync().unwrap();
    }

    #[test]
    fn test_wal_writer_switch_file() {
        let temp_dir = TempDir::new().unwrap();
        let writer = WalWriter::new(temp_dir.path());

        // 写入一些数据
        let data = [1u8; 100];
        writer.write(&data).unwrap();

        // 切换文件
        writer.switch_file().unwrap();

        // 检查新文件已创建
        assert_eq!(writer.file_no(), 1);
        let file_path = writer.get_file_path(1);
        assert!(file_path.exists());
    }

    #[test]
    fn test_wal_writer_close() {
        let temp_dir = tempfile::TempDir::new().unwrap();
        let writer = WalWriter::new(temp_dir.path());
        writer.write(&[1u8; 50]).unwrap();
        writer.close().unwrap();
    }
}
