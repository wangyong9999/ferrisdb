//! WAL Buffer
//!
//! 缓存 WAL 记录，批量写入磁盘。

use ferrisdb_core::{Lsn, Xid};
use ferrisdb_core::atomic::{AtomicU64, AtomicUsize, Ordering};
use ferrisdb_core::error::{FerrisDBError, Result, WalError};

use super::record::{WalAtomicGroupHeader, WalRecordHeader};

/// 默认 WAL Buffer 大小（16MB）
pub const DEFAULT_WAL_BUFFER_SIZE: usize = 16 * 1024 * 1024;

/// WAL Buffer
///
/// 缓存 WAL 记录，提供批量写入优化。
pub struct WalBuffer {
    /// 缓冲区数据（使用 UnsafeCell 支持内部可变性）
    data: std::cell::UnsafeCell<Vec<u8>>,
    /// 缓冲区大小
    size: usize,
    /// 当前写入位置
    write_pos: AtomicUsize,
    /// 刷新位置（已刷新到磁盘）
    flush_pos: AtomicUsize,
    /// 当前 LSN
    current_lsn: AtomicU64,
}

// WAL Buffer 可以在线程间共享（通过原子操作同步）
unsafe impl Sync for WalBuffer {}
unsafe impl Send for WalBuffer {}

impl WalBuffer {
    /// 创建新的 WAL Buffer
    pub fn new(size: usize) -> Self {
        let actual_size = if size == 0 { DEFAULT_WAL_BUFFER_SIZE } else { size };
        Self {
            data: std::cell::UnsafeCell::new(vec![0u8; actual_size]),
            size: actual_size,
            write_pos: AtomicUsize::new(0),
            flush_pos: AtomicUsize::new(0),
            current_lsn: AtomicU64::new(1), // Start from 1, since 0 is INVALID
        }
    }

    /// 获取数据引用
    #[inline]
    fn data(&self) -> &[u8] {
        unsafe { &*self.data.get() }
    }

    /// 获取数据可变引用
    #[inline]
    fn data_mut(&self) -> &mut [u8] {
        unsafe { &mut *self.data.get() }
    }

    /// 获取当前 LSN
    #[inline]
    pub fn current_lsn(&self) -> Lsn {
        Lsn::from_raw(self.current_lsn.load(Ordering::Acquire))
    }

    /// 获取写入位置
    #[inline]
    pub fn write_pos(&self) -> usize {
        self.write_pos.load(Ordering::Acquire)
    }

    /// 获取刷新位置
    #[inline]
    pub fn flush_pos(&self) -> usize {
        self.flush_pos.load(Ordering::Acquire)
    }

    /// 获取可用空间
    #[inline]
    pub fn available(&self) -> usize {
        self.size.saturating_sub(self.write_pos())
    }

    /// 获取未刷新的数据大小
    #[inline]
    pub fn unflushed(&self) -> usize {
        self.write_pos().saturating_sub(self.flush_pos())
    }

    /// 分配 LSN
    fn allocate_lsn(&self, size: usize) -> Lsn {
        let lsn = self.current_lsn.fetch_add(size as u64, Ordering::AcqRel);
        Lsn::from_raw(lsn)
    }

    /// 写入单个 WAL 记录
    ///
    /// 返回写入的 LSN
    pub fn write(&self, record_data: &[u8]) -> Result<Lsn> {
        let record_size = record_data.len();

        // 检查空间
        if self.available() < record_size {
            // 需要先刷新
            return Err(FerrisDBError::Wal(WalError::BufferFull));
        }

        // 分配 LSN
        let lsn = self.allocate_lsn(record_size);

        // 写入数据
        let write_pos = self.write_pos.fetch_add(record_size, Ordering::AcqRel);

        // 检查是否有足够空间（可能有并发）
        if write_pos + record_size > self.size {
            // 回滚写入位置
            self.write_pos.fetch_sub(record_size, Ordering::AcqRel);
            return Err(FerrisDBError::Wal(WalError::BufferFull));
        }

        // 复制数据
        self.data_mut()[write_pos..write_pos + record_size].copy_from_slice(record_data);

        // 内存屏障，确保数据可见
        std::sync::atomic::fence(Ordering::Release);

        Ok(lsn)
    }

    /// 开始原子组写入
    ///
    /// 返回原子组的起始位置
    pub fn begin_atomic_group(&self, xid: Xid) -> Result<usize> {
        let header_size = WalAtomicGroupHeader::header_size();

        // 检查空间
        if self.available() < header_size {
            return Err(FerrisDBError::Wal(WalError::BufferFull));
        }

        // 分配位置
        let write_pos = self.write_pos.fetch_add(header_size, Ordering::AcqRel);

        // 写入头部（暂时不设置 group_len 和 record_num）
        let header = WalAtomicGroupHeader::new(xid);
        let header_bytes = unsafe {
            std::slice::from_raw_parts(
                &header as *const WalAtomicGroupHeader as *const u8,
                header_size,
            )
        };
        self.data_mut()[write_pos..write_pos + header_size].copy_from_slice(header_bytes);

        Ok(write_pos)
    }

    /// 向原子组追加记录
    pub fn append_to_atomic_group(&self, group_pos: usize, record_data: &[u8]) -> Result<()> {
        let record_size = record_data.len();
        let header_size = WalAtomicGroupHeader::header_size();

        // 检查空间
        if self.available() < record_size {
            return Err(FerrisDBError::Wal(WalError::BufferFull));
        }

        // 分配位置
        let write_pos = self.write_pos.fetch_add(record_size, Ordering::AcqRel);

        // 复制记录数据
        self.data_mut()[write_pos..write_pos + record_size].copy_from_slice(record_data);

        // 原子更新原子组头部，避免并发追加时丢更新
        unsafe {
            let header_ptr = self.data_mut().as_mut_ptr().add(group_pos);
            let group_len_ptr = header_ptr.add(std::mem::offset_of!(WalAtomicGroupHeader, group_len)) as *const std::sync::atomic::AtomicU32;
            let record_num_ptr = header_ptr.add(std::mem::offset_of!(WalAtomicGroupHeader, record_num)) as *const std::sync::atomic::AtomicU32;
            (*group_len_ptr).fetch_add(record_size as u32, Ordering::AcqRel);
            (*record_num_ptr).fetch_add(1, Ordering::AcqRel);
        }

        Ok(())
    }

    /// 完成原子组写入
    ///
    /// 计算并设置 CRC，返回 LSN
    pub fn end_atomic_group(&self, group_pos: usize) -> Result<Lsn> {
        let header_size = WalAtomicGroupHeader::header_size();

        // 读取原子组长度
        let header = unsafe {
            &*(self.data().as_ptr().add(group_pos) as *const WalAtomicGroupHeader)
        };
        let group_len = header.group_len;
        let total_size = header_size + group_len as usize;

        // 分配 LSN
        let lsn = self.allocate_lsn(total_size);

        // 计算 CRC（简化实现：使用简单的 checksum）
        let crc = self.compute_crc(group_pos + header_size, group_len as usize);

        // 更新 CRC
        let header = unsafe {
            &mut *(self.data_mut().as_mut_ptr().add(group_pos) as *mut WalAtomicGroupHeader)
        };
        header.crc = crc;

        // 内存屏障
        std::sync::atomic::fence(Ordering::Release);

        Ok(lsn)
    }

    /// 获取未刷新的数据
    pub fn get_unflushed_data(&self) -> &[u8] {
        let flush_pos = self.flush_pos.load(Ordering::Acquire);
        let write_pos = self.write_pos.load(Ordering::Acquire);
        &self.data()[flush_pos..write_pos]
    }

    /// 标记已刷新
    pub fn mark_flushed(&self, size: usize) {
        self.flush_pos.fetch_add(size, Ordering::AcqRel);
    }

    /// 刷新缓冲区
    ///
    /// 将未刷新的数据返回给调用者写入磁盘
    pub fn flush(&self) -> Result<(Lsn, Vec<u8>)> {
        let flush_pos = self.flush_pos.load(Ordering::Acquire);
        let write_pos = self.write_pos.load(Ordering::Acquire);

        if flush_pos >= write_pos {
            return Ok((self.current_lsn(), Vec::new()));
        }

        let data = self.data()[flush_pos..write_pos].to_vec();
        let lsn = self.current_lsn();

        Ok((lsn, data))
    }

    /// 重置缓冲区
    pub fn reset(&self) {
        self.write_pos.store(0, Ordering::Release);
        self.flush_pos.store(0, Ordering::Release);
    }

    /// 计算 CRC32（使用硬件加速的 crc32fast）
    fn compute_crc(&self, start: usize, len: usize) -> u32 {
        let end = (start + len).min(self.size);
        crc32fast::hash(&self.data()[start..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wal_buffer_new() {
        let buf = WalBuffer::new(8192);
        assert_eq!(buf.size, 8192);
        assert_eq!(buf.write_pos(), 0);
        assert_eq!(buf.flush_pos(), 0);
    }

    #[test]
    fn test_wal_buffer_write() {
        let buf = WalBuffer::new(8192);
        let data = [1u8, 2, 3, 4, 5];

        let lsn = buf.write(&data).unwrap();
        assert!(lsn.is_valid());
        assert_eq!(buf.write_pos(), 5);
    }

    #[test]
    fn test_wal_buffer_available() {
        let buf = WalBuffer::new(8192);
        assert_eq!(buf.available(), 8192);

        let data = [1u8; 100];
        buf.write(&data).unwrap();
        assert_eq!(buf.available(), 8092);
    }

    #[test]
    fn test_wal_buffer_flush() {
        let buf = WalBuffer::new(8192);
        let data = [1u8, 2, 3, 4, 5];

        buf.write(&data).unwrap();
        let (lsn, flushed) = buf.flush().unwrap();
        assert!(lsn.is_valid());
        assert_eq!(flushed.len(), 5);
    }

    #[test]
    fn test_wal_buffer_atomic_group() {
        let buf = WalBuffer::new(8192);
        let xid = Xid::new(1, 0);

        let group_pos = buf.begin_atomic_group(xid).unwrap();

        let record1 = [1u8, 2, 3];
        let record2 = [4u8, 5, 6];

        buf.append_to_atomic_group(group_pos, &record1).unwrap();
        buf.append_to_atomic_group(group_pos, &record2).unwrap();

        let lsn = buf.end_atomic_group(group_pos).unwrap();
        assert!(lsn.is_valid());
    }
}
