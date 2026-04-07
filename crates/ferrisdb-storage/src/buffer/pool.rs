//! Buffer Pool 实现
//!
//! 管理内存中的页面缓存。

use super::desc::BufferDesc;
use super::lru::{LruQueue, NUM_LRU_QUEUES};
use super::table::BufTable;
use crate::page::PAGE_SIZE;
use crate::smgr::StorageManager;
use ferrisdb_core::{BufferTag, BufId, FerrisDBError, Result};
use ferrisdb_core::atomic::{AtomicU32, AtomicU64, AtomicPtr, Ordering};
use ferrisdb_core::lock::{ContentLock, LockMode};
use std::alloc::{alloc, dealloc, Layout};
use std::ptr::NonNull;
use std::sync::Arc;

/// Buffer Pool 配置
#[derive(Debug, Clone)]
pub struct BufferPoolConfig {
    /// Buffer 数量
    pub buffer_count: usize,
    /// 每个页面的大小（字节）
    pub page_size: usize,
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            buffer_count: 1024,
            page_size: PAGE_SIZE,
        }
    }
}

impl BufferPoolConfig {
    /// 创建新的配置
    pub fn new(buffer_count: usize) -> Self {
        Self {
            buffer_count,
            page_size: PAGE_SIZE,
        }
    }
}

/// Buffer Pool
///
/// 管理内存中的页面缓存。
pub struct BufferPool {
    /// 配置
    config: BufferPoolConfig,
    /// Buffer 描述符数组
    buffers: Vec<BufferDesc>,
    /// 页面数据区域
    page_area: NonNull<u8>,
    /// Hash 表
    table: BufTable,
    /// LRU 队列
    lru_queues: Vec<LruQueue>,
    /// 存储管理器
    smgr: Option<Arc<StorageManager>>,
    /// 下一个策略索引
    next_strategy: AtomicU32,
    /// 统计：命中次数
    stat_hits: AtomicU64,
    /// 统计：未命中次数
    stat_misses: AtomicU64,
    /// 统计：Pin 调用次数
    stat_pins: AtomicU64,
    /// 自旋锁（用于 buffer allocation）
    alloc_lock: AtomicU32,
    /// WAL Writer 引用（用于 flush 前检查 WAL LSN）
    wal_writer: Option<Arc<crate::wal::WalWriter>>,
}

// 安全实现
unsafe impl Send for BufferPool {}
unsafe impl Sync for BufferPool {}

impl BufferPool {
    /// 创建新的 Buffer Pool
    pub fn new(config: BufferPoolConfig) -> Result<Self> {
        Self::with_smgr(config, None)
    }

    /// 创建带存储管理器的 Buffer Pool
    pub fn with_smgr(config: BufferPoolConfig, smgr: Option<Arc<StorageManager>>) -> Result<Self> {
        if config.buffer_count == 0 {
            return Err(FerrisDBError::InvalidArgument(
                "buffer_count must be > 0".to_string(),
            ));
        }

        // 分配页面数据区域
        let page_area_size = config.buffer_count * config.page_size;
        let layout = Layout::from_size_align(page_area_size, config.page_size)
            .map_err(|e| FerrisDBError::Internal(format!("Failed to create layout: {}", e)))?;

        let page_area = unsafe {
            let ptr = alloc(layout);
            if ptr.is_null() {
                return Err(FerrisDBError::Internal("Failed to allocate page area".to_string()));
            }
            // 初始化为 0
            std::ptr::write_bytes(ptr, 0, page_area_size);
            NonNull::new_unchecked(ptr)
        };

        // 创建 Buffer 描述符
        let mut buffers = Vec::with_capacity(config.buffer_count);
        for i in 0..config.buffer_count {
            let desc = BufferDesc::new();
            // 设置页面指针
            let page_ptr = unsafe { page_area.as_ptr().add(i * config.page_size) };
            desc.buf_block.store(page_ptr, Ordering::Release);
            buffers.push(desc);
        }

        // 创建 Hash 表
        let table = BufTable::new(config.buffer_count);

        // 创建 LRU 队列
        let lru_queues = (0..NUM_LRU_QUEUES).map(|i| LruQueue::new(i as u32)).collect();

        Ok(Self {
            config,
            buffers,
            page_area,
            table,
            lru_queues,
            smgr,
            next_strategy: AtomicU32::new(0),
            stat_hits: AtomicU64::new(0),
            stat_misses: AtomicU64::new(0),
            stat_pins: AtomicU64::new(0),
            alloc_lock: AtomicU32::new(0),
            wal_writer: None,
        })
    }

    /// 设置存储管理器
    pub fn set_smgr(&mut self, smgr: Arc<StorageManager>) {
        self.smgr = Some(smgr);
    }

    /// 设置 WAL Writer（启用 WAL-before-data 检查）
    pub fn set_wal_writer(&mut self, writer: Arc<crate::wal::WalWriter>) {
        self.wal_writer = Some(writer);
    }

    /// 获取 Buffer 数量
    #[inline]
    pub fn buffer_count(&self) -> usize {
        self.config.buffer_count
    }

    /// 获取 Buffer 描述符
    #[inline]
    pub fn get_buffer(&self, buf_id: BufId) -> Option<&BufferDesc> {
        if buf_id >= 0 && (buf_id as usize) < self.config.buffer_count {
            Some(&self.buffers[buf_id as usize])
        } else {
            None
        }
    }

    /// 获取页面数据指针
    #[inline]
    pub fn get_page_data(&self, buf_id: BufId) -> Option<*mut u8> {
        if buf_id >= 0 && (buf_id as usize) < self.config.buffer_count {
            Some(self.buffers[buf_id as usize].buf_block.load(Ordering::Acquire))
        } else {
            None
        }
    }

    /// 查找 Buffer
    pub fn lookup(&self, tag: &BufferTag) -> Option<BufId> {
        let buf_id = self.table.lookup(tag);
        if buf_id.is_some() {
            self.stat_hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stat_misses.fetch_add(1, Ordering::Relaxed);
        }
        buf_id
    }

    /// Pin 一个 Buffer
    ///
    /// 返回 PinnedBuffer 守卫
    pub fn pin(&self, tag: &BufferTag) -> Result<PinnedBuffer<'_>> {
        self.stat_pins.fetch_add(1, Ordering::Relaxed);
        // 快速路径：查找已存在的
        if let Some(buf_id) = self.lookup(tag) {
            let desc = &self.buffers[buf_id as usize];
            desc.pin();
            return Ok(PinnedBuffer {
                pool: self,
                buf_id,
                mode: LockMode::Shared,
            });
        }

        // 慢速路径：需要分配一个新 buffer
        self.pin_slow(tag)
    }

    /// 慢速路径：分配新 buffer 并读取页面
    fn pin_slow(&self, tag: &BufferTag) -> Result<PinnedBuffer<'_>> {
        // 等待获取分配锁（短自旋 + park 避免 livelock）
        let mut spins = 0u32;
        while self.alloc_lock.compare_exchange(
            0, 1,
            Ordering::Acquire,
            Ordering::Relaxed,
        ).is_err() {
            spins += 1;
            if spins < 40 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                spins = 0;
            }
        }

        // 再次检查（可能其他线程已经分配）
        if let Some(buf_id) = self.table.lookup(tag) {
            self.alloc_lock.store(0, Ordering::Release);
            let desc = &self.buffers[buf_id as usize];
            desc.pin();
            return Ok(PinnedBuffer {
                pool: self,
                buf_id,
                mode: LockMode::Shared,
            });
        }

        // 找一个空闲 buffer
        let buf_id = self.find_victim()?;
        let desc = &self.buffers[buf_id as usize];

        // 从旧 hash 表移除
        let old_tag = desc.tag();
        if old_tag.is_valid() {
            self.table.remove(&old_tag);
        }

        // 脏页必须先 flush 成功才能替换（防止数据丢失）
        if desc.is_dirty() {
            // 在 alloc_lock 内 flush（确保 flush 失败时不丢页面）
            if let Err(e) = self.flush_buffer(buf_id) {
                // flush 失败：放弃这个 victim，释放锁，返回错误
                self.alloc_lock.store(0, Ordering::Release);
                return Err(FerrisDBError::Internal(format!(
                    "Buffer eviction failed: cannot flush dirty page: {:?}", e
                )));
            }
        }

        // flush 成功（或不需要 flush），安全替换 tag
        self.table.insert(*tag, buf_id);
        desc.pin();
        self.alloc_lock.store(0, Ordering::Release);

        // 读取新页面（alloc_lock 已释放，I/O 不阻塞其他线程）
        let page_data = desc.buf_block.load(Ordering::Acquire);
        if !page_data.is_null() {
            if let Some(ref smgr) = self.smgr {
                let read_result = unsafe {
                    smgr.read_page(tag, std::slice::from_raw_parts_mut(page_data, PAGE_SIZE))
                };
                match read_result {
                    Ok(()) => {
                        let page_slice = unsafe { std::slice::from_raw_parts(page_data, PAGE_SIZE) };
                        if !crate::page::PageHeader::verify_checksum(page_slice) {
                            // 非零页面 checksum 不匹配 = 磁盘数据损坏
                            let is_zero = page_slice.iter().all(|&b| b == 0);
                            if !is_zero {
                                eprintln!("[buffer] page checksum mismatch for {:?}, possible disk corruption", tag);
                                return Err(FerrisDBError::Internal(
                                    format!("Page checksum verification failed for {:?}", tag)
                                ));
                            }
                        }
                        unsafe { desc.set_tag(*tag); }
                    }
                    Err(_) => {
                        unsafe {
                            std::ptr::write_bytes(page_data, 0, PAGE_SIZE);
                            desc.set_tag(*tag);
                        }
                    }
                }
            } else {
                unsafe {
                    std::ptr::write_bytes(page_data, 0, PAGE_SIZE);
                    desc.set_tag(*tag);
                }
            }
        }

        Ok(PinnedBuffer {
            pool: self,
            buf_id,
            mode: LockMode::Shared,
        })
    }

    /// 查找可淘汰的 buffer
    fn find_victim(&self) -> Result<BufId> {
        // 简化实现：使用时钟算法
        let start = self.next_strategy.fetch_add(1, Ordering::AcqRel) as usize;
        let count = self.config.buffer_count;

        for i in 0..count {
            let idx = (start + i) % count;
            let desc = &self.buffers[idx];

            // 检查是否可以被淘汰
            if !desc.is_pinned() {
                return Ok(idx as BufId);
            }
        }

        Err(FerrisDBError::Buffer(ferrisdb_core::error::BufferError::PoolFull))
    }

    /// 刷新 buffer 到磁盘
    ///
    /// WAL-before-data 不变式：在写脏页到磁盘前，确保该页的 WAL 记录已 fsync。
    /// 读取页面头部的 LSN（前 8 字节），对比 WalWriter.flushed_lsn。
    fn flush_buffer(&self, buf_id: BufId) -> Result<()> {
        let desc = &self.buffers[buf_id as usize];
        let tag = desc.tag();

        if !tag.is_valid() || !desc.is_dirty() {
            return Ok(());
        }

        if let Some(ref smgr) = self.smgr {
            let page_data = desc.buf_block.load(Ordering::Acquire);
            if !page_data.is_null() {
                // WAL-before-data: 确保页面 LSN 对应的 WAL 已 fsync
                if let Some(ref wal_writer) = self.wal_writer {
                    let page_lsn = unsafe {
                        let lsn_bytes = std::slice::from_raw_parts(page_data, 8);
                        u64::from_le_bytes(lsn_bytes.try_into().unwrap_or([0; 8]))
                    };
                    if page_lsn > 0 {
                        let lsn = ferrisdb_core::Lsn::from_raw(page_lsn);
                        if lsn.raw() > wal_writer.flushed_lsn().raw() {
                            let _ = wal_writer.wait_for_lsn(lsn);
                        }
                    }
                }

                let page_slice = unsafe { std::slice::from_raw_parts_mut(page_data, PAGE_SIZE) };

                // Full-page write: checkpoint 后首次刷脏页时，
                // 将完整页面写入 WAL（保护 torn page）
                if let Some(ref wal_writer) = self.wal_writer {
                    let page_lsn = u64::from_le_bytes(page_slice[0..8].try_into().unwrap_or([0; 8]));
                    // 简化判断：总是写 FPW（生产中应只在 checkpoint 后首次写）
                    if page_lsn > 0 && page_slice.iter().any(|&b| b != 0) {
                        // FPW record: [WalRecordHeader(4)][page_data(8192)]
                        let header = crate::wal::WalRecordHeader::new(
                            crate::wal::WalRecordType::HeapPageFull,
                            PAGE_SIZE as u16,
                        );
                        let hdr_bytes = unsafe {
                            std::slice::from_raw_parts(
                                &header as *const _ as *const u8,
                                std::mem::size_of::<crate::wal::WalRecordHeader>(),
                            )
                        };
                        let mut fpw_buf = Vec::with_capacity(hdr_bytes.len() + PAGE_SIZE);
                        fpw_buf.extend_from_slice(hdr_bytes);
                        fpw_buf.extend_from_slice(page_slice);
                        wal_writer.write(&fpw_buf)?;
                    }
                }

                // 写入前计算并设置 checksum
                crate::page::PageHeader::set_checksum(page_slice);
                smgr.write_page(&tag, page_slice)?;
                desc.clear_dirty();
            }
        }

        Ok(())
    }

    /// 尝试 Pin 一个已存在的 Buffer
    pub fn try_pin_existing(&self, tag: &BufferTag) -> Option<PinnedBuffer<'_>> {
        let buf_id = self.table.lookup(tag)?;
        let desc = &self.buffers[buf_id as usize];
        desc.pin();
        Some(PinnedBuffer {
            pool: self,
            buf_id,
            mode: LockMode::Shared,
        })
    }

    /// Unpin 一个 Buffer
    pub fn unpin(&self, buf_id: BufId) {
        if buf_id >= 0 && (buf_id as usize) < self.config.buffer_count {
            self.buffers[buf_id as usize].unpin();
        }
    }

    /// 标记 buffer 为脏
    pub fn mark_dirty(&self, buf_id: BufId) {
        if buf_id >= 0 && (buf_id as usize) < self.config.buffer_count {
            let desc = &self.buffers[buf_id as usize];
            desc.set_dirty();
        }
    }

    /// 刷新所有脏页
    pub fn flush_all(&self) -> Result<()> {
        for (i, desc) in self.buffers.iter().enumerate() {
            if desc.is_dirty() {
                self.flush_buffer(i as BufId)?;
            }
        }
        Ok(())
    }

    /// 获取命中次数
    #[inline]
    pub fn stat_hits(&self) -> u64 {
        self.stat_hits.load(Ordering::Relaxed)
    }

    /// 获取未命中次数
    #[inline]
    pub fn stat_misses(&self) -> u64 {
        self.stat_misses.load(Ordering::Relaxed)
    }

    /// 重置统计信息
    pub fn reset_stats(&self) {
        self.stat_hits.store(0, Ordering::Relaxed);
        self.stat_misses.store(0, Ordering::Relaxed);
        self.stat_pins.store(0, Ordering::Relaxed);
    }

    /// 获取 Pin 次数
    pub fn stat_pins(&self) -> u64 {
        self.stat_pins.load(Ordering::Relaxed)
    }

    /// 获取脏页数量
    pub fn dirty_page_count(&self) -> usize {
        self.buffers.iter().filter(|d| d.is_dirty()).count()
    }

    /// 获取总页面数量
    pub fn total_page_count(&self) -> usize {
        self.config.buffer_count
    }

    /// 刷新最多 N 个脏页（从 LRU 尾部开始），返回实际刷新数量
    /// 刷新最多 N 个脏页（按 page LSN 升序，确保旧页先落盘）
    pub fn flush_some(&self, max_pages: usize) -> usize {
        // 收集脏页及其 LSN
        let mut dirty: Vec<(BufId, u64)> = Vec::new();
        for (idx, desc) in self.buffers.iter().enumerate() {
            if desc.is_dirty() && !desc.is_pinned() {
                let page_ptr = desc.buf_block.load(Ordering::Acquire);
                let lsn = if !page_ptr.is_null() {
                    unsafe { u64::from_le_bytes(std::slice::from_raw_parts(page_ptr, 8).try_into().unwrap_or([0; 8])) }
                } else { 0 };
                dirty.push((idx as BufId, lsn));
            }
        }
        // 按 LSN 升序排序（旧页先刷）
        dirty.sort_by_key(|&(_, lsn)| lsn);

        let mut flushed = 0;
        for (buf_id, _) in dirty.iter().take(max_pages) {
            if self.flush_buffer(*buf_id).is_ok() {
                flushed += 1;
            }
        }
        flushed
    }

    /// 获取命中率
    #[inline]
    pub fn hit_rate(&self) -> f64 {
        let hits = self.stat_hits();
        let misses = self.stat_misses();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

/// 后台脏页写出器
///
/// 周期性扫描 BufferPool，将脏页写回磁盘，减少检查点时的突发 I/O。
pub struct BackgroundWriter {
    /// 是否停止
    shutdown: Arc<ferrisdb_core::atomic::AtomicBool>,
}

impl BackgroundWriter {
    /// 启动后台写线程
    ///
    /// `interval_ms`: 扫描间隔（毫秒）
    /// `max_pages_per_round`: 每轮最多刷新页面数
    pub fn start(
        buffer_pool: Arc<BufferPool>,
        interval_ms: u64,
        max_pages_per_round: usize,
    ) -> Self {
        let shutdown = Arc::new(ferrisdb_core::atomic::AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        std::thread::spawn(move || {
            while !shutdown_clone.load(Ordering::Acquire) {
                let flushed = buffer_pool.flush_some(max_pages_per_round);
                let _ = flushed;
                std::thread::sleep(std::time::Duration::from_millis(interval_ms));
            }
        });

        Self { shutdown }
    }

    /// 停止后台写线程
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Release);
    }
}

impl Drop for BackgroundWriter {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Drop for BufferPool {
    fn drop(&mut self) {
        // 刷新所有脏页
        let _ = self.flush_all();

        // 释放页面数据区域
        let page_area_size = self.config.buffer_count * self.config.page_size;
        let layout = Layout::from_size_align(page_area_size, self.config.page_size).unwrap();
        unsafe {
            dealloc(self.page_area.as_ptr(), layout);
        }
    }
}

/// Pinned Buffer 守卫
///
/// RAII 风格的页面内容锁守卫，使用 BufferDesc 内嵌的 ContentLock。
pub struct PageLockGuard<'a> {
    lock: &'a ContentLock,
    mode: LockMode,
}

impl Drop for PageLockGuard<'_> {
    fn drop(&mut self) {
        self.lock.release(self.mode);
    }
}

/// RAII 风格的 Buffer 访问守卫。
pub struct PinnedBuffer<'a> {
    pool: &'a BufferPool,
    pub(crate) buf_id: BufId,
    mode: LockMode,
}

impl<'a> PinnedBuffer<'a> {
    /// 获取 Buffer ID
    #[inline]
    pub fn buf_id(&self) -> BufId {
        self.buf_id
    }

    /// 获取 Buffer 描述符
    #[inline]
    pub fn desc(&self) -> &BufferDesc {
        &self.pool.buffers[self.buf_id as usize]
    }

    /// 获取页面数据指针
    #[inline]
    pub fn page_data(&self) -> *mut u8 {
        self.desc().buf_block.load(Ordering::Acquire)
    }

    /// 获取页面数据切片
    #[inline]
    pub fn page_slice(&self) -> &[u8] {
        unsafe {
            std::slice::from_raw_parts(self.page_data(), PAGE_SIZE)
        }
    }

    /// 获取页面数据可变切片
    #[inline]
    pub fn page_slice_mut(&mut self) -> &mut [u8] {
        unsafe {
            std::slice::from_raw_parts_mut(self.page_data(), PAGE_SIZE)
        }
    }

    /// 获取内容锁（共享）
    pub fn lock_shared(&self) -> PageLockGuard<'_> {
        let lock = &self.desc().content_lock;
        lock.acquire_shared();
        PageLockGuard { lock, mode: LockMode::Shared }
    }

    /// 获取内容锁（排他）
    pub fn lock_exclusive(&self) -> PageLockGuard<'_> {
        let lock = &self.desc().content_lock;
        lock.acquire_exclusive();
        PageLockGuard { lock, mode: LockMode::Exclusive }
    }

    /// 标记为脏页
    pub fn mark_dirty(&self) {
        self.pool.mark_dirty(self.buf_id);
    }

    /// 获取 BufferTag
    pub fn tag(&self) -> BufferTag {
        self.desc().tag()
    }
}

impl Drop for PinnedBuffer<'_> {
    fn drop(&mut self) {
        self.pool.unpin(self.buf_id);
    }
}

/// BufferPool 实现 DirtyPageFlusher trait，供 CheckpointManager 使用
impl crate::wal::DirtyPageFlusher for BufferPool {
    fn flush_dirty_pages(&self, _up_to_lsn: ferrisdb_core::Lsn) -> Result<u64> {
        let mut flushed = 0u64;
        for (i, desc) in self.buffers.iter().enumerate() {
            if desc.is_dirty() {
                self.flush_buffer(i as BufId)?;
                flushed += 1;
            }
        }
        // fsync 所有数据文件，确保脏页真正落盘
        if let Some(ref smgr) = self.smgr {
            let _ = smgr.sync_all();
        }
        Ok(flushed)
    }

    fn dirty_page_count(&self) -> u64 {
        self.dirty_page_count() as u64
    }

    fn total_page_count(&self) -> u64 {
        self.total_page_count() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_config() {
        let config = BufferPoolConfig::new(1024);
        assert_eq!(config.buffer_count, 1024);
        assert_eq!(config.page_size, PAGE_SIZE);
    }

    #[test]
    fn test_buffer_pool_new() {
        let config = BufferPoolConfig::new(64);
        let pool = BufferPool::new(config).unwrap();
        assert_eq!(pool.buffer_count(), 64);
    }

    #[test]
    fn test_buffer_pool_get_buffer() {
        let config = BufferPoolConfig::new(64);
        let pool = BufferPool::new(config).unwrap();

        // 有效索引
        assert!(pool.get_buffer(0).is_some());
        assert!(pool.get_buffer(63).is_some());

        // 无效索引
        assert!(pool.get_buffer(-1).is_none());
        assert!(pool.get_buffer(64).is_none());
    }

    #[test]
    fn test_buffer_pool_stats() {
        let config = BufferPoolConfig::new(64);
        let pool = BufferPool::new(config).unwrap();

        assert_eq!(pool.stat_hits(), 0);
        assert_eq!(pool.stat_misses(), 0);
    }
}
