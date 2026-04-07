//! WAL Ring Buffer — Lock-Free Concurrent WAL Write Buffer
//!
//! 参照 C++ dstore WalStreamBuffer 设计：
//! - 环形 buffer + atomic fetch_add 分配槽位（无锁）
//! - Insert status 数组跟踪每个写入的完成状态
//! - 后台 drain 线程扫描连续完成区间写入 WalWriter
//! - Backpressure：writer 超过 buffer 容量时 yield 等待

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicBool, Ordering};

/// Insert status entry 数量（必须是 2 的幂）
const STATUS_ENTRY_COUNT: usize = 4096;
const STATUS_ENTRY_MASK: usize = STATUS_ENTRY_COUNT - 1;

/// 缓存行对齐的 atomic，避免 false sharing
#[repr(align(64))]
struct CacheAligned<T>(T);

/// WAL 环形 Buffer
pub struct WalRingBuffer {
    /// 数据区（固定大小环形 buffer）
    data: Vec<u8>,
    /// Buffer 大小
    size: usize,
    /// 写入位置（无限递增，取模得物理偏移）
    write_pos: CacheAligned<AtomicU64>,
    /// 已 flush 位置（drain 线程维护）
    flush_pos: CacheAligned<AtomicU64>,
    /// 最大连续完成位置
    max_continuous: CacheAligned<AtomicU64>,
    /// Insert status 数组：每个 entry 记录该槽位写入的 end position
    insert_status: Vec<AtomicU64>,
    /// 是否关闭
    shutdown: AtomicBool,
}

// SAFETY: data 通过 write_pos/flush_pos 的 atomic 操作保护并发访问。
// 每个 writer 通过 fetch_add 获得独占区间后才写入对应的 data 区域。
unsafe impl Send for WalRingBuffer {}
unsafe impl Sync for WalRingBuffer {}

impl WalRingBuffer {
    /// 创建新的环形 buffer（size 推荐 16MB）
    pub fn new(size: usize) -> Self {
        let mut insert_status = Vec::with_capacity(STATUS_ENTRY_COUNT);
        for _ in 0..STATUS_ENTRY_COUNT {
            insert_status.push(AtomicU64::new(0));
        }
        Self {
            data: vec![0u8; size],
            size,
            write_pos: CacheAligned(AtomicU64::new(0)),
            flush_pos: CacheAligned(AtomicU64::new(0)),
            max_continuous: CacheAligned(AtomicU64::new(0)),
            insert_status,
            shutdown: AtomicBool::new(false),
        }
    }

    /// 写入数据（无锁）。返回写入的起始位置。
    ///
    /// 如果 buffer 满（writer 超过 flusher 一个 buffer_size），spin-wait。
    pub fn write(&self, record_data: &[u8]) -> u64 {
        let len = record_data.len() as u64;

        // 1. Atomic 分配空间
        let start = self.write_pos.0.fetch_add(len, Ordering::AcqRel);
        let end = start + len;

        // 2. Backpressure：等待 flusher 追上（环形 buffer 不能覆盖未 flush 数据）
        let mut spins = 0u32;
        while end.saturating_sub(self.flush_pos.0.load(Ordering::Acquire)) > self.size as u64 {
            spins += 1;
            if spins < 100 {
                std::hint::spin_loop();
            } else {
                std::thread::yield_now();
                spins = 0;
            }
        }

        // 3. 拷贝数据到环形 buffer（可能跨尾部回绕）
        let phys_start = (start as usize) % self.size;
        let phys_end = phys_start + record_data.len();

        if phys_end <= self.size {
            // 不跨边界
            // SAFETY: fetch_add 保证此区间独占
            unsafe {
                let dst = self.data.as_ptr().add(phys_start) as *mut u8;
                std::ptr::copy_nonoverlapping(record_data.as_ptr(), dst, record_data.len());
            }
        } else {
            // 跨尾部回绕：分两段拷贝
            let first_len = self.size - phys_start;
            unsafe {
                let dst1 = self.data.as_ptr().add(phys_start) as *mut u8;
                std::ptr::copy_nonoverlapping(record_data.as_ptr(), dst1, first_len);
                let dst2 = self.data.as_ptr() as *mut u8;
                std::ptr::copy_nonoverlapping(record_data.as_ptr().add(first_len), dst2, record_data.len() - first_len);
            }
        }

        // 4. 标记 insert 完成（flusher 扫描此标记确定连续区间）
        let entry_idx = (start as usize) & STATUS_ENTRY_MASK;
        self.insert_status[entry_idx].store(end, Ordering::Release);

        start
    }

    /// 扫描连续完成区间的最大位置（drain 线程调用）
    fn scan_continuous(&self) -> u64 {
        let mut pos = self.max_continuous.0.load(Ordering::Acquire);
        loop {
            let entry_idx = (pos as usize) & STATUS_ENTRY_MASK;
            let end = self.insert_status[entry_idx].load(Ordering::Acquire);
            if end > pos {
                pos = end;
            } else {
                break;
            }
        }
        self.max_continuous.0.store(pos, Ordering::Release);
        pos
    }

    /// 获取待 flush 的数据。返回 (data_slice, new_flush_pos)。
    /// 可能返回空切片（无新数据）。
    pub fn get_flush_data(&self) -> (Vec<u8>, u64) {
        let flush = self.flush_pos.0.load(Ordering::Acquire);
        let continuous = self.scan_continuous();

        if continuous <= flush {
            return (Vec::new(), flush);
        }

        let len = (continuous - flush) as usize;
        let phys_start = (flush as usize) % self.size;
        let phys_end = phys_start + len;

        let mut result = Vec::with_capacity(len);
        if phys_end <= self.size {
            result.extend_from_slice(&self.data[phys_start..phys_end]);
        } else {
            // 跨回绕
            result.extend_from_slice(&self.data[phys_start..self.size]);
            result.extend_from_slice(&self.data[0..phys_end - self.size]);
        }

        (result, continuous)
    }

    /// 推进 flush 位置（drain 线程成功写入 WalWriter 后调用）
    pub fn advance_flush(&self, new_pos: u64) {
        self.flush_pos.0.store(new_pos, Ordering::Release);
    }

    /// 关闭信号
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Release);
    }

    /// 是否已关闭
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Acquire)
    }

    /// 当前写入位置
    pub fn write_pos(&self) -> u64 {
        self.write_pos.0.load(Ordering::Acquire)
    }

    /// 当前 flush 位置
    pub fn flush_pos(&self) -> u64 {
        self.flush_pos.0.load(Ordering::Acquire)
    }

    /// 未 flush 数据量
    pub fn unflushed(&self) -> u64 {
        self.write_pos().saturating_sub(self.flush_pos())
    }
}

/// 后台 drain 线程：持续将 WalRingBuffer 数据写入 WalWriter
pub fn start_drain_thread(
    ring: Arc<WalRingBuffer>,
    writer: Arc<super::writer::WalWriter>,
    interval_ms: u64,
) -> std::thread::JoinHandle<()> {
    std::thread::Builder::new()
        .name("wal-drain".into())
        .spawn(move || {
            while !ring.is_shutdown() {
                let (data, new_pos) = ring.get_flush_data();
                if !data.is_empty() {
                    if let Err(e) = writer.write(&data) {
                        eprintln!("[wal-drain] write failed: {:?}", e);
                    } else {
                        ring.advance_flush(new_pos);
                    }
                }
                std::thread::sleep(std::time::Duration::from_millis(interval_ms));
            }
            // 关闭前 flush 剩余数据
            let (data, new_pos) = ring.get_flush_data();
            if !data.is_empty() {
                let _ = writer.write(&data);
                ring.advance_flush(new_pos);
            }
        })
        .expect("Failed to spawn wal-drain thread")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ring_buffer_basic() {
        let rb = WalRingBuffer::new(1024);
        let pos = rb.write(b"hello world");
        assert_eq!(pos, 0);
        assert_eq!(rb.write_pos(), 11);
        assert_eq!(rb.flush_pos(), 0);
    }

    #[test]
    fn test_ring_buffer_flush() {
        let rb = WalRingBuffer::new(1024);
        rb.write(b"record_1");
        rb.write(b"record_2");

        let (data, new_pos) = rb.get_flush_data();
        assert_eq!(data.len(), 16); // "record_1" + "record_2"
        rb.advance_flush(new_pos);
        assert_eq!(rb.flush_pos(), 16);
    }

    #[test]
    fn test_ring_buffer_concurrent() {
        let rb = Arc::new(WalRingBuffer::new(64 * 1024));
        let mut handles = vec![];

        for t in 0..4 {
            let rb = Arc::clone(&rb);
            handles.push(std::thread::spawn(move || {
                for i in 0..1000 {
                    rb.write(format!("t{}_{:04}", t, i).as_bytes());
                }
            }));
        }
        for h in handles { h.join().unwrap(); }

        // 4000 records written
        assert!(rb.write_pos() > 0);
        let (data, _) = rb.get_flush_data();
        assert!(!data.is_empty());
    }

    #[test]
    fn test_ring_buffer_wrap_around() {
        let rb = WalRingBuffer::new(128); // 小 buffer 触发 wrap
        // 写入超过 buffer 大小的数据（需要先 flush 才能继续）
        let rb = Arc::new(rb);
        let rb2 = Arc::clone(&rb);

        // 后台 flush 线程
        let flusher = std::thread::spawn(move || {
            for _ in 0..100 {
                let (_, new_pos) = rb2.get_flush_data();
                rb2.advance_flush(new_pos);
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });

        // 写入多次（总量 > 128 bytes）
        for i in 0..20 {
            rb.write(format!("wrap_{:04}", i).as_bytes()); // ~9 bytes each = 180 total
        }

        flusher.join().unwrap();
    }
}
