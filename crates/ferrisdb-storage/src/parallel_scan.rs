//! Parallel Scan — 多线程分页表扫描
//!
//! 将表的页面范围分配给多个 worker 线程并行扫描，汇总结果。

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

/// 并行扫描协调器
pub struct ParallelScanCoordinator {
    /// 总页面数
    total_pages: u32,
    /// 下一个待分配的页号
    next_page: AtomicU32,
    /// 每次分配的页面数（batch size）
    batch_size: u32,
}

impl ParallelScanCoordinator {
    /// 创建协调器
    pub fn new(total_pages: u32, batch_size: u32) -> Self {
        Self {
            total_pages,
            next_page: AtomicU32::new(0),
            batch_size: if batch_size == 0 { 1 } else { batch_size },
        }
    }

    /// 获取下一批页面范围 (start, end)，返回 None 表示扫描完成
    pub fn next_batch(&self) -> Option<(u32, u32)> {
        let start = self.next_page.fetch_add(self.batch_size, Ordering::AcqRel);
        if start >= self.total_pages {
            return None;
        }
        let end = (start + self.batch_size).min(self.total_pages);
        Some((start, end))
    }

    /// 重置扫描
    pub fn reset(&self) {
        self.next_page.store(0, Ordering::Release);
    }

    /// 总页面数
    pub fn total_pages(&self) -> u32 { self.total_pages }

    /// 批大小
    pub fn batch_size(&self) -> u32 { self.batch_size }
}

/// 并行扫描多个 worker，返回每个 worker 扫描的页面范围列表
pub fn parallel_scan<F, R>(total_pages: u32, num_workers: usize, batch_size: u32, worker_fn: F) -> Vec<R>
where
    F: Fn(u32, u32) -> R + Send + Sync + 'static,
    R: Send + 'static,
{
    let coord = Arc::new(ParallelScanCoordinator::new(total_pages, batch_size));
    let worker_fn = Arc::new(worker_fn);
    let mut handles = Vec::with_capacity(num_workers);

    for _ in 0..num_workers {
        let coord = Arc::clone(&coord);
        let f = Arc::clone(&worker_fn);
        handles.push(std::thread::spawn(move || {
            let mut results = Vec::new();
            while let Some((start, end)) = coord.next_batch() {
                results.push(f(start, end));
            }
            results
        }));
    }

    let mut all_results = Vec::new();
    for h in handles {
        all_results.extend(h.join().unwrap());
    }
    all_results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn test_coordinator_basic() { let c = ParallelScanCoordinator::new(100, 10); let (s, e) = c.next_batch().unwrap(); assert_eq!(s, 0); assert_eq!(e, 10); }
    #[test] fn test_coordinator_exhausts() { let c = ParallelScanCoordinator::new(20, 10); assert!(c.next_batch().is_some()); assert!(c.next_batch().is_some()); assert!(c.next_batch().is_none()); }
    #[test] fn test_coordinator_last_batch_partial() { let c = ParallelScanCoordinator::new(15, 10); let (_,e1) = c.next_batch().unwrap(); assert_eq!(e1, 10); let (_,e2) = c.next_batch().unwrap(); assert_eq!(e2, 15); }
    #[test] fn test_coordinator_reset() { let c = ParallelScanCoordinator::new(10, 5); c.next_batch(); c.next_batch(); c.reset(); assert!(c.next_batch().is_some()); }
    #[test] fn test_parallel_scan_sum() { let results = parallel_scan(100, 4, 10, |start, end| (end - start) as u64); let total: u64 = results.iter().sum(); assert_eq!(total, 100); }
    #[test] fn test_parallel_scan_1_worker() { let results = parallel_scan(50, 1, 10, |s, e| (s, e)); assert_eq!(results.len(), 5); }
    #[test] fn test_parallel_scan_many_workers() { let results = parallel_scan(100, 8, 5, |s, e| e - s); let total: u32 = results.iter().sum(); assert_eq!(total, 100); }
    #[test] fn test_coordinator_zero_pages() { let c = ParallelScanCoordinator::new(0, 10); assert!(c.next_batch().is_none()); }
    #[test] fn test_coordinator_batch_1() { let c = ParallelScanCoordinator::new(5, 1); for _ in 0..5 { assert!(c.next_batch().is_some()); } assert!(c.next_batch().is_none()); }
    #[test] fn test_parallel_concurrent_correctness() { let coord = Arc::new(ParallelScanCoordinator::new(1000, 1)); let seen = Arc::new(std::sync::Mutex::new(vec![false; 1000])); let mut h = vec![]; for _ in 0..4 { let c = coord.clone(); let s = seen.clone(); h.push(std::thread::spawn(move || { while let Some((start, _end)) = c.next_batch() { s.lock().unwrap()[start as usize] = true; } })); } for j in h { j.join().unwrap(); } assert!(seen.lock().unwrap().iter().all(|&v| v), "All pages should be scanned exactly once"); }
}
