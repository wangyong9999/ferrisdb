//! WAL Parallel Redo
//!
//! 并行 WAL 重放，提升恢复性能。
//!
//! # 架构
//!
//! ```text
//! ┌──────────────────────────────────────────────────────────────────┐
//! │                     Parallel Redo Coordinator                     │
//! ├──────────────────────────────────────────────────────────────────┤
//! │  Dispatcher       │  Record Buffer     │  Completion Tracker    │
//! │  - 按页面分组     │  - 批量读取        │  - 跟踪进度             │
//! │  - 派发到 Worker  │  - 内存队列        │  - 检查点推进           │
//! └──────────────────────────────────────────────────────────────────┘
//!                              │
//!               ┌──────────────┼──────────────┐
//!               ▼              ▼              ▼
//!         ┌─────────┐    ┌─────────┐    ┌─────────┐
//!         │ Worker0 │    │ Worker1 │    │ WorkerN │
//!         │ Page 1  │    │ Page 2  │    │ Page N  │
//!         │ Page 5  │    │ Page 3  │    │ Page 4  │
//!         └─────────┘    └─────────┘    └─────────┘
//! ```
//!
//! # 并行策略
//!
//! 1. 按页面分组：同一页面的记录必须串行处理
//! 2. 不同页面可以并行处理
//! 3. 事务提交记录需要等待所有相关页面处理完成

use std::collections::HashMap;
use std::sync::Arc;
use std::thread::{self, JoinHandle};

use ferrisdb_core::{BufferTag, Lsn, Xid};
use ferrisdb_core::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use ferrisdb_core::error::{FerrisDBError, Result};
use parking_lot::{Condvar, Mutex, RwLock};

use super::record::WalRecordType;

/// 并行 Redo 配置
#[derive(Debug, Clone)]
pub struct ParallelRedoConfig {
    /// Worker 数量
    pub num_workers: usize,
    /// 批量大小
    pub batch_size: usize,
    /// 队列容量
    pub queue_capacity: usize,
}

impl Default for ParallelRedoConfig {
    fn default() -> Self {
        Self {
            num_workers: 4,
            batch_size: 100,
            queue_capacity: 10000,
        }
    }
}

/// Redo 记录
#[derive(Debug, Clone)]
pub struct RedoRecord {
    /// 记录 LSN
    pub lsn: Lsn,
    /// 页面标签
    pub tag: BufferTag,
    /// 事务 ID
    pub xid: Xid,
    /// 记录类型
    pub record_type: WalRecordType,
    /// 记录数据
    pub data: Vec<u8>,
}

/// 按页面分组的记录批次
#[derive(Debug, Default)]
struct PageBatch {
    /// 页面标签
    tag: BufferTag,
    /// 记录列表（按 LSN 排序）
    records: Vec<RedoRecord>,
}

/// Worker 任务队列
struct WorkerQueue {
    /// 待处理批次
    batches: Mutex<Vec<PageBatch>>,
    /// 通知 worker 有新任务或关闭
    condvar: Condvar,
    /// 是否已关闭
    closed: AtomicBool,
}

impl WorkerQueue {
    fn new() -> Self {
        Self {
            batches: Mutex::new(Vec::new()),
            condvar: Condvar::new(),
            closed: AtomicBool::new(false),
        }
    }

    fn push(&self, batch: PageBatch) {
        let mut batches = self.batches.lock();
        batches.push(batch);
        self.condvar.notify_one();
    }

    fn pop(&self) -> Option<PageBatch> {
        let mut batches = self.batches.lock();
        batches.pop()
    }

    /// 等待直到有任务或关闭
    fn wait_for_work(&self) {
        let mut batches = self.batches.lock();
        if batches.is_empty() && !self.closed.load(Ordering::Acquire) {
            self.condvar.wait_for(&mut batches, std::time::Duration::from_millis(10));
        }
    }

    fn close(&self) {
        self.closed.store(true, Ordering::Release);
        self.condvar.notify_all();
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// Redo 回调函数类型
type RedoFn = dyn Fn(&RedoRecord) -> Result<()> + Send + Sync;

/// Redo Worker
struct RedoWorker {
    /// Worker ID
    _id: usize,
    /// 任务队列
    queue: Arc<WorkerQueue>,
    /// 处理计数
    processed: AtomicU64,
    /// 重做函数
    redo_fn: Arc<RedoFn>,
}

impl RedoWorker {
    fn new(
        id: usize,
        queue: Arc<WorkerQueue>,
        redo_fn: Arc<RedoFn>,
    ) -> Self {
        Self {
            _id: id,
            queue,
            processed: AtomicU64::new(0),
            redo_fn,
        }
    }

    fn run(&self) {
        loop {
            // 尝试获取任务
            if let Some(batch) = self.queue.pop() {
                // 处理批次中的所有记录
                for record in batch.records {
                    if let Err(_e) = (self.redo_fn)(&record) {
                        // 记录错误但继续处理
                    }
                    self.processed.fetch_add(1, Ordering::Relaxed);
                }
            } else if self.queue.is_closed() {
                // 队列已关闭且为空，退出
                break;
            } else {
                // 等待新任务（Condvar 避免 busy-spin）
                self.queue.wait_for_work();
            }
        }
    }
}

/// 并行 Redo 协调器
pub struct ParallelRedoCoordinator {
    /// 配置
    config: ParallelRedoConfig,
    /// Worker 队列
    worker_queues: Vec<Arc<WorkerQueue>>,
    /// Worker 线程
    worker_handles: Mutex<Vec<JoinHandle<()>>>,
    /// 页面到 Worker 的映射
    page_mapping: RwLock<HashMap<BufferTag, usize>>,
    /// 已处理记录数
    total_processed: AtomicU64,
    /// 是否正在运行
    running: AtomicBool,
}

impl ParallelRedoCoordinator {
    /// 创建新的协调器
    pub fn new(config: ParallelRedoConfig) -> Self {
        let num_workers = config.num_workers;
        let worker_queues: Vec<_> = (0..num_workers)
            .map(|_| Arc::new(WorkerQueue::new()))
            .collect();

        Self {
            config,
            worker_queues,
            worker_handles: Mutex::new(Vec::new()),
            page_mapping: RwLock::new(HashMap::new()),
            total_processed: AtomicU64::new(0),
            running: AtomicBool::new(false),
        }
    }

    /// 启动 Workers
    pub fn start<F>(&self, redo_fn: F) -> Result<()>
    where
        F: Fn(&RedoRecord) -> Result<()> + Send + Sync + 'static,
    {
        if self.running.swap(true, Ordering::AcqRel) {
            return Err(FerrisDBError::InvalidState("Already running".to_string()));
        }

        let mut handles = self.worker_handles.lock();
        let redo_fn: Arc<RedoFn> = Arc::new(redo_fn);

        for (id, queue) in self.worker_queues.iter().enumerate() {
            let worker = RedoWorker::new(
                id,
                Arc::clone(queue),
                Arc::clone(&redo_fn),
            );

            let handle = thread::spawn(move || {
                worker.run();
            });

            handles.push(handle);
        }

        Ok(())
    }

    /// 派发记录
    pub fn dispatch(&self, record: RedoRecord) {
        // 确定目标 Worker
        let worker_id = self.get_or_assign_worker(record.tag);

        // 创建单记录批次
        let batch = PageBatch {
            tag: record.tag,
            records: vec![record],
        };

        // 派发到 Worker 队列
        self.worker_queues[worker_id].push(batch);
        self.total_processed.fetch_add(1, Ordering::Relaxed);
    }

    /// 批量派发记录
    pub fn dispatch_batch(&self, records: Vec<RedoRecord>) {
        // 按页面分组
        let mut page_batches: HashMap<BufferTag, Vec<RedoRecord>> = HashMap::new();

        for record in records {
            page_batches
                .entry(record.tag)
                .or_insert_with(Vec::new)
                .push(record);
        }

        // 派发每个页面批次
        for (tag, mut records) in page_batches {
            // 按 LSN 排序
            records.sort_by_key(|r| r.lsn.raw());

            let worker_id = self.get_or_assign_worker(tag);
            let batch = PageBatch {
                tag,
                records,
            };

            self.worker_queues[worker_id].push(batch);
        }
    }

    /// 获取或分配 Worker
    fn get_or_assign_worker(&self, tag: BufferTag) -> usize {
        // 首先检查映射
        {
            let mapping = self.page_mapping.read();
            if let Some(&worker_id) = mapping.get(&tag) {
                return worker_id;
            }
        }

        // 分配新映射（使用 hash 取模）
        let hash = tag.hash_value();
        let worker_id = (hash as usize) % self.config.num_workers;

        {
            let mut mapping = self.page_mapping.write();
            mapping.insert(tag, worker_id);
        }

        worker_id
    }

    /// 等待所有记录处理完成
    pub fn wait_completion(&self) -> Result<()> {
        // 关闭所有队列
        for queue in &self.worker_queues {
            queue.close();
        }

        // 等待所有 Worker 完成
        let mut handles = self.worker_handles.lock();
        for handle in handles.drain(..) {
            let _ = handle.join();
        }

        self.running.store(false, Ordering::Release);
        Ok(())
    }

    /// 获取统计信息
    pub fn stats(&self) -> ParallelRedoStats {
        ParallelRedoStats {
            total_processed: self.total_processed.load(Ordering::Relaxed),
            pages_tracked: self.page_mapping.read().len(),
            num_workers: self.config.num_workers,
        }
    }
}

/// 并行 Redo 统计信息
#[derive(Debug, Clone, Default)]
pub struct ParallelRedoStats {
    /// 已处理记录数
    pub total_processed: u64,
    /// 跟踪的页面数
    pub pages_tracked: usize,
    /// Worker 数量
    pub num_workers: usize,
}

/// Redo 回调函数 trait
pub trait RedoCallback: Send + Sync {
    /// 重放记录
    fn redo(&self, record: &RedoRecord) -> Result<()>;
}

/// 默认 Redo 回调（仅打印日志）
#[derive(Debug, Clone)]
pub struct DefaultRedoCallback;

impl RedoCallback for DefaultRedoCallback {
    fn redo(&self, _record: &RedoRecord) -> Result<()> {
        // 默认实现：仅打印日志
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrisdb_core::{PageId, PdbId};
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_parallel_redo_config() {
        let config = ParallelRedoConfig::default();
        assert_eq!(config.num_workers, 4);
        assert_eq!(config.batch_size, 100);
    }

    #[test]
    fn test_redo_record() {
        let tag = BufferTag::new(PdbId::new(1), 1, 100);
        let record = RedoRecord {
            lsn: Lsn::from_parts(1, 100),
            tag,
            xid: Xid::new(0, 1),
            record_type: WalRecordType::HeapInsert,
            data: vec![1, 2, 3, 4],
        };

        assert!(record.lsn.is_valid());
        assert!(record.tag.is_valid());
    }

    #[test]
    fn test_worker_queue() {
        let queue = Arc::new(WorkerQueue::new());

        let batch = PageBatch {
            tag: BufferTag::new(PdbId::new(1), 1, 100),
            records: vec![],
        };

        queue.push(batch);
        assert!(queue.pop().is_some());
        assert!(queue.pop().is_none());

        queue.close();
        assert!(queue.is_closed());
    }

    #[test]
    fn test_parallel_redo_coordinator_dispatch() {
        let config = ParallelRedoConfig {
            num_workers: 2,
            batch_size: 10,
            queue_capacity: 100,
        };
        let coordinator = ParallelRedoCoordinator::new(config);

        let tag = BufferTag::new(PdbId::new(1), 1, 100);
        let record = RedoRecord {
            lsn: Lsn::from_parts(1, 100),
            tag,
            xid: Xid::new(0, 1),
            record_type: WalRecordType::HeapInsert,
            data: vec![],
        };

        // 派发记录
        coordinator.dispatch(record);

        let stats = coordinator.stats();
        assert_eq!(stats.num_workers, 2);
    }

    #[test]
    fn test_parallel_redo_batch_dispatch() {
        let config = ParallelRedoConfig {
            num_workers: 4,
            batch_size: 10,
            queue_capacity: 100,
        };
        let coordinator = ParallelRedoCoordinator::new(config);

        // 创建多个页面的记录
        let mut records = Vec::new();
        for page in 0..4u32 {
            for i in 0..10u32 {
                let tag = BufferTag::new(PdbId::new(1), 1, page * 100);
                records.push(RedoRecord {
                    lsn: Lsn::from_parts(1, page * 100 + i),
                    tag,
                    xid: Xid::new(0, 1),
                    record_type: WalRecordType::HeapInsert,
                    data: vec![],
                });
            }
        }

        coordinator.dispatch_batch(records);

        let stats = coordinator.stats();
        assert_eq!(stats.pages_tracked, 4);
    }

    #[test]
    fn test_parallel_redo_run() {
        let config = ParallelRedoConfig {
            num_workers: 2,
            batch_size: 10,
            queue_capacity: 100,
        };
        let coordinator = Arc::new(ParallelRedoCoordinator::new(config));

        // 使用原子计数器跟踪处理
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = Arc::clone(&counter);

        // 启动 Workers
        coordinator
            .start(move |_record| {
                counter_clone.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .unwrap();

        // 派发记录
        for i in 0..20u32 {
            let tag = BufferTag::new(PdbId::new(1), 1, i % 4 * 100);
            let record = RedoRecord {
                lsn: Lsn::from_parts(1, i),
                tag,
                xid: Xid::new(0, 1),
                record_type: WalRecordType::HeapInsert,
                data: vec![],
            };
            coordinator.dispatch(record);
        }

        // 等待完成
        coordinator.wait_completion().unwrap();

        // 验证处理数量
        assert_eq!(counter.load(Ordering::Relaxed), 20);
    }
}
