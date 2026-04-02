//! FerrisDB Core Library
//!
//! 核心类型、并发原语和线程抽象，支持共享内存兼容。
//!
//! # 模块层次
//!
//! ```text
//! Level 0 (无依赖):
//! ├── types/    - 核心类型 (Xid, Csn, Lsn, BufferTag)
//! ├── error/    - 错误类型
//! └── atomic/   - 原子操作扩展
//!
//! Level 1 (依赖 Level 0):
//! ├── list/     - 共享内存兼容链表
//! └── shmem/    - 共享内存工具
//!
//! Level 2 (依赖 Level 0-1):
//! └── waiter/   - LWLock 等待者节点
//!
//! Level 3 (依赖 Level 0-2):
//! ├── lock/     - LWLock, SpinLock
//! └── thread/   - ThreadCore 抽象
//! ```

#![deny(missing_docs)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]

pub mod atomic;
pub mod config;
pub mod error;
#[macro_use]
pub mod log;
pub mod stats;
pub mod list;
pub mod lock;
pub mod shmem;
pub mod thread;
pub mod types;
pub mod waiter;

// 重导出常用类型
pub use error::{BufferError, FerrisDBError, LockError, Result, TransactionError};
pub use types::{
    AtomicXid, BufferTag, BufId, Csn, CsnAllocator, FileTag, Glsn, Lsn, LsnAllocator, PageId,
    PageNo, PdbId, Plsn, Snapshot, SnapshotData, SnapshotManager, Xid, PAGE_MASK, PAGE_SIZE,
};
