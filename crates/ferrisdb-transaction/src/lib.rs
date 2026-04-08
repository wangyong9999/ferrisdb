//! FerrisDB Transaction Management
//!
//! 事务管理、MVCC 可见性、Undo 和锁管理。

#![deny(missing_docs)]
#![allow(dead_code)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]

pub mod heap;
pub mod lockmgr;
pub mod ssi;
pub mod transaction;
pub mod undo;

// 重导出常用类型
pub use heap::{HeapScan, HeapTable, TupleHeader, TupleId};
pub use lockmgr::{DeadlockDetector, Lock, LockManager, LockMode as HeavyLockMode, LockTag};
pub use transaction::{Transaction, TransactionManager, TransactionState, UndoAction};
pub use undo::{UndoRecord, UndoRecordType, UndoZone};
pub use ssi::{SsiTracker, AccessKey};
