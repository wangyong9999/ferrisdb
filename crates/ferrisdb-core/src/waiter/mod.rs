//! LWLock 等待者节点
//!
//! 独立于 LWLock 和 ThreadCore，避免循环依赖。

mod lwlock_waiter;

pub use lwlock_waiter::{LockMode, LWLockWaiter};
