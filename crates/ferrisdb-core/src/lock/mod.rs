//! 锁原语
//!
//! 提供 LWLock（轻量级锁）、ContentLock（紧凑内容锁）和 SpinLock（自旋锁）实现。

mod content_lock;
mod lwlock;
mod spinlock;
mod state_bits;

pub use content_lock::ContentLock;
pub use lwlock::{LockGuard, LWLock};
pub use spinlock::SpinLock;
pub use state_bits::*;

// Re-export LockMode from waiter module
pub use crate::waiter::LockMode;
