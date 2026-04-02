//! 线程抽象
//!
//! 提供 ThreadCore trait，用于抽象线程操作，支持跨线程唤醒。

mod thread_core;

pub use thread_core::{StdThreadCore, ThreadCore, ThreadId};
