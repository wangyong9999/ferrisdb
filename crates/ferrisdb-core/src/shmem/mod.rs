//! 共享内存工具
//!
//! 提供共享内存兼容的基础设施，包括内存布局计算、偏移算术等。

mod layout;
mod offset;

pub use layout::{align_up, is_aligned, CacheAligned};
pub use offset::{Offset, OffsetPtr};
