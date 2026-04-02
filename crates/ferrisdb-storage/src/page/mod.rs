//! Page 定义
//!
//! 磁盘页面布局和数据结构。

mod header;
mod heap_page;

pub use header::{PageHeader, PageId, PageNo, PageType, PAGE_SIZE, PAGE_MASK};
pub use heap_page::{HeapPage, HeapPageHeader, ItemIdData, ItemIdFlags, ItemPointerData};
