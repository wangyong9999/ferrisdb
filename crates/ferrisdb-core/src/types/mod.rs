//! 核心类型定义
//!
//! 定义存储引擎使用的基础类型，与 C++ 版本二进制兼容。

mod buffer_tag;
mod buf_id;
mod csn;
mod file_tag;
mod lsn;
mod page_id;
mod pdb_id;
mod snapshot;
mod xid;

pub use buffer_tag::BufferTag;
pub use buf_id::BufId;
pub use csn::{Csn, CsnAllocator};
pub use file_tag::FileTag;
pub use lsn::{Glsn, Lsn, LsnAllocator, Plsn};
pub use page_id::{PageId, PageNo, PAGE_MASK, PAGE_SIZE};
pub use pdb_id::PdbId;
pub use snapshot::{Snapshot, SnapshotData, SnapshotManager};
pub use xid::{AtomicXid, Xid};
