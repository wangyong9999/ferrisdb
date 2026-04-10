//! FerrisDB Storage Engine
//!
//! 存储引擎核心模块：Buffer Pool、Page、WAL、Storage Manager、Index。

#![deny(missing_docs)]
#![allow(dead_code)]
#![deny(unsafe_op_in_unsafe_fn)]
#![warn(clippy::all)]

pub mod buffer;
pub mod catalog;
pub mod row_codec;
pub mod control;
pub mod index;
pub mod lob;
pub mod page;
pub mod parallel_scan;
pub mod smgr;
pub mod tablespace;
pub mod wal;

// 重导出常用类型
pub use buffer::{BufferDesc, BufferPool, BufferPoolConfig, BufTable, LruQueue, PinnedBuffer, BackgroundWriter};
pub use index::{BTree, BTreeCursor, BTreeItem, BTreeKey, BTreePage, BTreePageType, BTreeStats, BTreeValue};
pub use page::{PageHeader, PageId, HeapPage, ItemIdData, ItemIdFlags, ItemPointerData};
pub use catalog::{SystemCatalog, RelationMeta, RelationType, ColumnDef, DataType};
pub use row_codec::{Value, encode_row, decode_row};
pub use control::{ControlFile, ControlFileData};
pub use lob::{LobStore, LobHeader, LOB_CHUNK_SIZE};
pub use parallel_scan::{ParallelScanCoordinator, parallel_scan};
pub use smgr::StorageManager;
pub use tablespace::{Tablespace, TablespaceManager, DEFAULT_EXTENT_SIZE};
pub use wal::{
    WalBuffer, WalWriter,
    WalHeapInsert, WalHeapDelete, WalHeapInplaceUpdate,
    WalHeapUpdateNewPage, WalHeapUpdateOldPage,
    WalRecordType, WalTxnCommit,
    WalRecovery, RecoveryMode,
};
