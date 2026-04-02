//! WAL (Write-Ahead Logging) 模块
//!
//! 管理事务日志的写入和恢复。
//!
//! # 架构
//!
//! - `WalBuffer`: 内存缓冲区，批量写入优化
//! - `WalWriter`: 文件写入器，segment 管理
//! - `WalRecord*`: 各种 WAL 记录类型
//! - `WalReader`: WAL 记录读取器
//! - `WalRecovery`: WAL 恢复
//! - `ParallelRedoCoordinator`: 并行 Redo 协调器
//! - `CheckpointManager`: 检查点管理器
//!
//! # 使用示例
//!
//! ```ignore
//! use ferrisdb_storage::wal::{WalBuffer, WalWriter, WalRecovery, CheckpointManager};
//!
//! let buffer = WalBuffer::new(16 * 1024 * 1024);
//! let writer = WalWriter::new("/var/lib/dstore/wal");
//!
//! // 写入 WAL 记录
//! let lsn = buffer.write(&record_data)?;
//! let flush_data = buffer.flush()?;
//! writer.write_and_sync(&flush_data.1)?;
//!
//! // 恢复
//! let recovery = WalRecovery::new("/var/lib/dstore/wal");
//! recovery.recover(RecoveryMode::CrashRecovery)?;
//!
//! // 检查点
//! let checkpoint = CheckpointManager::new(config, writer);
//! checkpoint.checkpoint(CheckpointType::Online)?;
//! ```

mod buffer;
mod checkpoint;
mod parallel_redo;
mod reader;
mod record;
mod recovery;
mod writer;

pub use buffer::{WalBuffer, DEFAULT_WAL_BUFFER_SIZE};
pub use checkpoint::{
    ActiveTransactionCollector,
    CheckpointConfig,
    CheckpointManager,
    CheckpointRecovery,
    CheckpointState,
    CheckpointStats,
    CheckpointType,
    DirtyPageFlusher,
};
pub use parallel_redo::{
    DefaultRedoCallback,
    ParallelRedoConfig,
    ParallelRedoCoordinator,
    ParallelRedoStats,
    RedoCallback,
    RedoRecord,
};
pub use reader::WalReader;
pub use record::{
    WalAtomicGroupHeader,
    WalCheckpoint,
    WalHeapDelete,
    WalHeapInplaceUpdate,
    WalHeapInsert,
    WalHeapSamePageAppend,
    WalHeapUpdateNewPage,
    WalHeapUpdateOldPage,
    WalRecordForPage,
    WalRecordHeader,
    WalRecordType,
    WalTxnCommit,
};
pub use recovery::{RecoveryMode, RecoveryStage, RecoveryStats, WalRecovery};
pub use writer::{
    WalFileHeader,
    WalFlusher,
    WalWriter,
    WAL_FILE_MAGIC,
    WAL_FILE_SIZE,
};
