//! 统一错误处理

use thiserror::Error;

/// FerrisDB 核心错误类型
#[derive(Debug, Error)]
pub enum FerrisDBError {
    /// Buffer 相关错误
    #[error("Buffer error: {0}")]
    Buffer(#[from] BufferError),

    /// 锁相关错误
    #[error("Lock error: {0}")]
    Lock(#[from] LockError),

    /// 事务相关错误
    #[error("Transaction error: {0}")]
    Transaction(#[from] TransactionError),

    /// WAL 相关错误
    #[error("WAL error: {0}")]
    Wal(#[from] WalError),

    /// IO 错误
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// 数据损坏
    #[error("Data corruption: {0}")]
    Corruption(String),

    /// 无效参数
    #[error("Invalid argument: {0}")]
    InvalidArgument(String),

    /// 状态错误
    #[error("Invalid state: {0}")]
    InvalidState(String),

    /// 超时
    #[error("Timeout: {0}")]
    Timeout(String),

    /// 未实现
    #[error("Not implemented: {0}")]
    NotImplemented(String),

    /// 内部错误
    #[error("Internal error: {0}")]
    Internal(String),

    /// 重复键
    #[error("Duplicate key: {0}")]
    DuplicateKey(String),

    /// 资源不存在
    #[error("Not found: {0}")]
    NotFound(String),
}

impl FerrisDBError {
    /// 数值错误码（用于应用程序区分错误类型）
    pub fn error_code(&self) -> u32 {
        match self {
            Self::Buffer(_) => 1000,
            Self::Lock(_) => 2000,
            Self::Transaction(_) => 3000,
            Self::Wal(_) => 4000,
            Self::Io(_) => 5000,
            Self::Corruption(_) => 6000,
            Self::InvalidArgument(_) => 7000,
            Self::InvalidState(_) => 7001,
            Self::Timeout(_) => 7002,
            Self::NotImplemented(_) => 7003,
            Self::Internal(_) => 9000,
            Self::DuplicateKey(_) => 8000,
            Self::NotFound(_) => 8001,
        }
    }

    /// 错误是否可恢复（应用可重试）
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Timeout(_) | Self::Lock(LockError::Deadlock))
    }
}

/// Buffer 相关错误
#[derive(Debug, Error)]
pub enum BufferError {
    /// 无效 Buffer ID
    #[error("Invalid buffer ID: {0}")]
    InvalidBufferId(i32),

    /// Buffer Pool 已满
    #[error("Buffer pool is full")]
    PoolFull,

    /// Buffer 未找到
    #[error("Buffer not found")]
    NotFound,

    /// Buffer 已被 Pin
    #[error("Buffer is pinned")]
    Pinned,

    /// IO 错误
    #[error("Buffer IO error: {0}")]
    Io(String),

    /// 页面损坏
    #[error("Page corruption detected")]
    Corruption,

    /// Hash 表已满
    #[error("Hash table is full")]
    TableFull,
}

/// 锁相关错误
#[derive(Debug, Error)]
pub enum LockError {
    /// 锁冲突
    #[error("Lock conflict")]
    Conflict,

    /// 死锁
    #[error("Deadlock detected")]
    Deadlock,

    /// 锁等待超时
    #[error("Lock wait timeout")]
    Timeout,

    /// 锁未持有
    #[error("Lock not held")]
    NotHeld,

    /// 无效锁模式
    #[error("Invalid lock mode")]
    InvalidMode,

    /// 会阻塞
    #[error("Lock would block")]
    WouldBlock,
}

/// 事务相关错误
#[derive(Debug, Error)]
pub enum TransactionError {
    /// 事务未找到
    #[error("Transaction not found")]
    NotFound,

    /// 事务状态错误
    #[error("Invalid transaction state: {0}")]
    InvalidState(String),

    /// 快照错误
    #[error("Snapshot error: {0}")]
    Snapshot(String),

    /// Undo 错误
    #[error("Undo error: {0}")]
    Undo(String),

    /// 2PC 错误
    #[error("2PC error: {0}")]
    TwoPhase(String),

    /// 无可用槽位
    #[error("No free slot available")]
    NoFreeSlot,

    /// 事务超时
    #[error("Transaction timeout")]
    Timeout,
}

/// WAL 相关错误
#[derive(Debug, Error)]
pub enum WalError {
    /// WAL Buffer 已满
    #[error("WAL buffer is full")]
    BufferFull,

    /// WAL 文件错误
    #[error("WAL file error: {0}")]
    FileError(String),

    /// WAL 记录损坏
    #[error("WAL record corruption")]
    Corruption,

    /// WAL LSN 无效
    #[error("Invalid WAL LSN")]
    InvalidLsn,

    /// WAL 写入失败
    #[error("WAL write failed: {0}")]
    WriteFailed(String),

    /// WAL 读取失败
    #[error("WAL read failed: {0}")]
    ReadFailed(String),
}

/// 结果类型别名
pub type Result<T> = std::result::Result<T, FerrisDBError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display() {
        let err = BufferError::PoolFull;
        assert_eq!(format!("{}", err), "Buffer pool is full");

        let err = LockError::Deadlock;
        assert_eq!(format!("{}", err), "Deadlock detected");
    }

    #[test]
    fn test_error_conversion() {
        let buffer_err = BufferError::NotFound;
        let dstore_err: FerrisDBError = buffer_err.into();
        assert!(matches!(dstore_err, FerrisDBError::Buffer(_)));
    }

    #[test]
    fn test_error_code() {
        assert_eq!(FerrisDBError::Buffer(BufferError::PoolFull).error_code(), 1000);
        assert_eq!(FerrisDBError::Lock(LockError::Deadlock).error_code(), 2000);
        assert_eq!(FerrisDBError::Transaction(TransactionError::NotFound).error_code(), 3000);
        assert_eq!(FerrisDBError::Wal(WalError::BufferFull).error_code(), 4000);
        assert_eq!(FerrisDBError::Corruption("x".into()).error_code(), 6000);
        assert_eq!(FerrisDBError::InvalidArgument("x".into()).error_code(), 7000);
        assert_eq!(FerrisDBError::InvalidState("x".into()).error_code(), 7001);
        assert_eq!(FerrisDBError::Timeout("x".into()).error_code(), 7002);
        assert_eq!(FerrisDBError::NotImplemented("x".into()).error_code(), 7003);
        assert_eq!(FerrisDBError::Internal("x".into()).error_code(), 9000);
        assert_eq!(FerrisDBError::DuplicateKey("x".into()).error_code(), 8000);
        assert_eq!(FerrisDBError::NotFound("x".into()).error_code(), 8001);
    }

    #[test]
    fn test_is_retryable() {
        assert!(FerrisDBError::Timeout("t".into()).is_retryable());
        assert!(FerrisDBError::Lock(LockError::Deadlock).is_retryable());
        assert!(!FerrisDBError::Internal("x".into()).is_retryable());
        assert!(!FerrisDBError::Buffer(BufferError::PoolFull).is_retryable());
    }

    #[test]
    fn test_all_error_types_display() {
        // Exercise Display for all variants
        let _ = format!("{}", FerrisDBError::Io(std::io::Error::new(std::io::ErrorKind::Other, "test")));
        let _ = format!("{}", WalError::FileError("f".into()));
        let _ = format!("{}", WalError::WriteFailed("w".into()));
        let _ = format!("{}", WalError::ReadFailed("r".into()));
        let _ = format!("{}", WalError::InvalidLsn);
        let _ = format!("{}", WalError::Corruption);
        let _ = format!("{}", TransactionError::InvalidState("s".into()));
        let _ = format!("{}", TransactionError::Timeout);
        let _ = format!("{}", TransactionError::NoFreeSlot);
        let _ = format!("{}", LockError::Conflict);
        let _ = format!("{}", LockError::Timeout);
        let _ = format!("{}", LockError::NotHeld);
        let _ = format!("{}", LockError::WouldBlock);
        let _ = format!("{}", BufferError::InvalidBufferId(0));
        let _ = format!("{}", BufferError::Pinned);
        let _ = format!("{}", BufferError::Io("io".into()));
        let _ = format!("{}", BufferError::Corruption);
        let _ = format!("{}", BufferError::TableFull);
    }
}
