//! 日志框架 — 基于 tracing 的结构化日志
//!
//! 提供宏和工具函数，集成 GUC log_level 控制。

/// 日志级别
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u32)]
pub enum LogLevel {
    /// 关闭日志
    Off = 0,
    /// 错误
    Error = 1,
    /// 警告
    Warn = 2,
    /// 信息
    Info = 3,
    /// 调试
    Debug = 4,
}

impl From<u32> for LogLevel {
    fn from(v: u32) -> Self {
        match v {
            0 => Self::Off,
            1 => Self::Error,
            2 => Self::Warn,
            3 => Self::Info,
            4 => Self::Debug,
            _ => Self::Info,
        }
    }
}

/// 获取当前日志级别
pub fn current_level() -> LogLevel {
    LogLevel::from(crate::config::config().log_level.load(std::sync::atomic::Ordering::Relaxed))
}

/// 检查指定级别是否应输出
pub fn should_log(level: LogLevel) -> bool {
    level <= current_level() && level != LogLevel::Off
}

/// 输出日志（内部使用）
pub fn log_msg(level: LogLevel, module: &str, msg: &str) {
    if !should_log(level) { return; }
    let prefix = match level {
        LogLevel::Error => "ERROR",
        LogLevel::Warn => "WARN ",
        LogLevel::Info => "INFO ",
        LogLevel::Debug => "DEBUG",
        LogLevel::Off => return,
    };
    eprintln!("[{}] [{}] {}", prefix, module, msg);
}

/// 错误日志宏
#[macro_export]
macro_rules! dstore_error {
    ($module:expr, $($arg:tt)*) => {
        $crate::log::log_msg($crate::log::LogLevel::Error, $module, &format!($($arg)*))
    };
}

/// 警告日志宏
#[macro_export]
macro_rules! dstore_warn {
    ($module:expr, $($arg:tt)*) => {
        $crate::log::log_msg($crate::log::LogLevel::Warn, $module, &format!($($arg)*))
    };
}

/// 信息日志宏
#[macro_export]
macro_rules! dstore_info {
    ($module:expr, $($arg:tt)*) => {
        $crate::log::log_msg($crate::log::LogLevel::Info, $module, &format!($($arg)*))
    };
}

/// 调试日志宏
#[macro_export]
macro_rules! dstore_debug {
    ($module:expr, $($arg:tt)*) => {
        $crate::log::log_msg($crate::log::LogLevel::Debug, $module, &format!($($arg)*))
    };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn test_log_level_from() { assert_eq!(LogLevel::from(0), LogLevel::Off); assert_eq!(LogLevel::from(3), LogLevel::Info); }
    #[test] fn test_should_log() { assert!(should_log(LogLevel::Error)); assert!(should_log(LogLevel::Info)); }
    #[test] fn test_log_msg_no_panic() { log_msg(LogLevel::Info, "test", "hello"); log_msg(LogLevel::Debug, "test", "debug"); }
    #[test] fn test_macros() { dstore_error!("test", "error {}", 42); dstore_info!("test", "info"); }
}
