//! Buffer ID 定义

/// Buffer ID 类型
///
/// 用于索引 Buffer Pool 中的 buffer。
pub type BufId = i32;

/// Buffer ID 常量
pub mod buf_id {
    use super::BufId;

    /// 无效 Buffer ID
    pub const INVALID: BufId = -1;

    /// 检查 Buffer ID 是否有效
    #[inline]
    pub const fn is_valid(buf_id: BufId) -> bool {
        buf_id >= 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buf_id_valid() {
        assert!(buf_id::is_valid(0));
        assert!(buf_id::is_valid(1));
        assert!(buf_id::is_valid(100));
        assert!(!buf_id::is_valid(-1));
        assert!(!buf_id::is_valid(buf_id::INVALID));
    }
}
