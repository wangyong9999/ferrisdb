//! 页面标识定义

use std::fmt;

/// 页面号
pub type PageNo = u32;

/// 页面大小（8KB）
pub const PAGE_SIZE: usize = 8192;

/// 页面掩码
pub const PAGE_MASK: usize = PAGE_SIZE - 1;

/// 页面 ID
///
/// 标识表空间内的一个页面。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(C)]
pub struct PageId {
    /// 表空间号
    pub tablespace: u32,
    /// 数据库号
    pub database: u32,
    /// 关系号
    pub relation: u32,
    /// 块号
    pub block: u32,
}

impl PageId {
    /// 创建新的页面 ID
    #[inline]
    pub const fn new(tablespace: u32, database: u32, relation: u32, block: u32) -> Self {
        Self {
            tablespace,
            database,
            relation,
            block,
        }
    }

    /// 无效页面 ID
    pub const INVALID: Self = Self::new(0, 0, 0, 0);

    /// 是否有效
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.tablespace != 0 || self.database != 0 || self.relation != 0
    }
}

impl fmt::Display for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PageId({}.{}.{}.{})",
            self.tablespace, self.database, self.relation, self.block
        )
    }
}
