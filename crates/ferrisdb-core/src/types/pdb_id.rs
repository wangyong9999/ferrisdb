//! PDB ID 定义 (Pluggable Database ID)

use std::fmt;

/// PDB ID
///
/// 可插拔数据库标识符，用于多租户隔离。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(transparent)]
pub struct PdbId(u32);

impl PdbId {
    /// 无效 PDB ID
    pub const INVALID: Self = Self(0);

    /// 创建新的 PDB ID
    #[inline]
    pub const fn new(id: u32) -> Self {
        Self(id)
    }

    /// 是否有效
    #[inline]
    pub const fn is_valid(&self) -> bool {
        self.0 != Self::INVALID.0
    }

    /// 获取原始值
    #[inline]
    pub const fn raw(&self) -> u32 {
        self.0
    }

    /// 从原始值创建
    #[inline]
    pub const fn from_raw(raw: u32) -> Self {
        Self(raw)
    }
}

impl fmt::Display for PdbId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_valid() {
            write!(f, "PdbId({})", self.0)
        } else {
            write!(f, "PdbId(INVALID)")
        }
    }
}

impl From<u32> for PdbId {
    #[inline]
    fn from(id: u32) -> Self {
        Self::new(id)
    }
}

impl From<PdbId> for u32 {
    #[inline]
    fn from(pdb_id: PdbId) -> Self {
        pdb_id.raw()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pdb_id_valid() {
        let pdb = PdbId::new(123);
        assert!(pdb.is_valid());
        assert_eq!(pdb.raw(), 123);
    }

    #[test]
    fn test_pdb_id_invalid() {
        let pdb = PdbId::INVALID;
        assert!(!pdb.is_valid());
    }

    #[test]
    fn test_pdb_id_from_raw_default() {
        let pdb = PdbId::from_raw(42);
        assert_eq!(pdb.raw(), 42);
        let pdb2: PdbId = Default::default();
        let _ = format!("{:?}", pdb2);
        assert_eq!(pdb, PdbId::new(42));
    }
}
