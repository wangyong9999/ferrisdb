//! Undo Zone
//!
//! 每个事务有一个 Undo Zone 用于存储 Undo Records。

use ferrisdb_core::Xid;
use ferrisdb_core::atomic::{AtomicU32, AtomicU64, Ordering};
use ferrisdb_core::error::{FerrisDBError, TransactionError};
use std::sync::Mutex;

/// Undo Zone 大小（页面数）
pub const UNDO_ZONE_SIZE: usize = 1024;

/// Undo Zone
///
/// 每个事务的 Undo 记录存储区域。
pub struct UndoZone {
    /// Zone ID
    zone_id: u32,
    /// 起始页面号
    start_page: u32,
    /// 当前页面号
    current_page: AtomicU32,
    /// 当前偏移
    current_offset: AtomicU32,
    /// 事务 ID
    xid: AtomicU64,
    /// 保护分配的锁
    lock: Mutex<()>,
}

impl UndoZone {
    /// 创建新的 Undo Zone
    pub fn new(zone_id: u32, start_page: u32) -> Self {
        Self {
            zone_id,
            start_page,
            current_page: AtomicU32::new(start_page),
            current_offset: AtomicU32::new(0),
            xid: AtomicU64::new(0),
            lock: Mutex::new(()),
        }
    }

    /// 获取 Zone ID
    #[inline]
    pub fn zone_id(&self) -> u32 {
        self.zone_id
    }

    /// 获取关联的事务 ID
    #[inline]
    pub fn xid(&self) -> Xid {
        Xid::from_raw(self.xid.load(Ordering::Acquire))
    }

    /// 设置事务 ID
    pub fn set_xid(&self, xid: Xid) {
        self.xid.store(xid.raw(), Ordering::Release);
    }

    /// 分配空间
    ///
    /// 返回 (page_no, offset)
    pub fn allocate(&self, size: u32) -> ferrisdb_core::Result<(u32, u32)> {
        let _guard = self.lock.lock().unwrap();

        let page_size = 8192u32;
        let mut page = self.current_page.load(Ordering::Acquire);
        let mut offset = self.current_offset.load(Ordering::Acquire);

        // 检查当前页面是否有足够空间
        if offset + size > page_size {
            // 需要新页面
            page += 1;
            offset = 0;

            // 检查是否超出 Zone 范围
            if page >= self.start_page + UNDO_ZONE_SIZE as u32 {
                return Err(FerrisDBError::Transaction(TransactionError::NoFreeSlot));
            }

            self.current_page.store(page, Ordering::Release);
        }

        let result = (page, offset);
        self.current_offset.store(offset + size, Ordering::Release);

        Ok(result)
    }

    /// 重置 Zone（事务结束时调用）
    pub fn reset(&self) {
        let _guard = self.lock.lock().unwrap();
        self.current_page.store(self.start_page, Ordering::Release);
        self.current_offset.store(0, Ordering::Release);
        self.xid.store(0, Ordering::Release);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_undo_zone_new() {
        let zone = UndoZone::new(0, 0);
        assert_eq!(zone.zone_id(), 0);
        assert!(!zone.xid().is_valid());
    }

    #[test]
    fn test_undo_zone_allocate() {
        let zone = UndoZone::new(0, 0);
        let xid = Xid::new(0, 1);
        zone.set_xid(xid);

        let (page, offset) = zone.allocate(100).unwrap();
        assert_eq!(page, 0);
        assert_eq!(offset, 0);

        let (page2, offset2) = zone.allocate(50).unwrap();
        assert_eq!(page2, 0);
        assert_eq!(offset2, 100);
    }

    #[test]
    fn test_undo_zone_reset() {
        let zone = UndoZone::new(0, 0);
        let xid = Xid::new(0, 1);
        zone.set_xid(xid);

        zone.allocate(100).unwrap();
        zone.reset();

        assert!(!zone.xid().is_valid());
    }
}
