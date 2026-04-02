//! LWLock 状态字位定义
//!
//! 与 C++ 版本完全一致的 64 位状态字布局。

/// 低 32 位掩码
pub const LOW32_MASK: u64 = 0xFFFFFFFF;

/// 高 32 位起始位置
pub const HIGH32_SHIFT: u64 = 32;

// === 低 32 位 ===

/// 共享锁计数掩码 (bits 0-23)
pub const SHARE_COUNT_MASK: u64 = (1 << 24) - 1;

/// 已有共享锁标志 (bit 24)
pub const SHARED: u64 = 1 << 24;

/// 已有排他锁标志 (bit 25)
pub const EXCLUSIVE: u64 = 1 << 25;

/// 禁止抢占标志 (bit 26)
///
/// 当长时间持有锁时设置，防止被抢占
pub const DISALLOW_PREEMPT: u64 = 1 << 26;

/// 锁标志掩码
pub const LOCK_MASK: u64 = SHARED | EXCLUSIVE;

// === 高 32 位 ===

/// 有等待者标志 (bit 40)
pub const HAS_WAITERS: u64 = 1 << 40;

/// 可以唤醒等待者标志 (bit 41)
pub const RELEASE_OK: u64 = 1 << 41;

/// 等待队列已锁定标志 (bit 48)
pub const WAIT_LIST_LOCKED: u64 = 1 << 48;

/// 获取共享锁计数
#[inline]
pub const fn get_share_count(state: u64) -> u32 {
    (state & SHARE_COUNT_MASK) as u32
}

/// 检查是否有共享锁
#[inline]
pub const fn has_shared(state: u64) -> bool {
    (state & SHARED) != 0
}

/// 检查是否有排他锁
#[inline]
pub const fn has_exclusive(state: u64) -> bool {
    (state & EXCLUSIVE) != 0
}

/// 检查是否禁止抢占
#[inline]
pub const fn is_disallow_preempt(state: u64) -> bool {
    (state & DISALLOW_PREEMPT) != 0
}

/// 检查是否有等待者
#[inline]
pub const fn has_waiters(state: u64) -> bool {
    (state & HAS_WAITERS) != 0
}

/// 检查是否可以唤醒等待者
#[inline]
pub const fn is_release_ok(state: u64) -> bool {
    (state & RELEASE_OK) != 0
}

/// 检查等待队列是否已锁定
#[inline]
pub const fn is_wait_list_locked(state: u64) -> bool {
    (state & WAIT_LIST_LOCKED) != 0
}

/// 检查锁是否被持有（共享或排他）
#[inline]
pub const fn is_locked(state: u64) -> bool {
    (state & LOCK_MASK) != 0
}

/// 检查锁是否可被获取（无任何锁，无 DISALLOW_PREEMPT）
#[inline]
pub const fn can_acquire(state: u64) -> bool {
    (state & (LOCK_MASK | DISALLOW_PREEMPT)) == 0
}

/// 检查共享锁是否可被获取
///
/// 条件：已有共享锁且无排他锁且未禁止抢占
#[inline]
pub const fn can_acquire_shared(state: u64) -> bool {
    has_shared(state) && !has_exclusive(state) && !is_disallow_preempt(state)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bit_constants() {
        assert_eq!(SHARE_COUNT_MASK, 0x00FFFFFF);
        assert_eq!(SHARED, 0x01000000);
        assert_eq!(EXCLUSIVE, 0x02000000);
        assert_eq!(DISALLOW_PREEMPT, 0x04000000);
        assert_eq!(LOCK_MASK, 0x03000000);

        assert_eq!(HAS_WAITERS, 0x00000100_00000000);
        assert_eq!(RELEASE_OK, 0x00000200_00000000);
        assert_eq!(WAIT_LIST_LOCKED, 0x00010000_00000000);
    }

    #[test]
    fn test_helper_functions() {
        // 共享锁计数
        let state = 5u64; // 5 个共享锁
        assert_eq!(get_share_count(state), 5);
        assert!(!has_shared(state));

        // 有共享锁标志
        let state = SHARED | 3;
        assert!(has_shared(state));
        assert_eq!(get_share_count(state), 3);

        // 有排他锁
        let state = EXCLUSIVE;
        assert!(has_exclusive(state));
        assert!(is_locked(state));

        // 禁止抢占
        let state = DISALLOW_PREEMPT;
        assert!(is_disallow_preempt(state));

        // 等待者
        let state = HAS_WAITERS;
        assert!(has_waiters(state));

        // 可以获取（无任何锁）
        let state = 0u64;
        assert!(can_acquire(state));
        // can_acquire_shared 需要已有 SHARED 标志
        assert!(!can_acquire_shared(state));

        // 不可以获取（有排他锁）
        let state = EXCLUSIVE;
        assert!(!can_acquire(state));
        assert!(!can_acquire_shared(state));

        // 可以获取共享锁（已有共享锁，无排他锁）
        let state = SHARED | 2;
        assert!(!can_acquire(state));
        assert!(can_acquire_shared(state));
    }
}
