//! Buffer Hash Table
//!
//! 用于快速查找 BufferTag 对应的 Buffer ID。

use ferrisdb_core::atomic::{AtomicU32, AtomicU64, Ordering};
use ferrisdb_core::{BufferTag, BufId};
use parking_lot::RwLock;

/// Hash 表默认大小（必须是 2 的幂次方）
pub const DEFAULT_HASH_TABLE_SIZE: usize = 1024;

/// Buffer 表条目
#[derive(Debug)]
#[repr(C)]
pub struct BufTableEntry {
    /// Buffer 标签
    pub tag: UnsafeCell<BufferTag>,
    /// Buffer ID
    pub buf_id: AtomicI32,
    /// 链表下一个（用于冲突处理）
    pub next: AtomicU32,
}

use std::cell::UnsafeCell;

/// Buffer ID 类型（可以为负）
pub type AtomicI32 = std::sync::atomic::AtomicI32;

impl BufTableEntry {
    /// 创建新的条目
    #[inline]
    pub const fn new() -> Self {
        Self {
            tag: UnsafeCell::new(BufferTag::INVALID),
            buf_id: AtomicI32::new(-1),
            next: AtomicU32::new(u32::MAX),
        }
    }
}

impl Default for BufTableEntry {
    fn default() -> Self {
        Self::new()
    }
}

/// Buffer Hash 表
///
/// 使用链地址法处理冲突。
pub struct BufTable {
    /// Hash 槽位数组
    buckets: Vec<AtomicU32>,
    /// 条目数组
    entries: Vec<BufTableEntry>,
    /// 空闲条目链表头
    free_list: AtomicU32,
    /// 保护 tag 读写（RwLock：lookup 取读锁，insert/remove 取写锁）
    lock: RwLock<()>,
    /// 掩码（用于取模）
    mask: usize,
}

// SAFETY: UnsafeCell<BufferTag> 通过分区 RwLock 保护所有读写
unsafe impl Send for BufTable {}
unsafe impl Sync for BufTable {}

impl BufTable {
    /// 创建新的 Buffer 表
    pub fn new(size: usize) -> Self {
        let size = size.next_power_of_two();
        let mut buckets = Vec::with_capacity(size);
        for _ in 0..size {
            buckets.push(AtomicU32::new(u32::MAX));
        }

        let entry_count = size * 2;
        let mut entries = Vec::with_capacity(entry_count);
        for i in 0..entry_count {
            let mut entry = BufTableEntry::new();
            entry.next = AtomicU32::new(if i + 1 < entry_count { (i + 1) as u32 } else { u32::MAX });
            entries.push(entry);
        }

        Self {
            buckets,
            entries,
            free_list: AtomicU32::new(0),
            lock: RwLock::new(()),
            mask: size - 1,
        }
    }

    /// 计算 hash 值
    #[inline]
    fn hash(&self, tag: &BufferTag) -> usize {
        tag.hash_value() as usize & self.mask
    }

    /// 查找 Buffer（读锁保护，防止 torn read）
    pub fn lookup(&self, tag: &BufferTag) -> Option<BufId> {
        let _guard = self.lock.read();
        let hash = self.hash(tag);
        let mut idx = self.buckets[hash].load(Ordering::Acquire);

        while idx != u32::MAX {
            let entry = &self.entries[idx as usize];
            // SAFETY: RwLock read guard prevents concurrent tag writes
            let entry_tag = unsafe { *entry.tag.get() };
            if entry_tag == *tag {
                let buf_id = entry.buf_id.load(Ordering::Acquire);
                if buf_id >= 0 {
                    return Some(buf_id);
                }
            }
            idx = entry.next.load(Ordering::Acquire);
        }

        None
    }

    /// 插入 Buffer
    ///
    /// 返回是否成功（失败表示已存在或表已满）
    pub fn insert(&self, tag: BufferTag, buf_id: BufId) -> bool {
        let _guard = self.lock.write();
        let hash = self.hash(&tag);

        // 检查是否已存在
        let mut idx = self.buckets[hash].load(Ordering::Acquire);
        while idx != u32::MAX {
            let entry = &self.entries[idx as usize];
            // SAFETY: We hold the lock, so this is safe
            let entry_tag = unsafe { *entry.tag.get() };
            if entry_tag == tag {
                return false; // 已存在
            }
            idx = entry.next.load(Ordering::Acquire);
        }

        // 从空闲链表获取一个条目
        let free_idx = self.free_list.load(Ordering::Acquire);
        if free_idx == u32::MAX {
            return false; // 表已满
        }

        let entry = &self.entries[free_idx as usize];
        self.free_list.store(entry.next.load(Ordering::Acquire), Ordering::Release);

        // 设置条目
        // SAFETY: We hold the lock, so this is safe
        unsafe {
            *entry.tag.get() = tag;
        }
        entry.buf_id.store(buf_id, Ordering::Release);

        // 插入到链表头部
        entry.next.store(self.buckets[hash].load(Ordering::Acquire), Ordering::Release);
        self.buckets[hash].store(free_idx, Ordering::Release);

        true
    }

    /// 删除 Buffer
    pub fn remove(&self, tag: &BufferTag) -> Option<BufId> {
        let _guard = self.lock.write();
        let hash = self.hash(tag);

        let mut prev_idx = u32::MAX;
        let mut idx = self.buckets[hash].load(Ordering::Acquire);

        while idx != u32::MAX {
            let entry = &self.entries[idx as usize];
            // SAFETY: We hold the lock, so this is safe
            let entry_tag = unsafe { *entry.tag.get() };
            if entry_tag == *tag {
                let buf_id = entry.buf_id.load(Ordering::Acquire);

                // 从链表中移除
                if prev_idx == u32::MAX {
                    self.buckets[hash].store(entry.next.load(Ordering::Acquire), Ordering::Release);
                } else {
                    self.entries[prev_idx as usize]
                        .next
                        .store(entry.next.load(Ordering::Acquire), Ordering::Release);
                }

                // 添加到空闲链表
                entry.next.store(self.free_list.load(Ordering::Acquire), Ordering::Release);
                self.free_list.store(idx, Ordering::Release);

                // 清理条目
                entry.buf_id.store(-1, Ordering::Release);

                return Some(buf_id);
            }

            prev_idx = idx;
            idx = entry.next.load(Ordering::Acquire);
        }

        None
    }

    /// 获取条目数量
    #[inline]
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// 获取桶数量
    #[inline]
    pub fn bucket_count(&self) -> usize {
        self.buckets.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ferrisdb_core::{FileTag, PdbId};

    #[test]
    fn test_buf_table_new() {
        let table = BufTable::new(1024);
        assert_eq!(table.bucket_count(), 1024);
        assert_eq!(table.entry_count(), 2048);
    }

    #[test]
    fn test_buf_table_insert_lookup() {
        let table = BufTable::new(16);
        let tag = BufferTag::new(PdbId::new(1), 100, 200);

        // 插入
        assert!(table.insert(tag, 42));

        // 查找
        assert_eq!(table.lookup(&tag), Some(42));

        // 重复插入失败
        assert!(!table.insert(tag, 43));

        // 查找不存在的
        let tag2 = BufferTag::new(PdbId::new(2), 100, 200);
        assert_eq!(table.lookup(&tag2), None);
    }

    #[test]
    fn test_buf_table_remove() {
        let table = BufTable::new(16);
        let tag = BufferTag::new(PdbId::new(1), 100, 200);

        // 插入
        assert!(table.insert(tag, 42));

        // 删除
        assert_eq!(table.remove(&tag), Some(42));

        // 再次查找
        assert_eq!(table.lookup(&tag), None);

        // 再次删除失败
        assert_eq!(table.remove(&tag), None);
    }
}
