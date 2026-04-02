//! Large Object (LOB) 支持 — 超页面大小数据的分片存储
//!
//! 当 tuple 数据超过单页可存储上限时，将数据分成多个 chunk 存储，
//! 主 tuple 存储 LOB 头部（chunk 列表），实际数据存在独立的 LOB 页面。

use ferrisdb_core::{Result, FerrisDBError};

/// LOB chunk 大小（每个 chunk 最多存储的数据量）
pub const LOB_CHUNK_SIZE: usize = 7000;

/// LOB 头部 — 存储在主 tuple 中
#[derive(Debug, Clone)]
pub struct LobHeader {
    /// 总数据大小
    pub total_size: u32,
    /// chunk 数量
    pub chunk_count: u16,
    /// LOB ID（用于定位 chunk 页面）
    pub lob_id: u32,
}

impl LobHeader {
    /// 创建 LOB 头部
    pub fn new(total_size: u32, lob_id: u32) -> Self {
        let chunk_count = ((total_size as usize + LOB_CHUNK_SIZE - 1) / LOB_CHUNK_SIZE) as u16;
        Self { total_size, chunk_count, lob_id }
    }

    /// 序列化（10 字节）
    pub fn to_bytes(&self) -> [u8; 10] {
        let mut buf = [0u8; 10];
        buf[0..4].copy_from_slice(&self.total_size.to_le_bytes());
        buf[4..6].copy_from_slice(&self.chunk_count.to_le_bytes());
        buf[6..10].copy_from_slice(&self.lob_id.to_le_bytes());
        buf
    }

    /// 反序列化
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 10 { return None; }
        Some(Self {
            total_size: u32::from_le_bytes(data[0..4].try_into().ok()?),
            chunk_count: u16::from_le_bytes(data[4..6].try_into().ok()?),
            lob_id: u32::from_le_bytes(data[6..10].try_into().ok()?),
        })
    }
}

/// LOB chunk
#[derive(Debug, Clone)]
pub struct LobChunk {
    /// chunk 序号
    pub index: u16,
    /// 数据
    pub data: Vec<u8>,
}

/// LOB 存储 — 内存中的 LOB 管理（简化实现）
pub struct LobStore {
    /// lob_id → chunks
    lobs: parking_lot::RwLock<std::collections::HashMap<u32, Vec<Vec<u8>>>>,
    /// 下一个 LOB ID
    next_id: std::sync::atomic::AtomicU32,
}

impl LobStore {
    /// 创建空的 LOB 存储
    pub fn new() -> Self {
        Self {
            lobs: parking_lot::RwLock::new(std::collections::HashMap::new()),
            next_id: std::sync::atomic::AtomicU32::new(1),
        }
    }

    /// 存储大对象，返回 LobHeader
    pub fn store(&self, data: &[u8]) -> Result<LobHeader> {
        let lob_id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        let chunks: Vec<Vec<u8>> = data.chunks(LOB_CHUNK_SIZE)
            .map(|c| c.to_vec())
            .collect();
        let header = LobHeader::new(data.len() as u32, lob_id);
        self.lobs.write().insert(lob_id, chunks);
        Ok(header)
    }

    /// 读取大对象
    pub fn fetch(&self, lob_id: u32) -> Result<Vec<u8>> {
        let lobs = self.lobs.read();
        let chunks = lobs.get(&lob_id)
            .ok_or_else(|| FerrisDBError::Internal("LOB not found".to_string()))?;
        let mut result = Vec::new();
        for chunk in chunks {
            result.extend_from_slice(chunk);
        }
        Ok(result)
    }

    /// 删除大对象
    pub fn delete(&self, lob_id: u32) -> Result<()> {
        self.lobs.write().remove(&lob_id);
        Ok(())
    }

    /// LOB 数量
    pub fn count(&self) -> usize {
        self.lobs.read().len()
    }

    /// 判断数据是否需要 LOB 存储
    pub fn needs_lob(data_size: usize) -> bool {
        data_size > LOB_CHUNK_SIZE
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test] fn test_lob_header_roundtrip() { let h = LobHeader::new(15000, 42); let b = h.to_bytes(); let r = LobHeader::from_bytes(&b).unwrap(); assert_eq!(r.total_size, 15000); assert_eq!(r.lob_id, 42); assert_eq!(r.chunk_count, 3); }
    #[test] fn test_lob_header_single_chunk() { let h = LobHeader::new(1000, 1); assert_eq!(h.chunk_count, 1); }
    #[test] fn test_lob_header_exact_boundary() { let h = LobHeader::new(LOB_CHUNK_SIZE as u32, 1); assert_eq!(h.chunk_count, 1); }
    #[test] fn test_lob_header_empty() { assert!(LobHeader::from_bytes(&[]).is_none()); }
    #[test] fn test_lob_store_small() { let s = LobStore::new(); let data = vec![0xAA; 5000]; let h = s.store(&data).unwrap(); let r = s.fetch(h.lob_id).unwrap(); assert_eq!(r, data); }
    #[test] fn test_lob_store_large() { let s = LobStore::new(); let data = vec![0xBB; 50000]; let h = s.store(&data).unwrap(); assert!(h.chunk_count > 1); let r = s.fetch(h.lob_id).unwrap(); assert_eq!(r, data); }
    #[test] fn test_lob_store_exact() { let s = LobStore::new(); let data = vec![0xCC; LOB_CHUNK_SIZE]; let h = s.store(&data).unwrap(); assert_eq!(h.chunk_count, 1); let r = s.fetch(h.lob_id).unwrap(); assert_eq!(r, data); }
    #[test] fn test_lob_delete() { let s = LobStore::new(); let h = s.store(b"delete_me").unwrap(); s.delete(h.lob_id).unwrap(); assert!(s.fetch(h.lob_id).is_err()); }
    #[test] fn test_lob_count() { let s = LobStore::new(); s.store(b"a").unwrap(); s.store(b"b").unwrap(); assert_eq!(s.count(), 2); }
    #[test] fn test_lob_needs() { assert!(!LobStore::needs_lob(100)); assert!(LobStore::needs_lob(LOB_CHUNK_SIZE + 1)); }
    #[test] fn test_lob_multiple() { let s = LobStore::new(); let mut ids = vec![]; for i in 0..10 { let data = vec![i as u8; (i+1) * 1000]; ids.push(s.store(&data).unwrap()); } for (i, h) in ids.iter().enumerate() { let r = s.fetch(h.lob_id).unwrap(); assert_eq!(r.len(), (i+1) * 1000); } }
    #[test] fn test_lob_concurrent() { let s = std::sync::Arc::new(LobStore::new()); let mut h = vec![]; for t in 0..4 { let s = s.clone(); h.push(std::thread::spawn(move || { for i in 0..20 { let data = vec![(t*20+i) as u8; 1000]; s.store(&data).unwrap(); } })); } for j in h { j.join().unwrap(); } assert_eq!(s.count(), 80); }
    #[test] fn test_lob_very_large() { let s = LobStore::new(); let data = vec![0xFF; 100_000]; let h = s.store(&data).unwrap(); assert!(h.chunk_count >= 14); let r = s.fetch(h.lob_id).unwrap(); assert_eq!(r.len(), 100_000); }
    #[test] fn test_lob_empty_data() { let s = LobStore::new(); let h = s.store(b"").unwrap(); let r = s.fetch(h.lob_id).unwrap(); assert_eq!(r.len(), 0); }
    #[test] fn test_lob_binary() { let s = LobStore::new(); let data: Vec<u8> = (0..=255).cycle().take(20000).collect(); let h = s.store(&data).unwrap(); let r = s.fetch(h.lob_id).unwrap(); assert_eq!(r, data); }
    #[test] fn test_lob_store_delete_reuse() { let s = LobStore::new(); let h1 = s.store(b"first").unwrap(); s.delete(h1.lob_id).unwrap(); let h2 = s.store(b"second").unwrap(); assert_ne!(h1.lob_id, h2.lob_id); assert_eq!(s.count(), 1); }
    #[test] fn test_lob_header_chunk_count() { assert_eq!(LobHeader::new(0, 1).chunk_count, 0); assert_eq!(LobHeader::new(1, 1).chunk_count, 1); assert_eq!(LobHeader::new(7000, 1).chunk_count, 1); assert_eq!(LobHeader::new(7001, 1).chunk_count, 2); }
    #[test] fn test_lob_fetch_nonexistent() { let s = LobStore::new(); assert!(s.fetch(999).is_err()); }
    #[test] fn test_lob_needs_boundary() { assert!(!LobStore::needs_lob(LOB_CHUNK_SIZE)); assert!(LobStore::needs_lob(LOB_CHUNK_SIZE + 1)); }
    #[test]
    fn test_lob_concurrent_store_fetch() {
        let s = std::sync::Arc::new(LobStore::new());
        let ids = std::sync::Arc::new(std::sync::Mutex::new(Vec::<(u32, Vec<u8>)>::new()));
        let mut h = vec![];
        for t in 0..4u8 {
            let s = s.clone();
            let ids = ids.clone();
            h.push(std::thread::spawn(move || {
                for i in 0..10u8 {
                    let data = vec![t * 10 + i; 5000];
                    let hdr = s.store(&data).unwrap();
                    ids.lock().unwrap().push((hdr.lob_id, data));
                }
            }));
        }
        for j in h { j.join().unwrap(); }
        let ids = ids.lock().unwrap();
        for (id, expected) in ids.iter() {
            assert_eq!(s.fetch(*id).unwrap(), *expected);
        }
    }
}
