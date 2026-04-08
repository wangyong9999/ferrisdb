# 大事务 Undo Spill-to-Disk 设计

## 1. 当前限制

- `undo_log: Vec<UndoAction>` 纯内存，1M 条上限
- 每条 UndoAction 含 old_data (Vec<u8>)
- 100 万条 × 100B = 100MB；100 万条 × 1KB = 1GB
- 超限报错，事务中断

## 2. 业界方案对比

| 系统 | 方案 | 特点 |
|------|------|------|
| PostgreSQL | 无 undo log，旧版本 in-place 存堆页 | 最简单，但 vacuum 压力大 |
| InnoDB | Undo tablespace segments，循环复用 | 完整但复杂 |
| Oracle | 专用 undo tablespace，自动扩展 | 生产级 |
| C++ dstore | Undo zone + varint 压缩 + 异步 worker | 高性能 |

## 3. 推荐方案：分级 undo（内存 + 临时文件）

**设计原则**：简单、可靠、无架构腐化。

```
层级 1：内存 Vec（≤100K 条）—— 99% 事务走这里
层级 2：临时文件（>100K 条）—— 大批量事务自动 spill
```

### 实现

```rust
struct UndoLog {
    /// 内存中的 undo entries（快速路径）
    mem: Vec<UndoAction>,
    /// 溢出到磁盘的临时文件
    spill_file: Option<File>,
    /// 溢出的 entry 数量
    spill_count: usize,
    /// 溢出阈值
    spill_threshold: usize,  // default 100_000
}

impl UndoLog {
    fn push(&mut self, action: UndoAction) -> Result<()> {
        if self.mem.len() < self.spill_threshold {
            self.mem.push(action);
        } else {
            // 序列化到临时文件：[len:4][serialized_action]
            let file = self.ensure_spill_file()?;
            let bytes = action.serialize();
            file.write_all(&(bytes.len() as u32).to_le_bytes())?;
            file.write_all(&bytes)?;
            self.spill_count += 1;
        }
        Ok(())
    }
    
    fn apply_undo_reverse(&mut self, bp: &BufferPool) {
        // 1. 先 apply 内存中的（逆序）
        for action in self.mem.iter().rev() {
            apply_single_undo(action, bp);
        }
        // 2. 再 apply 磁盘上的（逆序读取）
        if let Some(file) = &self.spill_file {
            let entries = self.read_spill_reverse(file);
            for action in entries {
                apply_single_undo(&action, bp);
            }
        }
    }
}
```

### 逆序读取方案

临时文件格式：`[len:4][data:len] [len:4][data:len] ...`

逆序读取：
1. 获取文件大小
2. 从尾部向前读取：先读最后 4 字节得到 len，再回退 len 字节读 data
3. 重复直到文件头

或更简单：维护一个 offset 索引数组（Vec<u64>），记录每条 entry 的文件偏移。
逆序遍历 offset 数组，seek 到对应位置读取。

## 4. 风险评估

| 风险 | 严重度 | 缓解 |
|------|--------|------|
| 临时文件 IO 开销 | LOW | 只有 >100K 条的大事务走磁盘 |
| 临时文件清理 | LOW | commit/abort 后删除 |
| crash 后临时文件残留 | LOW | 启动时扫描删除 |
| 序列化格式兼容 | NONE | UndoAction 已有 serialize_for_wal |
