# BTree 并发 Insert 方案设计

> 参照 C++ dstore `dstore_btree_split.cpp`

## 1. C++ dstore 实际协议（非教科书 lock coupling）

C++ dstore 不是同时持父子两把锁下降（教科书 lock coupling），而是：

### 下降阶段
```
释放父 → 锁子（sequential，非 coupled）
和我们当前完全一样
```

### Split 阶段（关键区别）
```
1. Leaf 满了 → 持 leaf EXCLUSIVE 锁
2. 在 leaf 锁内创建 right page、rebuild left、设 right_link
3. 标记 left page 为 "SPLIT_IN_PROGRESS"（incomplete split）
4. 释放 right page 锁（安全：parent 还没指向它）
5. 仍持 leaf 锁 → 找到 parent → 锁 parent（同时持两个锁）
6. 在 parent 中插入 downlink → 标记 "SPLIT_COMPLETE"
7. 释放两个锁
```

### Incomplete Split 协助机制
```
如果线程 A split 后 crash 或被抢占（步骤 3-7 之间）：
- left page 标记为 SPLIT_IN_PROGRESS
- parent 没有 downlink 到 right page
任何后续线程下降经过这个 page 时：
- 检测到 SPLIT_IN_PROGRESS
- 调用 CompleteSplit() 帮忙完成步骤 5-7
- 然后继续正常操作
```

## 2. Rust 实现方案

### 核心改动

**不改下降方式**（已经是 sequential），只改 split 传播机制：

```
当前（split_mutex 全局锁）：
  insert() {
    split_mutex.lock()          // 全局串行
    insert_or_split(root, item)
    split_mutex.unlock()
  }

改为（incomplete split 协议）：
  insert() {
    loop {
      match insert_or_split(root, item) {
        Ok(None) => return Ok(()),     // 常见路径，无锁
        Ok(Some(split)) => {
          // root split 需要原子操作
          let _guard = split_mutex.lock()  // 仅 root split 取锁
          if root 没变: handle_root_split(split)
          continue  // retry
        }
        Err(retry) => continue,        // incomplete split 检测到，retry
      }
    }
  }
```

### BTreePageHeader 增加字段
```rust
pub struct BTreePageHeader {
    // ... existing fields ...
    /// Split 状态：0=complete, 1=in_progress
    pub split_status: u8,
}
```

### insert_or_split 改动

**Leaf 正常 insert（无变化）**：持 page exclusive lock → insert → 释放。

**Leaf split（核心改动）**：
```rust
fn insert_or_split(page_no, item) {
    let pinned = bp.pin(page_no);
    let lock = pinned.lock_exclusive();
    
    if page.is_leaf() {
        if page.has_free_space(item) {
            page.insert(item);          // 常见路径
            drop(lock);
            return Ok(None);
        }
        
        // Split: 持锁完成
        let (left_items, right_items, separator) = split(page, item);
        let right_page = allocate_page();
        
        // 标记 incomplete split
        page.header_mut().split_status = 1;  // IN_PROGRESS
        page.rebuild(left_items);
        page.header_mut().right_link = right_page;
        
        // 构建 right page
        build_right_page(right_page, right_items);
        
        drop(lock);  // 释放 leaf 锁
        
        // 向 parent 传播 separator
        // 这里不需要全局锁——因为 parent 有自己的 page lock
        return self.propagate_to_parent(page_no, separator, right_page);
    }
    
    // Internal node: 下降
    let child = page.find_child(item.key);
    drop(lock);
    
    match self.insert_or_split(child, item)? {
        None => Ok(None),
        Some(split) => self.insert_separator_into(page_no, split),
    }
}
```

**propagate_to_parent（新函数）**：
```rust
fn propagate_to_parent(leaf_page, separator, right_page) {
    // 从 root 重新下降找到 parent（不持任何锁）
    let parent = self.find_parent(leaf_page);
    
    // 锁 parent
    let parent_pinned = bp.pin(parent);
    let parent_lock = parent_pinned.lock_exclusive();
    
    // 插入 downlink
    if parent_page.has_free_space(sep_item) {
        parent_page.insert(sep_item);
        
        // 标记 leaf split 完成
        let leaf_pinned = bp.pin(leaf_page);
        let leaf_lock = leaf_pinned.lock_exclusive();
        leaf_page.header_mut().split_status = 0;  // COMPLETE
        drop(leaf_lock);
        
        drop(parent_lock);
        return Ok(None);
    }
    
    // Parent 也满了 → 递归 split parent
    // ...similar logic...
}
```

**下降时检测 incomplete split**：
```rust
fn insert_or_split(page_no, item) {
    let pinned = bp.pin(page_no);
    let lock = pinned.lock_exclusive();
    
    // 检测 incomplete split（由之前的线程未完成）
    if page.header().split_status == 1 {
        // 帮忙完成 split
        self.complete_split(page_no, &page);
        drop(lock);
        return Err("retry");  // 完成后重试
    }
    // ... normal insert ...
}
```

## 3. 风险评估

| 风险 | 严重度 | 缓解 |
|------|--------|------|
| propagate_to_parent 找 parent 路径上有并发 split | MEDIUM | find_parent 用 right_link 跟踪 |
| 两个线程同时 complete_split 同一 page | LOW | exclusive lock 互斥 + 状态检查 |
| root split 和 leaf split 并发 | MEDIUM | root split 仍用 split_mutex |
| split_status 持久化 | LOW | WAL 记录 split 状态 |
| complete_split 找不到 parent | MEDIUM | 用 BtrStack 记录下降路径（或从 root 重新搜索） |

## 4. 最大风险点

**find_parent 的正确性**：propagate_to_parent 需要找到 leaf 的 parent。如果中间有并发 split，parent 可能变了。

C++ 用 `BtrStack`（下降时记录路径）来找 parent。但 stack 可能过时（并发 split 后 parent 变了）。C++ 的处理：
- 先尝试 stack 中的 parent
- 如果 parent 不对（downlink offset 不匹配），从 root 重新搜索
- 最多重试 10000 次

## 5. 简化方案（降低风险）

**不做完整的 incomplete split 协议，只做"split 时同时锁 parent"**：

```
改动最小化：
1. insert_or_split 递归下降时，在 parent 返回 SplitResult 时：
   - 重新锁 parent → 插入 separator
   - parent 也满了 → 返回 SplitResult 继续向上
2. 只有 root split 用 split_mutex
3. 不引入 split_status/incomplete split
```

这就是我们之前尝试的方案——但因 parent propagation 时 parent 已被并发修改而失败。

**根因**：两个线程 split 不同 leaf 后，同时 propagate 到同一 parent。parent exclusive lock 保证互斥，但第二个线程看到 parent nkeys 已变（第一个线程 split 了 parent），无法正确处理。

**C++ 的解法**：`m_needRetrySearchBtree = true` → 从 root 重新搜索。最多 10000 次。

## 6. 最终方案（推荐）

**保留 split_mutex，但缩小范围**：

```
insert() {
    loop {
        let root = self.root_page.load();
        
        // Phase 1: 无锁下降到 leaf，尝试 insert
        match self.try_insert_leaf(root, &item) {
            Ok(()) => return Ok(()),  // 常见路径：leaf 有空间
            Err(NeedSplit(leaf_page)) => {
                // Phase 2: 需要 split，取 split_mutex
                let _guard = self.split_mutex.lock();
                // 重新从 root 下降（树结构可能变了）
                match self.insert_or_split(root, &item) {
                    Ok(None) => return Ok(()),
                    Ok(Some(split)) => self.handle_root_split(split)?,
                    Err(_) => continue,
                }
            }
        }
    }
}
```

**优势**：
- 常见路径（leaf 有空间）完全无锁
- 只在 split 发生时（<1% 的 insert）取 split_mutex
- 正确性有保证（split_mutex 保护 split 路径）
- 改动量小（~50 行），风险可控

**性能预期**：
- 大多数 insert（>99%）走无锁路径
- Split 路径串行但极少发生
- 预计 16T 吞吐提升 2-5x（取决于 split 频率）
