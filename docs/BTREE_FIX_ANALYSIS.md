# BTree 并发 Insert 失败根因分析与修复方案

## 1. 4 次失败的共同根因

**不是 rebuild 覆盖，而是"路由过时"(stale routing)。**

```
Thread A: 读 internal I，得到 child = Leaf L，释放 I 的锁
Thread B: split L 为 L(左半)/L'(右半)，separator S 插入 parent I
Thread A: 锁 L，发现有空间（左半变小了），insert
          → 但 A 的 key > S，应该去 L' → 错误插入到 L → 数据丢失
```

## 2. 为什么 lookup 的 right_link 不受影响

lookup 已经实现了：如果 key 在当前 leaf 找不到，检查 right_link → 跟踪到右兄弟。
所以 lookup 总能找到正确的数据。

## 3. 修复方案

**在 insert 的叶子路径也加 right_link 检查**（和 lookup 一样）：

```rust
fn try_insert_no_split(page_no, item) {
    let mut current = page_no;
    loop {
        lock leaf exclusively
        
        // 关键修复：检查 key 是否属于右兄弟（路由过时检测）
        if right_link 有效 && page 有 items {
            let max_key = page.last_key();
            if item.key > max_key {
                // key 属于右兄弟，跟踪 right_link
                current = right_link;
                release lock;
                continue;  // 重试右页
            }
        }
        
        // key 属于当前页
        if has_free_space {
            insert(item);  // 安全：key 确实在本页范围内
            return Ok(true);
        }
        return Ok(false);  // 需要 split
    }
}
```

## 4. 正确性论证

**为什么 right_link 检查足够？**

- Split 把 leaf L 分为 L(items < S) 和 L'(items >= S)
- L.right_link = L'
- Thread A 到达 L 后：
  - 如果 A.key < S → A.key ≤ max_key(L) → 检查通过 → insert 到 L ✓
  - 如果 A.key ≥ S → A.key > max_key(L) → 检查失败 → 跟踪到 L' → insert 到 L' ✓
- 如果 L' 也被 split 了？→ 到 L' 后再次检查 → 跟踪到 L'' → 最终找到正确 leaf

**为什么不需要 incomplete-split 协议？**

因为我们只是把"找正确 leaf"这一步从全局锁（split_mutex）下移到了 page lock + right_link 检查。
Split 本身仍然在 split_mutex 下执行（正确性保证）。
核心创新：**常见路径（leaf 有空间 + key 在本页范围内）完全无全局锁。**

## 5. 风险评估

| 风险 | 严重度 | 分析 |
|------|--------|------|
| right_link 链太长导致延迟 | LOW | 通常只跳 1-2 次，split 后右兄弟就是目标 |
| leaf split 和 right_link 检查的竞态 | NONE | right_link 在 split 持锁期间设置，后续 reader 看到的是一致的 |
| internal node split 后路由完全失效 | LOW | 最终总能到某个 leaf，right_link 链会指向正确位置 |

## 6. 与 C++ dstore 的对比

| 维度 | C++ dstore | 本方案 |
|------|-----------|--------|
| 下降方式 | 释放父→锁子（sequential） | 相同 |
| leaf 路由修正 | StepRightIfNeeded | right_link 检查 |
| split 保护 | 无全局锁（lock coupling） | split_mutex（仅 split 路径） |
| 常见路径锁 | page lock only | page lock only ✅ |
