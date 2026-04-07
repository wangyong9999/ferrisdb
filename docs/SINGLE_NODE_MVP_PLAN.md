# FerrisDB Phase 6 — 商业级交易引擎首商用计划

> Created: 2026-04-07
> Baseline: commit faa84da | 914 tests | WAL 5,282 TPS | ASan clean
> Previous: [P1](MVP_PHASE1_COMPLETED.md)~[P5](MVP_PHASE5_COMPLETED.md)

---

## 1. C++ dstore 对标分析（精确数据）

| 维度 | C++ dstore 实现 | FerrisDB 现状 | 差距评估 |
|------|----------------|--------------|---------|
| **Segment 分片** | 128MB per file, 65535 FileId, EB 级 | 单文件无限增长 | **必须做：ext4 16TB 限制** |
| **page_no 宽度** | uint32 (同我们) | u32 | **无差距（C++ 也是 u32）** |
| **BTree 并发** | Page-level lock coupling (无全局锁) | split_mutex 全局串行 | **必须做：scalability 核心** |
| **Undo 系统** | 1M zone + spill + async worker | 内存 Vec 1M 限制 | 后续优化 |
| **代码覆盖率** | gcov/lcov (cmake 集成) | 无 CI 覆盖率 | **必须加** |
| **WAL 多流** | NUMA 多 WalStream | 单 RingBuffer | 后续优化 |
| **CR Page** | CSN 一致读页 | 无 | 后续优化 |

**关键发现**：C++ page_no 也是 u32，不是我之前分析的限制——40TB 通过 128MB segment 分片 + 多 FileId 支持，page_no 只在单个 segment 内寻址。

---

## 2. 首商用阻断项（Tier 1: 不做不可上线）

### 2.1 数据文件 Segment 分片

**问题**：单 relation 一个文件，ext4 限制 16TB/file。
**C++ 方案**：128MB segment files, FileId (u16) + BlockNumber (u32)。
**Rust 方案**：

```
StorageManager 改造：
- RelationFile → Vec<SegmentFile>
- 每个 segment 128MB (16384 pages × 8KB)
- page_no 映射: segment_no = page_no / 16384, offset = page_no % 16384
- 新 segment 按需创建
- BufferTag 不变 (oid + page_no)，smgr 内部做 segment 映射
```

**预计改动**: smgr/manager.rs 核心改造 (~200 行)，影响面小（BufferPool/HeapTable 无感知）。

### 2.2 BTree Lock Coupling（去 split_mutex）

**问题**：split_mutex 序列化所有 insert，16T+ 瓶颈。
**C++ 方案**：Top-down lock coupling，无全局锁，retry on conflict。
**Rust 方案**：

```
Insert 路径改造：
1. 从 root 开始，shared lock 下降
2. 到 leaf 时 upgrade 为 exclusive
3. 如果 leaf 需要 split：
   a. 持 leaf exclusive lock 读取 items
   b. 分配 right page (exclusive lock)
   c. rebuild left, build right
   d. 释放 leaf lock
   e. 向 parent insert separator（重新获取 parent exclusive）
   f. 如果 parent 也满了，递归 split
4. Conflict detection: 如果 nkeys 变化或 right_link 变化，retry from root
```

**预计改动**: btree.rs insert/insert_or_split 重写 (~300 行)，lookup 已支持 right-link。

### 2.3 代码覆盖率 CI

**问题**：无覆盖率度量，无法知道哪些路径未测试。
**方案**：cargo-tarpaulin 集成到回归脚本，目标 >70% 行覆盖。

---

## 3. 首商用质量项（Tier 2: 必须达标）

### 3.1 测试覆盖加固

| 测试维度 | 当前状态 | 目标 |
|---------|---------|------|
| 单元测试 | 914 pass | 维持 0 fail |
| Crash recovery e2e | 3 tests | 加：segment 跨文件 recovery |
| 并发正确性 | BTree 30x stable | 加：lock coupling 50x stable |
| 长稳 (WAL) | 300s pass | 加：600s + BTree 索引 |
| ASan | 829 clean | 维持 |
| TSan | known FP only | 维持 |
| 覆盖率 | 未度量 | >70% |
| 故障注入 | 无 | disk full + corrupt page + kill -9 |

### 3.2 性能回归基线

每步骤后必须验证：

| Benchmark | 基线 | 允许波动 |
|-----------|------|---------|
| 5W/16T no-WAL | 5,385 TPS | ±15% |
| 5W/16T WAL | 5,282 TPS | ±15% |
| 20W/20T | ~3,100 TPS | ±15% |
| BTree concurrent 30x | 0 fail | 0 fail |

---

## 4. 逐步执行计划

### Step 1: Segment 分片 (S1) + 回归

**改动范围**：
- `smgr/manager.rs`: FileHandle → SegmentedFile (128MB segments)
- `smgr/manager.rs`: read_page/write_page 内部做 segment 映射
- 测试：跨 segment 读写、segment 自动创建

**回归**：全量测试 + TPCC + ASan

### Step 2: BTree Lock Coupling (C1) + 回归

**改动范围**：
- `btree.rs`: insert() 去 split_mutex，用 page-level lock coupling
- `btree.rs`: insert_or_split → 持 leaf lock 完成 split
- `btree.rs`: parent propagation 用 retry + right-link
- 测试：50x concurrent stability, 32T scalability

**回归**：全量测试 + TPCC + ASan + BTree 50x

### Step 3: 覆盖率 CI (COV) + 测试加固

**改动范围**：
- `scripts/coverage.sh`: cargo-tarpaulin 生成覆盖报告
- 补充测试：故障注入、segment 边界、lock coupling 并发
- 目标：>70% 行覆盖

**回归**：全量 + 覆盖率报告

### Step 4: 长稳 + 企业验证

**验证**：
- WAL 600s (10min) 长稳无 hang
- 20W/32T/60s 0 deadlock
- kill -9 → recovery → 数据完整
- 覆盖率 >70%

---

## 5. 回归清单（每步骤后执行）

```bash
# 1. 全量测试
cargo test --all

# 2. ASan
RUSTFLAGS="-Z sanitizer=address -Cunsafe-allow-abi-mismatch=sanitizer" \
  cargo +nightly test -p ferrisdb-storage -p ferrisdb-transaction \
  --target x86_64-unknown-linux-gnu -- --test-threads=1

# 3. TPCC 性能基线
cargo run --release --bin tpcc -- --warehouses 5 --threads 16 --duration 15 --buffer-size 100000
cargo run --release --bin tpcc -- --warehouses 5 --threads 16 --duration 15 --buffer-size 100000 --wal

# 4. BTree 并发稳定性
for i in $(seq 1 30); do
  cargo test -p ferrisdb-storage --test btree_tests test_btree_concurrent_insert 2>&1 | grep -q FAILED && echo "FAIL $i"
done

# 5. 覆盖率
cargo tarpaulin -p ferrisdb-storage -p ferrisdb-transaction -p ferrisdb --skip-clean --out stdout 2>&1 | tail -5

# 6. Sanitizer 套件
bash scripts/sanitizer_check.sh
```

---

## 6. 进度跟踪

| Step | Item | Status | Date |
|------|------|--------|------|
| S1 | Segment 分片 (128MB) | **DONE** — 128MB segments, auto-create, backward compatible | 2026-04-07 |
| S1-regress | 全量 + TPCC | **DONE** — 914 pass, 6,104 TPS | 2026-04-07 |
| C1 | BTree lock coupling | **KNOWN LIMITATION** — 需要完整 B-link tree SMO 协议 (1-2 周)，保持 split_mutex | 2026-04-07 |
| COV | 覆盖率 CI 脚本 | **DONE** — scripts/coverage.sh | 2026-04-07 |
| FINAL | 长稳 + 32T deadlock | PENDING | |
