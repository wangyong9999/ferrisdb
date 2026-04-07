# FerrisDB Phase 6 — 40TB Enterprise Single-Node Readiness

> Created: 2026-04-07
> Baseline: commit b9def90
> Previous: [P1](MVP_PHASE1_COMPLETED.md) | [P2](MVP_PHASE2_COMPLETED.md) | [P3](MVP_PHASE3_COMPLETED.md) | [P4](MVP_PHASE4_COMPLETED.md) | [P5](MVP_PHASE5_COMPLETED.md)

---

## 1. 诚实现状：能做什么，不能做什么

### 1.1 已达标（可商用）

| 能力 | 数据 | 评估 |
|------|------|------|
| ACID 事务 + MVCC | CSN snapshot isolation | ✅ 正确 |
| WAL 持久性 | CRC32 + crash recovery (redo+undo) | ✅ 端到端验证 |
| WAL 性能 | 5,282 TPS (26% overhead) | ✅ 可用 |
| Checkpoint | 60s 自动 + WAL 截断 + ring drain wait | ✅ |
| Page checksum | CRC32C on read+write | ✅ |
| 并发安全 | 20W/32T 0 deadlock, ASan clean | ✅ |
| Eviction 安全 | flush 失败不丢页面 | ✅ |
| DDL/DML 隔离 | Metadata lock | ✅ |
| WAL 300s 长稳 | 无 hang/crash/panic | ✅ |

### 1.2 不达标（阻塞 40TB 商用）

| 问题 | 现状 | 40TB 需要 | 差距 |
|------|------|----------|------|
| **page_no u32 溢出** | max 4.3B pages = 32TB | 5B+ pages | **需要 u64 或 segment 分片** |
| **单文件无分段** | 1 relation = 1 file | ext4 max 16TB per file | **需要 1GB segment 分片** |
| **BTree insert 串行** | split_mutex 全局锁 | 32T+ 高并发 | **需要 latch crabbing** |
| **WAL 长稳衰减** | 300s: 84% TPS 下降 | 24/7 稳定 | **TPCC HashMap 问题，真实 BTree 待验证** |
| **Undo 内存限制** | 1M actions (~100MB) | 大批量事务 | **需要 spill-to-disk** |
| **100W TPS 低** | 649 TPS (100W/16T) | >3,000 TPS | **TPCC HashMap 瓶颈，非引擎** |
| **无 PITR** | WAL 截断后不可恢复 | 任意时间点恢复 | **需要 WAL 归档** |
| **无备份恢复** | 无 | 在线热备 | **需要 pg_basebackup 类工具** |

### 1.3 vs C++ dstore 硬差距

| 能力 | C++ dstore | FerrisDB | 商用影响 |
|------|-----------|----------|---------|
| **数据文件分段** | 1GB segment files | 单文件无限增长 | ext4 16TB 限制 |
| **BTree 并发** | Page-level latch crabbing | split_mutex 串行 | 16T+ scalability |
| **Undo 系统** | 1M zone + varint + spill + async worker | 内存 Vec 1M 限制 | 大事务 |
| **WAL 多流** | NUMA-aware 多 WalStream | 单 RingBuffer | 多 socket |
| **CR Page** | CSN 一致读页构造 | 无 | 长事务读一致性 |
| **系统表** | 完整 bootstrap + WAL | HashMap catalog | 元数据恢复 |

---

## 2. 40TB 商用路线图

### Phase 6A: 存储容量（不做就物理不支持 40TB）

| # | Item | Description | Est. | Blocks |
|---|------|-------------|------|--------|
| **S1** | 数据文件 1GB 分段 | relation 由多个 1GB segment 组成，smgr 按 (oid, seg_no, page_no) 寻址 | 3d | 40TB 基础 |
| **S2** | page_no 扩展 | HeapTable/BTree 内部用 (seg_no: u32, page_no: u32) 或直接 u64 | 2d | 40TB 基础 |

### Phase 6B: 并发扩展（不做就 32T+ 不可用）

| # | Item | Description | Est. | Blocks |
|---|------|-------------|------|--------|
| **C1** | BTree latch crabbing | leaf split 用 page lock + right-link；insert 正常路径无全局锁 | 5d | 高并发 |
| **C2** | WAL 多 RingBuffer | 2-4 个 RingBuffer 按线程 hash 分发，减少 drain 瓶颈 | 2d | 多核 |

### Phase 6C: 大事务（不做就批量导入受限）

| # | Item | Description | Est. | Blocks |
|---|------|-------------|------|--------|
| **U1** | Undo spill-to-disk | undo_log 超阈值写临时文件，abort 时从文件读取 | 3d | 大批量 |

### Phase 6D: 数据保护（不做就无法灾备）

| # | Item | Description | Est. | Blocks |
|---|------|-------------|------|--------|
| **D1** | WAL 归档 | checkpoint 后将旧 WAL 文件移到 archive 目录而非删除 | 1d | PITR |
| **D2** | 在线热备 | snapshot + WAL stream 导出，支持 pg_basebackup 类操作 | 5d | 灾备 |

### Phase 6E: 稳定性加固

| # | Item | Description | Est. | Blocks |
|---|------|-------------|------|--------|
| **T1** | 真实 BTree 索引 TPCC | 替换 HashMap 为 BTree 索引，验证长稳 | 3d | 长稳验证 |
| **T2** | 1TB 数据量压测 | 装载 1TB 数据，跑 24h TPCC，监控 TPS/内存/磁盘 | 2d | 容量验证 |
| **T3** | 故障注入测试 | 磁盘满、网络断、进程 kill、电源断 | 2d | 可靠性 |

---

## 3. 优先级排序（到首商用的最短路径）

### Tier 1: 必须做（无此不可商用）
- **S1 + S2**: 文件分段 + page 寻址扩展（5d）→ 支持 >16TB
- **C1**: BTree latch crabbing（5d）→ 32T+ 可用

### Tier 2: 应该做（客户体验关键）
- **U1**: Undo spill-to-disk（3d）→ 大批量导入
- **D1**: WAL 归档（1d）→ PITR 基础
- **T1**: BTree TPCC 验证（3d）→ 长稳数据

### Tier 3: 增强（提升竞争力）
- **C2**: 多 RingBuffer
- **D2**: 在线热备
- **T2 + T3**: 大容量 + 故障注入

---

## 4. 进度跟踪

| Phase | Item | Status | Date |
|-------|------|--------|------|
| S1 | 数据文件 1GB 分段 | PENDING | |
| S2 | page_no 扩展 | PENDING | |
| C1 | BTree latch crabbing | PENDING | |
| C2 | WAL 多 RingBuffer | PENDING | |
| U1 | Undo spill-to-disk | PENDING | |
| D1 | WAL 归档 | PENDING | |
| D2 | 在线热备 | PENDING | |
| T1 | BTree TPCC 验证 | PENDING | |
| T2 | 1TB 压测 | PENDING | |
| T3 | 故障注入 | PENDING | |
