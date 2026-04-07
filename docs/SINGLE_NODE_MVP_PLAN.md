# FerrisDB Phase 5 — Production 24/7 Reliability

> Created: 2026-04-07
> Baseline: commit 577f825 (1,031 tests, 0 failures)
> WAL TPS: 5,282 | No-WAL TPS: 7,108
> Previous: [Phase 1](MVP_PHASE1_COMPLETED.md) | [Phase 2](MVP_PHASE2_COMPLETED.md) | [Phase 3](MVP_PHASE3_COMPLETED.md) | [Phase 4](MVP_PHASE4_COMPLETED.md)

---

## 1. 已达成能力

| 能力 | 状态 | 数据 |
|------|------|------|
| ACID + MVCC | ✅ | CSN-based snapshot isolation |
| WAL + CRC32 + crash recovery | ✅ | Redo + undo 端到端验证 |
| WAL 无锁写入 | ✅ | RingBuffer + drain，overhead 26% |
| Checkpoint + WAL 截断 | ✅ | 60s 自动，drain wait |
| AutoVacuum + FSM 更新 | ✅ | 10s 后台，vacuum 写 WAL |
| Page checksum on read/write | ✅ | CRC32C 双向校验 |
| BufTable 128 分区锁 | ✅ | 减少 writer 阻塞 reader |
| 事务超时 + 强制 abort | ✅ | 30s 默认，后台扫描 |
| Engine lockfile | ✅ | flock 互斥 |
| Graceful shutdown | ✅ | 等待事务 + drain + checkpoint + fsync |
| EngineStats API | ✅ | 运维监控 |
| BTree free page 持久化 | ✅ | Engine save/restore |
| 7 项 WAL 可靠性修复 | ✅ | Commit 保序、abort 传播、vacuum WAL 等 |
| ASan clean | ✅ | 0 errors |
| 并发稳定性 | ✅ | BTree 30x、LockManager 30x、20W/32T 0 deadlock |

## 2. 深度分析：24/7 生产残留风险

### 2.1 CRITICAL — 会导致数据损坏或丢失

| # | 问题 | 场景 | 根因 |
|---|------|------|------|
| **R1** | **Disk full → page 丢失** | 磁盘满时 eviction flush 失败，page 从 buffer pool 清除但未落盘 | `flush_buffer` 失败后 page 已被替换，数据永久丢失 |
| **R2** | **DDL + DML 竞态** | `DROP TABLE` 和 `INSERT` 并发，OID 重用 | 无表级锁，catalog 立即删除 |

### 2.2 HIGH — 会导致系统不可用

| # | 问题 | 场景 | 根因 |
|---|------|------|------|
| **R3** | **WAL 文件无限积累** | 长事务阻止 checkpoint 推进 → WAL 不截断 → 磁盘满 | 无 WAL 最大保留策略 |
| **R4** | **长事务 undo 内存** | 100M 行批量导入，undo_log 在 1M 行处报错 | 无 undo spill-to-disk |

### 2.3 MEDIUM — 影响运维或性能

| # | 问题 | 场景 | 根因 |
|---|------|------|------|
| **R5** | **Slot 耗尽错误不友好** | 65 连接时 "NoFreeSlot" 无上下文 | 错误信息缺少 current/max |
| **R6** | **无监控告警集成** | WAL 积累、undo 增长、disk 占用无告警 | 仅 eprintln |

### 2.4 vs C++ dstore 架构差距

| 能力 | C++ dstore | FerrisDB | 差距影响 |
|------|-----------|----------|---------|
| BTree 并发 insert | Page-level latch crabbing + SMO | 全局 split_mutex | 16T+ insert 串行化 |
| WAL 多流 | NUMA-aware 多 WalStream | 单 RingBuffer | NUMA 服务器 scalability |
| Undo 系统 | 1M zone + varint + 异步 rollback worker | 内存 Vec + 同步 abort | 长事务受限 |
| 表级锁/MDL | Metadata Lock 保护 DDL/DML | 无 | DDL+DML 竞态 |
| CR Page | CSN-based 一致读页构造 | 无 | 长查询可能读到不一致 |

---

## 3. Fix Plan

### Phase 5A: P0 — 数据安全（阻塞商用）

| # | Item | Description | Est. |
|---|------|-------------|------|
| R1 | Eviction flush 失败保护 | flush 失败时不替换 buffer，选另一个 victim | 1h |
| R2 | 表级 metadata lock | DDL 前取排他 MDL，DML 前取共享 MDL | 2h |

### Phase 5B: P1 — 系统可用性

| # | Item | Description | Est. |
|---|------|-------------|------|
| R3 | WAL 最大保留 + 紧急清理 | 配置 max_wal_size，超限触发紧急 checkpoint | 1h |
| R4 | 大事务分批提示 | 改善错误信息，建议 "commit every N rows" | 0.5h |
| R5 | Slot 错误上下文 | 错误包含 current_slots/max_slots | 0.5h |

### Phase 5C: P2 — 运维与长稳

| # | Item | Description | Est. |
|---|------|-------------|------|
| R6 | Engine health check | EngineStats 加 wal_size_bytes、undo_max_size、disk_usage | 1h |
| T1 | 长稳压力测试 | TPCC WAL 5W/16T/300s 完成无 hang/crash/OOM | 验证 |
| T2 | Disk full 模拟测试 | 注入 IO error → 验证 graceful degradation | 验证 |

---

## 4. Regression Checklist

```bash
# Tests
cargo test --all

# ASan
RUSTFLAGS="-Z sanitizer=address -Cunsafe-allow-abi-mismatch=sanitizer" \
  cargo +nightly test -p ferrisdb-storage -p ferrisdb-transaction \
  --target x86_64-unknown-linux-gnu -- --test-threads=1

# TPCC
cargo run --release --bin tpcc -- --warehouses 5 --threads 16 --duration 20 --buffer-size 100000
cargo run --release --bin tpcc -- --warehouses 5 --threads 16 --duration 20 --buffer-size 100000 --wal

# WAL long-run (300s, verify no hang/crash)
cargo run --release --bin tpcc -- --warehouses 5 --threads 8 --duration 300 --buffer-size 100000 --wal

# Sanitizer suite
bash scripts/sanitizer_check.sh
```

---

## 5. Progress Tracking

| Phase | Item | Status | Date |
|-------|------|--------|------|
| R1 | Eviction flush failure protection | **DONE** — flush 失败不替换 victim，返回错误 | 2026-04-07 |
| R2 | Table-level metadata lock | **DONE** — DDL 排他锁，DML 共享锁 via get_table_lock() | 2026-04-07 |
| R3 | WAL max retention | **DONE** — wal_size_threshold 触发紧急 checkpoint | 2026-04-07 |
| R4 | Large txn error improvement | **DONE** — 包含当前/最大 action 数 + 建议分批 | 2026-04-07 |
| R5 | Slot error context | **DONE** — 包含 active/max slots + 建议 | 2026-04-07 |
| R6 | Engine health: wal_ring_unflushed | **DONE** — EngineStats 增加 ring buffer 监控 | 2026-04-07 |
| T1 | WAL 300s stability | **DONE** — 5W/8T/300s WAL 完成，无 hang/crash/panic | 2026-04-07 |
| T2 | ASan + TSan regression | **DONE** — ASan 829 pass 0 fail, TSan 无新 race | 2026-04-07 |
