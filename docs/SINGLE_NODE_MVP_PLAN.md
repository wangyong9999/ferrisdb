# FerrisDB — 单节点商用后续路线

> Updated: 2026-04-07
> Baseline: commit 418dbf6 | 914 tests 0 fail | WAL 6,337 TPS | ASan clean
> 全量对比报告: [PERFORMANCE_COMPARISON.md](PERFORMANCE_COMPARISON.md)
> BTree 设计文档: [BTREE_LOCK_COUPLING_DESIGN.md](BTREE_LOCK_COUPLING_DESIGN.md)
> 历史: [P1](MVP_PHASE1_COMPLETED.md)~[P6](MVP_PHASE6_COMPLETED.md)

---

## 已完成能力一览

✅ ACID + MVCC (CSN snapshot isolation)
✅ WAL 持久化 (CRC32 + redo + undo, **4.5% overhead**)
✅ WAL RingBuffer (无锁 DML, drain 线程)
✅ Crash Recovery (端到端验证)
✅ Checkpoint (60s 自动 + WAL 截断 + ring drain wait)
✅ Segment 分片 (128MB, **40TB+ 支持**)
✅ Page Checksum (CRC32C read + write)
✅ Buffer Pool (128 分区 RwLock)
✅ AutoVacuum + FSM (10s 后台, vacuum 写 WAL)
✅ DDL/DML 隔离 (Metadata Lock)
✅ 事务超时 (30s + 后台强制 abort)
✅ Eviction 安全 (flush 失败不丢页面)
✅ Engine Lockfile (flock 互斥)
✅ EngineStats API (运维监控)
✅ BTree free page 持久化
✅ ASan clean (829 tests)
✅ 并发稳定 (BTree 15/15, LockManager 30/30, 20W/32T 0 deadlock)
✅ WAL 300s 长稳 (无 hang/crash/panic)

## 后续专项（按优先级）

| # | 项目 | 估期 | 阻塞场景 |
|---|------|------|---------|
| 1 | **BTree incomplete-split 协议** | 1-2 周 | BTree 高并发写入 (16T+ insert) |
| 2 | **TPCC BTree 索引** | 3 天 | 长稳验证（消除 HashMap 衰减） |
| 3 | **Undo spill-to-disk** | 3 天 | 超大事务 (>1M 行) |
| 4 | **WAL 归档** | 1 天 | PITR 基础 |
| 5 | **在线热备** | 1 周 | 灾备 |
| 6 | **覆盖率 >70%** | 2 天 | 质量度量 |
