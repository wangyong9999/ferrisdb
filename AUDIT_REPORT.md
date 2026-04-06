# FerrisDB (dstore-rust) 系统性深度审计报告

> 审计日期: 2026-04-02
> 代码规模: ~28,100 行 Rust (80+ 源文件), ~5,200 行集成测试 (29 文件)
> 对比参照: dstore C++ 版本 (~200,000 行, 370+ 文件)

---

## 目录

- [一、致命缺陷 (CRITICAL)](#一致命缺陷-critical)
- [二、严重缺陷 (HIGH)](#二严重缺陷-high)
- [三、中等缺陷 (MEDIUM)](#三中等缺陷-medium)
- [四、并发与原子性BUG](#四并发与原子性bug)
- [五、对比C++版本的核心功能缺失](#五对比c版本的核心功能缺失)
- [六、测试覆盖度分析](#六测试覆盖度分析)
- [七、架构性建议](#七架构性建议)
- [八、优先级排序修复路线图](#八优先级排序修复路线图)

---

## 一、致命缺陷 (CRITICAL)

### C1. WAL记录无CRC校验 — 静默数据腐败不可检测
**文件:** `wal/record.rs`, `wal/writer.rs`
**描述:** `WalRecordHeader` 仅有4字节 (record_type + flags), 没有任何CRC/校验和字段。WAL记录写入磁盘后, 如果发生静默数据腐烂或部分写入, 恢复代码无法检测, 会将垃圾数据当作有效WAL记录重放, 导致数据库状态损坏。
**C++对比:** C++ dstore的WAL记录头包含CRC字段, 恢复时逐条校验。

### C2. Checkpoint期间WAL段文件被过早删除
**文件:** `wal/checkpoint.rs:380-402` (`cleanup_old_wal_segments`)
**描述:** 清理逻辑删除 `file_no < checkpoint_file_no - 1` 的WAL文件。但在checkpoint写入脏页之前就可能已经删除了恢复所需的WAL段。如果在此期间崩溃, 恢复所需的WAL文件已被删除, 导致**数据永久丢失**。
**C++对比:** C++ dstore使用 `KeepWALFilesKeeping` 基于REDO指针保留文件, 确保恢复总能访问到所需的WAL。

### C3. Checkpoint操作顺序错误 — 脏页先刷、WAL后写
**文件:** `wal/checkpoint.rs:274-293`
**描述:** 当前顺序: (1) 刷脏页 → (2) 写checkpoint WAL记录。正确顺序应为: (1) 写checkpoint WAL记录 → (2) fsync WAL → (3) 刷脏页 → (4) 更新控制文件。当前顺序下, 如果在刷脏页完成后、写checkpoint记录前崩溃, 已刷的脏页对应的WAL可能已被清理, 无法恢复。
**C++对比:** C++ dstore严格遵循 WAL-first 原则。

### C4. 事务commit时WAL记录写入顺序错误 — 已提交事务可丢失
**文件:** `transaction/mod.rs:303-332`
**描述:** `commit()` 先设置 `slot_csns` 为已提交CSN, 再写WAL commit记录, 最后释放slot。如果崩溃发生在设置CSN之后、写WAL之前, 事务在内存中显示已提交但WAL中没有记录, 重启后该事务被当作未提交回滚 — **已确认的提交被丢失**。
正确顺序: (1) 写WAL commit记录 → (2) fsync → (3) 设置CSN → (4) 释放slot。
**C++对比:** C++ dstore先写WAL再更新slot状态。

### C5. Transaction持有TransactionManager裸指针 — Use-After-Free
**文件:** `transaction/mod.rs:227-237` (`TransactionManagerRef`)
**描述:** `Transaction` 通过 `Arc<TransactionManagerRef>` 持有指向 `TransactionManager` 的裸指针 `*const TransactionManager`。如果 `TransactionManager` 被移动或销毁, 而 `Transaction` 仍持有该指针, 在 `Drop` 时调用 `release_slot` 将产生 use-after-free。`unsafe impl Send/Sync` 标记使其更加脆弱。
**C++对比:** C++ dstore使用 `shared_ptr` 管理生命周期。

### C6. BufTable lookup无锁读取tag — 数据竞争 (Undefined Behavior)
**文件:** `buffer/table.rs:98-116`
**描述:** `lookup()` 不获取 `self.lock`, 直接通过 `unsafe { *entry.tag.get() }` 读取tag (12字节)。而 `insert()` 和 `remove()` 在持锁状态下修改tag。由于 `BufferTag` 为12字节, 超过8字节, 在大多数架构上不是原子写入, 导致**撕裂读(torn read)** — 这是Rust内存模型下的未定义行为。
**C++对比:** C++ dstore的 `BufTable` 使用 `SharedInvalLocker` 或 `LWLock` 保护查找。

### C7. Lock.try_acquire TOCTOU — 冲突锁可同时授予
**文件:** `lockmgr/lock.rs:147-158`
**描述:** `try_acquire` 先 load `hold_mode`, 检查冲突, 再 store 新mode。两个线程可同时读取 `hold_mode=None`, 分别通过冲突检查(如Share和Exclusive), 然后都成功store — 结果两个互斥锁同时被授予, 破坏隔离性。
**C++对比:** C++ dstore使用 CAS 保护整个 acquire 过程。

### C8. BufferDesc.unlock_header 非原子store覆盖并发更新
**文件:** `buffer/desc.rs:373-377`
**描述:** `unlock_header()` 执行 load → compute → store。在load和store之间, 其他线程可通过 `pin()` 的 `fetch_add` 修改状态字的低32位。store 会**覆盖**这些修改, 丢失 pin count 或 dirty flag。
**C++对比:** C++ dstore使用 CAS loop 而非 load+store。

### C9. Recovery不处理DDL WAL记录 — 崩溃后建表/删表丢失
**文件:** `wal/recovery.rs`, `engine.rs`
**描述:** `Engine::write_ddl_wal` 写入 `CreateTable`/`DropTable`/`CreateIndex` 等DDL WAL记录, 但 `recovery.rs` 的 `redo_record` 中这些类型落入 `_ => Ok(true)` 兜底分支, 不做任何处理。崩溃恢复后, DDL操作创建的数据文件不会被清理(如果DDL回滚), 已删除的表数据文件不会被恢复。
**C++对比:** C++ dstore有完整的DDL WAL redo/undo逻辑。

### C10. Tablespace元数据不持久化 — 重启后空间分配丢失
**文件:** `tablespace.rs:49-55`
**描述:** `relation_extents` (关系到区段的映射) 仅在内存中, 不持久化到磁盘。重启后所有区段分配信息丢失, 导致页号重用和覆盖已有数据。`DataSegment` 的 `free_bitmap` 同样是纯内存数据。
**C++对比:** C++ dstore在特殊catalog页中持久化表空间元数据。

---

## 二、严重缺陷 (HIGH)

### H1. LWLock acquire_shared 快速路径状态损坏
**文件:** `lock/lwlock.rs:170-184`
**描述:** `acquire_shared()` 无条件执行 `fetch_add(1)` 增加共享计数, 即使独占锁已被持有。回滚路径通过 `fetch_sub(1)` 尝试撤销, 但在这之间其他线程可能观察到 EXCLUSIVE + 非零共享计数的损坏状态。同样的问题存在于 `ContentLock` (`content_lock.rs:55-64`)。

### H2. WAL flush_buffer 忽略WAL写入错误
**文件:** `buffer/pool.rs:536-537`
**描述:** `flush_buffer` 中 `let _ = wal_writer.write(&fpw_buf)` 静默丢弃WAL写入错误。如果WAL写入失败, 脏页仍会被刷到磁盘, 违反WAL-before-data不变量 — 刷出的脏页对应的WAL记录不存在, 崩溃后无法恢复。

### H3. Undo WAL记录类型全部硬编码为 UndoInsertRecord
**文件:** `transaction/mod.rs:368-374`
**描述:** `UndoAction::serialize_for_wal` 对所有undo动作类型(Insert/Update/Delete等)统一使用 `WalRecordType::UndoInsertRecord`。恢复时无法区分undo动作类型, 无法正确回滚。
**C++对比:** C++ dstore的每种undo操作有独立的WAL记录类型。

### H4. MVCC快照不跟踪活跃事务 — 快照隔离被打破
**文件:** `types/snapshot.rs:129-133`
**描述:** `SnapshotManager::get_snapshot()` 用空 `vec![]` 作为 `active_xids`。`is_visible()` 中的 `is_active()` 永远返回false, 导致活跃事务的修改可能对其他事务可见, 破坏REPEATABLE READ隔离级别。

### H5. SSI仅检测2-transaction环 — 无法检测经典危险结构
**文件:** `ssi.rs:52-81`
**描述:** `check_conflict` 仅检测T1读写T2、T2读写T1的直接环路。真正SSI需要检测3事务危险结构 (T1读A, T2写A读B, T3写B), 这是SSI设计要预防的典型write-snapshot skew异常。
**C++对比:** C++ dstore实现完整的SSI依赖图追踪。

### H6. WAL Flusher关闭逻辑反转
**文件:** `wal/writer.rs:438-461`
**描述:** `WalFlusher` 线程的park回调中shutdown检查逻辑反转 — shutdown为true时才执行sync, 为false时直接返回unpark token。导致flusher在正常工作时过早退出或在应该停止时继续运行。

### H7. Engine.shutdown竞态 — 不阻止新事务
**文件:** `engine.rs:151-161`
**描述:** `shutdown()` 调用 `flush_all()` 和 `wal.sync()` 但没有机制阻止正在进行的操作或防止新事务开始。在 `flush_all()` 和 `sync()` 之间, 新事务可能启动并产生额外变更, 这些变更不会被同步。
**C++对比:** C++ dstore设置状态标志阻止新事务, 等待进行中事务完成。

### H8. Recovery错误被静默忽略
**文件:** `engine.rs:131-137`
**描述:** `let _ = recovery.recover(...)` 丢弃恢复错误。如果恢复失败, 数据库以不一致状态启动, 后续操作可能导致更严重的数据损坏。
**C++对比:** C++ dstore中恢复失败是致命错误, 阻止数据库启动。

### H9. WalBuffer LSN分配但写入失败时产生LSN空洞
**文件:** `wal/buffer.rs:98-127`
**描述:** `write()` 先通过 `fetch_add` 预留空间和分配LSN, 如果后续检测到溢出并回滚 `write_pos`, LSN已经递增但无数据写入。产生WAL中的永久空洞, 恢复时会跳过不存在的记录。

### H10. WalBuffer end_atomic_group非原子读取并发写入的字段
**文件:** `wal/buffer.rs:187-213`
**描述:** `end_atomic_group` 非原子读取 `group_len` (原始u32), 而 `append_to_atomic_group` 通过 `AtomicU32::fetch_add` 并发更新。这是数据竞争 — 可能读到部分更新的值。

### H11. Recovery通过StorageManager直接操作绕过BufferPool
**文件:** `engine.rs:84-148`, `wal/recovery.rs`
**描述:** 恢复直接通过 `StorageManager` 读写页面, 不经过BufferPool。如果BufferPool已缓存了这些页面, 恢复写入磁盘后缓存与磁盘不一致。后续读取缓存将返回过时数据。
**C++对比:** C++ dstore恢复通过BufferPool, 保证缓存一致性。

### H12. 跨页update缺少ctid转发链的WAL记录
**文件:** `heap/mod.rs:287-326`
**描述:** 跨页update后, 旧页的ctid应指向新位置, 但旧页的ctid修改没有WAL记录。崩溃恢复后ctid链断裂, 通过旧TID无法找到新版本。
**C++对比:** C++ dstore有专门的 `HeapUpdateOldPage`/`HeapUpdateNewPage` WAL记录对。

### H13. 并发BTree插入split_mutex保护范围过窄
**文件:** `index/btree.rs:565`
**描述:** `split_mutex` 仅保护split操作本身, 不保护 `find_parent` 和 `insert_separator_into_parent` 的组合。多线程并发插入同一子树时, 一个线程的split可能导致另一线程的parent引用失效, 产生树结构损坏。

### H14. Release双重abort — slot可能被释放两次
**文件:** `transaction/mod.rs:561-568`
**描述:** `Drop` 实现在 `self.active` 为true时调用 `abort()`。如果 `commit()` 在设置 `active=false` 之前panic, `Drop` 触发 `abort()` 对已提交事务执行回滚。另外如果 `abort()` 内部panic, 可能导致 `release_slot` 被调用两次。

---

## 三、中等缺陷 (MEDIUM)

### M1. Lock.release 无条件设置hold_mode为None
**文件:** `lockmgr/lock.rs:162`
**描述:** 如果多个持有者存在(设计上似乎支持), 一个持有者释放会清除所有人的模式。锁认为空闲但其他持有者仍活跃。

### M2. LockManager使用全局Condvar — 惊群效应
**文件:** `lockmgr/manager.rs`
**描述:** 所有等待锁的事务共享同一个Condvar, 一次notify会唤醒所有等待者, 但只有一个能获取锁, 其余重新睡眠。高并发下性能严重退化。

### M3. DeadlockDetector.remove_wait移除所有等待关系
**文件:** `lockmgr/deadlock.rs:31-34`
**描述:** 事务获取一个锁后调用 `remove_wait`, 移除该事务的所有等待关系。如果事务同时等待多个锁(多个HashSet entry), 获取一个后其余等待关系也被删除, 导致漏检死锁。

### M4. SpinLock.acquire效率低下
**文件:** `lock/spinlock.rs:33-39`, `atomic/helpers.rs:33-51`
**描述:** `spin_try_lock` 自旋1000次后返回false, `acquire` 循环调用它, 形成1000次自旋+yield的重复模式。应使用单一CAS循环。

### M5. Buffer eviction期间background writer与eviction竞争
**文件:** `buffer/pool.rs:208-252`
**描述:** `pin_slow` 释放 `alloc_lock` 后执行I/O。此时background writer的 `flush_some` 可能在扫描同一缓冲区, 两者可能同时操作同一脏页。

### M6. Catalog变更不自动持久化
**文件:** `engine.rs`, `catalog.rs`
**描述:** DDL操作(create_table, create_index, drop_table)修改catalog后不自动调用 `persist()`。仅在显式调用 `save_table_state`/`save_index_state` 时才持久化。崩溃即丢失。

### M7. Checkpoint状态转换瞬态
**文件:** `wal/checkpoint.rs:242-246`
**描述:** checkpoint完成后状态从 `Completed` 立即重置为 `Idle`, 外部调用者可能永远看不到 `Completed` 状态, 导致重复触发checkpoint。

### M8. ParallelRedo worker空队列时busy-spin
**文件:** `wal/parallel_redo.rs:151-170`
**描述:** 工作线程在队列为空且未关闭时执行 `spin_loop()`, 消耗100% CPU。应使用条件变量或通知机制。

### M9. ParallelRedo total_processed永远为零
**文件:** `wal/parallel_redo.rs:184`
**描述:** `total_processed` 声明但从未递增, `stats()` 始终返回0, 使性能监控无效。

### M10. Tablespace allocate_extent使用读锁持有过久
**文件:** `tablespace.rs:107-112`
**描述:** `allocate_extent` 获取 `tablespaces` HashMap 的读锁后调用 `allocate_extent`, 整个分配过程持锁, 阻塞其他表空间的创建/删除。

### M11. DataSegment.free_page锁顺序可能死锁
**文件:** `tablespace.rs:176-188`
**描述:** `free_page` 获取 `extent_chain` 的读锁和 `free_bitmap` 的写锁。如果另一线程以相反顺序获取锁, 将产生死锁。

### M12. CheckpointRecovery.find_latest_checkpoint未正确反序列化
**文件:** `wal/checkpoint.rs:427-445`
**描述:** 用硬编码的 `LSN=0` 和空 `Vec<Xid>` 创建checkpoint, 而非从WAL字节反序列化。活跃事务列表丢失。

### M13. LWLock独占锁饥饿
**文件:** `lock/lwlock.rs:328-369`
**描述:** 连续的共享锁获取流可使独占锁等待者永久饥饿, 因为共享快速路径总是成功。

### M14. Undo序列化字节序依赖平台
**文件:** `undo/record.rs:206-211`
**描述:** 将 `#[repr(C)]` 结构体直接cast为字节, 产生的字节流依赖平台字节序。跨平台不兼容。

---

## 四、并发与原子性BUG

### A1. WalBuffer AtomicU32类型转换可能未对齐
**文件:** `wal/buffer.rs:173-179`
**描述:** 将原始字节指针cast为 `*const AtomicU32`, 但 `Vec<u8>` 仅保证1字节对齐。如果偏移量不是4的倍数, 为未定义行为。

### A2. WalBuffer.reset()与并发write()竞争
**文件:** `wal/buffer.rs:245-248`
**描述:** `reset()` 存储0到write_pos和flush_pos。并发 `write()` 已通过 `fetch_add` 预留空间但未复制数据时, store被覆盖, 产生不一致状态。

### A3. WalBuffer flush返回不一致切片
**文件:** `wal/buffer.rs:230-242`
**描述:** `flush()` 分别加载flush_pos和write_pos, 之间并发write可能推进write_pos。返回的切片可能包含部分写入的记录数据。

### A4. LWLock等待队列未实际使用
**文件:** `lock/lwlock.rs:439-456`
**描述:** `wait_shared()`/`wait_exclusive()` 获取wait_list锁后立即释放, 不将waiter加入队列。所有waiter在 `wait_on_lock` 中纯自旋, HAS_WAITERS位无实际作用。

### A5. LruQueue.add_to_head使用swap而非store
**文件:** `buffer/lru.rs:107`
**描述:** 在已持锁的临界区使用 `swap`, 暗示无锁设计意图, 但锁已保证互斥。store即可, swap增加不必要的原子操作开销。

### A6. ParallelRedo.get_or_assign_worker TOCTOU
**文件:** `wal/parallel_redo.rs:279-298`
**描述:** 先读锁查找mapping, 释放后获取写锁插入。两次操作之间另一线程可能已插入相同tag。

---

## 五、对比C++版本的核心功能缺失

### 缺失模块 (C++有, Rust无)

| 功能 | C++实现 | Rust状态 | 严重程度 |
|------|---------|----------|---------|
| **Logical Replication (逻辑复制)** | 完整decode pipeline, replication slots, 并行decode worker, decode dictionary | 完全缺失 | HIGH |
| **Flashback (闪回)** | CSN-based flashback table, delta/lost tuple计算 | 完全缺失 | MEDIUM |
| **Pluggable Database (PDB)** | 多租户, switchover/failover, PRIMARY/STANDBY角色 | 仅PdbId类型定义 | HIGH |
| **VFS抽象** | 可插拔VFS, 租户隔离, PageStore | 直接文件I/O | MEDIUM |
| **Autonomous Transaction** | CreateAutonomousTransaction/Destroy | 完全缺失 | MEDIUM |
| **CR Page (一致读页)** | CRContext, GetCrPage, ConstructCrPage | 完全缺失 | HIGH |
| **Parallel BTree Build** | dstore_btree_build_parallel, tuplesort | 完全缺失 | MEDIUM |
| **Concurrent Index (CCINDEX)** | delta index, incomplete index for CCINDEX | 完全缺失 | MEDIUM |
| **Sample Scan** | HeapInterface::SampleScan | 完全缺失 | LOW |
| **Batch Insert** | HeapInterface::BatchInsert, HeapInsertHandler | 完全缺失 | MEDIUM |
| **LOB集成** | LobStore存在但与HeapTable无集成 | 部分实现 | MEDIUM |
| **System Table** | 完整系统表管理, bootstrap data, system table WAL | 仅catalog HashMap | HIGH |
| **Control File体系** | CSN info, WAL info, relmap, tablespace, PDB info, logicrep控制文件 | 仅32字节ControlFile | HIGH |
| **配置系统** | 200+ GUC参数 | ~10个配置项 | MEDIUM |
| **Fault Injection框架** | Heap, index, lock, transaction, undo, WAL fault injection | 仅测试级注入 | LOW |
| **性能计数器** | 多级perf counter, skip list存储 | 仅EngineStats计数器 | LOW |
| **NUMA感知** | CPU auto-binding, NUMA topology | 完全缺失 | LOW |
| **Parallel Execution框架** | ParallelWorkController | 仅parallel_scan骨架 | MEDIUM |
| **DDL WAL恢复** | 完整DDL redo/undo | 仅写DDL WAL但不恢复 | HIGH |
| **异步Undo Rollback** | 后台rollback worker | 仅同步abort | MEDIUM |
| **Tuple Sort (外部排序)** | Logical tape, tape-based external sort | 完全缺失 | MEDIUM |
| **Savepoint WAL记录** | WAL-level savepoint tracking | 无WAL记录 | LOW |
| **2PC Prepare** | TransactionState::Prepared存在但未使用 | 枚举占位 | LOW |

### 功能差异详情

#### 1. MVCC可见性 — 不完整
C++ dstore实现了3种快照类型 (MVCC, NOW, DIRTY), 完整的CSN-based可见性判断, 以及CR page构造。Rust版本的 `SnapshotData` 不跟踪活跃事务, `is_visible()` 逻辑不完整。

#### 2. Undo系统 — 量级差距
C++ dstore支持1M undo zone, 带varint压缩的序列化, 异步rollback worker, undo info cache, transaction slot管理。Rust版本是简单的per-transaction Vec。

#### 3. WAL系统 — 缺少关键特性
C++ dstore有multi-stream WAL, WAL throttling, SPSC queue, 可配置WAL级别 (minimal到logical), WAL dump工具。Rust版本是单流、无校验和、无节流。

#### 4. Buffer Pool — 缺少关键机制
C++ dstore有partitioned buffer table, hot/cold LRU splitting, bulk read/write rings, 动态resize, 临时buffer manager。Rust版本是简单的全局HashMap + 单一LRU。

#### 5. 锁管理器 — 差距显著
C++ dstore有分区锁表, 6种专用锁管理器 (table/tablespace/object/advisory/package/procedure), lazy lock优化, 完整的wait-for graph死锁检测。Rust版本是单一全局HashMap + 简化BFS。

---

## 六、测试覆盖度分析

### 测试统计
- 集成测试: ~29个文件, ~5,200行
- 内联单元测试: 56个文件含 `#[cfg(test)]` 模块
- 总测试数: ~1,004个 `#[test]`
- `#[ignore]` 测试: 0个

### 关键测试缺口

| 优先级 | 模块 | 缺失的测试场景 |
|--------|------|---------------|
| **HIGH** | BTree | 崩溃恢复中split正确性验证 |
| **HIGH** | Recovery | 并行redo实际执行测试 |
| **HIGH** | Engine | DDL期间崩溃 + 恢复验证 |
| **HIGH** | WAL | 真实torn write (扇区边界截断) |
| **HIGH** | WAL | WAL记录损坏payload检测 |
| **HIGH** | Engine | 不调用shutdown直接drop后的恢复 |
| **MEDIUM** | Lock | 3+事务死锁端到端测试 |
| **MEDIUM** | Lock | 锁升级/降级 |
| **MEDIUM** | SSI | 3事务危险结构检测 |
| **MEDIUM** | Buffer | 并发eviction压力测试 |
| **MEDIUM** | Buffer | 读取路径checksum失败检测 |
| **MEDIUM** | Heap | Vacuum + 并发scan交互 |
| **MEDIUM** | Heap | LOB与HeapTable集成 |
| **LOW** | BTree | Merge正确性验证 |
| **LOW** | WAL | 恢复中缺失WAL段 |
| **LOW** | SSI | 并发cleanup竞争 |
| **LOW** | Transaction | 深层嵌套savepoint (5+级) |

### 测试质量问题

1. **BTree并发测试容忍数据丢失**: `test_btree_8t_concurrent` 插入4000个key但只assert `found >= 3000`, 接受25%的数据丢失。这暗示已知的并发bug被掩盖而非修复。

2. **WAL恢复测试仅验证统计**: 大多数恢复测试只检查 `records_redone + records_skipped > 0`, 不验证实际页面数据正确性。

3. **Buffer eviction测试仅验证不崩溃**: 不验证evicted页面被正确从磁盘重新读取。

4. **大量重复测试**: BTree insert/lookup在6个文件中重复测试, WAL write/recovery在6个文件中重复, 导致5-6倍冗余覆盖而关键场景未测试。

5. **所有锁测试成功无死锁**: 并发测试操作不同锁或快速释放, 未测试真正触发死锁检测路径的场景。

---

## 七、架构性建议

### 7.1 WAL系统需要重构
- 添加记录级CRC校验
- 修正checkpoint操作顺序
- 修复flusher关闭逻辑
- 添加WAL throttling
- 考虑multi-stream架构

### 7.2 并发控制需要重写
- LWLock/ContentLock快速路径需CAS-based而非fetch_add
- Lock.acquire需要CAS保护整个acquire过程
- 添加proper wait queue (非空壳)
- 考虑分区锁表减少争用

### 7.3 MVCC需要补完
- `get_snapshot` 必须捕获活跃事务列表
- `is_visible` 需要完整的CSN-based可见性规则
- 需要实现CR page构造

### 7.4 Recovery需要加强
- 添加DDL WAL记录的redo处理
- 恢复应通过BufferPool而非直接I/O
- 并行redo需要实际测试
- 添加CRC验证

### 7.5 BufferPool需要加固
- `unlock_header` 使用CAS替代load+store
- BufTable lookup需要加锁或使用lock-free哈希表
- Eviction逻辑需要更严格的原子性保证

### 7.6 持久化层需要补充
- Tablespace元数据必须持久化
- Catalog变更应自动持久化
- 需要完整的ControlFile体系

---

## 八、优先级排序修复路线图

### Phase 1: 数据安全 (阻止数据丢失/损坏) — 最高优先级
1. **[C4]** 修正事务commit的WAL写入顺序
2. **[C3]** 修正checkpoint操作顺序
3. **[C2]** 修复WAL段文件过早删除
4. **[C1]** 添加WAL记录CRC校验
5. **[C9]** 实现DDL WAL记录的recovery处理
6. **[H2]** flush_buffer不忽略WAL错误
7. **[H8]** Recovery错误不可忽略

### Phase 2: 并发正确性 (消除数据竞争和UB)
1. **[C6]** BufTable lookup加锁或改为lock-free
2. **[C7]** Lock.acquire使用CAS保护
3. **[C8]** BufferDesc.unlock_header使用CAS
4. **[C5]** TransactionManager引用生命周期修正
5. **[H1]** LWLock/ContentLock快速路径修正
6. **[C10]** Tablespace元数据持久化
7. **[H14]** 修复双重abort问题

### Phase 3: MVCC与事务正确性
1. **[H4]** 实现活跃事务跟踪
2. **[H3]** Undo WAL记录类型区分
3. **[H5]** SSI完整dangerous structure检测
4. **[H12]** 跨页update的ctid WAL记录
5. **[H13]** BTree split并发保护加强

### Phase 4: 健壮性
1. **[H6]** 修复WAL flusher关闭逻辑
2. **[H7]** Engine shutdown事务隔离
3. **[H9]** WalBuffer LSN空洞修复
4. **[H10][A1]** WalBuffer原子操作对齐修复
5. **[H11]** Recovery通过BufferPool执行

### Phase 5: 功能补齐 (按重要性)
1. CR Page一致读
2. 系统表管理
3. Control File体系
4. DDL WAL恢复
5. 逻辑复制
6. Batch Insert
7. 并行BTree Build
8. 外部排序(Tuple Sort)
9. Flashback
10. PDB多租户

### Phase 6: 测试补齐
1. 崩溃恢复端到端测试 (不调用shutdown)
2. BTree split恢复正确性
3. 并行redo执行测试
4. 并发eviction压力测试
5. WAL torn write测试
6. 3+事务死锁测试
7. MVCC可见性边界测试

---

## 附录: 代码质量观察

1. **无TODO/FIXME/HACK注释**: 整个代码库零标记, 可能意味着系统性技术债务未被记录。
2. **unsafe使用**: 大量 `UnsafeCell`, 裸指针cast, `unsafe impl Send/Sync` — 需要系统性安全审计。
3. **错误处理**: 统一使用 `FerrisDBError`, 但多处用 `let _ =` 丢弃错误, 掩盖故障。
4. **模块依赖**: 严格的分层依赖 (core → storage/transaction → facade), 这是好的设计。
5. **内存对齐**: LWLock和BufferDesc使用128字节对齐, 符合缓存行优化最佳实践。
