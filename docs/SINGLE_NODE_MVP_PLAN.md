# FerrisDB Single-Node MVP Phase 3 — Enterprise Readiness

> Created: 2026-04-07
> Baseline: commit f94b9bd (1,027 tests, 0 failures, ASan clean)
> Previous phases: [Phase 1](MVP_PHASE1_COMPLETED.md) | [Phase 2](MVP_PHASE2_COMPLETED.md)

---

## 1. Benchmark Data

### 1.1 Warehouse Scalability (16 threads, 15s)

| Warehouses | TPS | Notes |
|-----------|-----|-------|
| 1 | 3,601 | High contention on single warehouse |
| 5 | **5,332** | Peak throughput |
| 10 | 4,731 | Slight decline |
| 20 | 3,480 | Working set exceeds L3 cache |
| 50 | 1,495 | Significant degradation |

**Issue**: 50W drops to 1,495 TPS (72% loss from peak). Root cause: buffer pool 300K pages covers ~2.4GB, 50W data set exceeds this.

### 1.2 Thread Scalability (20W, 15s)

| Threads | TPS | Speedup vs 1T |
|---------|-----|---------------|
| 1 | 1,079 | 1.0x |
| 2 | 1,715 | 1.6x |
| 4 | 2,479 | 2.3x |
| 8 | 2,768 | 2.6x |
| 16 | 3,170 | 2.9x |
| 32 | 3,113 | 2.9x (saturated) |

**Issue**: Scalability saturates at 8T (only 2.9x at 16T). Root cause: BTree split_mutex serializes all index inserts; page-level ContentLock contention on district pages.

### 1.3 WAL Mode Performance

| Mode | TPS | Overhead |
|------|-----|----------|
| No WAL | 4,959 | — |
| **With WAL** | **602** | **-88%** |

**CRITICAL**: WAL mode is unusable for production. Root cause: `Transaction::commit()` calls `writer.wait_for_lsn()` which blocks until WAL fsync completes — **every commit pays a full fsync**. Should use group commit (batch fsync in background thread, commit just writes to OS page cache).

### 1.4 Long-Running Stability (5 min, 5W/8T)

| Duration | TPS |
|----------|-----|
| 15s | 4,288 |
| **300s** | **1,168** |

**CRITICAL**: 73% TPS degradation over 5 minutes. Root cause candidates:
- Dead tuple accumulation (autovacuum may not keep up)
- Buffer pool fill + eviction overhead
- FSM not finding free space efficiently

### 1.5 vs C++ dstore

| Metric | FerrisDB (Rust) | dstore (C++) | Assessment |
|--------|----------------|--------------|------------|
| 20W/20T no-WAL | 3,170 TPS | ~2,500-3,000 TPS | **Comparable** |
| WAL mode | 602 TPS | ~2,000+ TPS (group commit) | **3x slower** |
| Thread scalability | 2.9x at 16T | ~3-4x (partitioned locks) | **Weaker** |
| Long-running | 73% degradation at 5min | Stable (autovacuum + FSM) | **Much weaker** |

---

## 2. Critical Gaps

### GAP-1: WAL Group Commit (Blocks production WAL mode)

**Current**: Every `commit()` calls `wait_for_lsn()` → synchronous fsync.
**Target**: Write WAL record to OS page cache → return immediately. Background flusher batches fsync every 5-100ms. Multiple commits share one fsync (group commit).

**File**: `transaction/mod.rs:314`
**Fix**: Remove `wait_for_lsn()` from normal commit path. Only wait on `synchronous_commit = true` (configurable).

**Impact**: WAL mode TPS should improve from 602 → ~3,000+ (5x improvement).

### GAP-2: Long-Running TPS Degradation

**Root cause analysis needed**. Candidates:
- Dead tuple accumulation → scan overhead grows
- Buffer pool dirty page ratio increasing → more eviction I/O
- FSM roving pointer not finding free pages as table grows

**Fix**: Profile 5-minute run, identify dominant cost. Likely need more aggressive vacuum or smarter FSM.

### GAP-3: Insert-then-WAL-fail Leaves Orphaned Tuple

**Current**: `insert()` writes tuple to page BEFORE writing WAL. If WAL fails, tuple is in buffer pool but not in undo log.
**Fix**: Record undo FIRST (or at least atomically with page write). If WAL fails after page write, the undo action must still be in memory undo_log.

**File**: `heap/mod.rs:306-324`

### GAP-4: BTree WAL Error Silently Swallowed

**Current**: `wal_write_insert()` ignores WAL write errors.
**Fix**: Return `Result`, propagate to caller. Index insert that can't be WAL-logged should fail.

**File**: `btree.rs:626-630`

### GAP-5: Error Messages Lack Debug Context

**Current**: "Tuple not found during update" — no table, page, offset info.
**Fix**: Include `table_oid`, `page_no`, `tuple_offset` in all DML error messages.

### GAP-6: BTree Free Page List Not Persisted

**Current**: `free_pages: Mutex<Vec<u32>>` is memory-only. Crash → recycled pages lost → file bloat.
**Fix**: Persist free list to catalog on checkpoint or shutdown.

---

## 3. Fix Plan (Priority Order)

### Phase 3A: Performance (must fix for production competitiveness)

| # | Item | Description | Impact |
|---|------|-------------|--------|
| **G1** | WAL group commit | Remove wait_for_lsn from commit(); let flusher batch fsync | WAL TPS 602→3000+ |
| **G2** | Long-run stability | Profile + fix 5-min degradation (vacuum/FSM/buffer) | Stable throughput |

### Phase 3B: Data Safety

| # | Item | Description | Impact |
|---|------|-------------|--------|
| **G3** | Insert WAL ordering | Ensure undo recorded before/atomically with page write | No orphan tuples |
| **G4** | BTree WAL propagation | wal_write_insert returns Result | No silent index loss |

### Phase 3C: Operational Quality

| # | Item | Description | Impact |
|---|------|-------------|--------|
| **G5** | Error message context | Add table/page/offset to all DML errors | Debuggability |
| **G6** | BTree free page persist | Save free_pages on checkpoint | No file bloat |

### Phase 3D: Enterprise Test Suite

| # | Item | Description |
|---|------|-------------|
| **T1** | WAL mode TPCC: 5W/16T/60s > 2,000 TPS | Validates group commit |
| **T2** | 5-minute stability: TPS drop < 20% | Validates long-run |
| **T3** | 50W scalability: TPS > 3,000 | Validates large working set |
| **T4** | Deadlock under load: 20W/20T/60s 0 hangs | Validates lock mgr |
| **T5** | WAL crash recovery e2e with Engine | Full lifecycle |

---

## 4. Regression Checklist

```bash
# 1. Full test suite
cargo test --all

# 2. ASan
RUSTFLAGS="-Z sanitizer=address -Cunsafe-allow-abi-mismatch=sanitizer" \
  cargo +nightly test -p ferrisdb-storage -p ferrisdb-transaction \
  --target x86_64-unknown-linux-gnu -- --test-threads=1

# 3. TPCC no-WAL (baseline)
cargo run --release --bin tpcc -- --warehouses 5 --threads 16 --duration 20 --buffer-size 100000

# 4. TPCC WAL mode (must be > 2,000 TPS after G1)
cargo run --release --bin tpcc -- --warehouses 5 --threads 16 --duration 20 --buffer-size 100000 --wal

# 5. Long-run stability (TPS drop < 20% at 5min)
cargo run --release --bin tpcc -- --warehouses 5 --threads 8 --duration 300 --buffer-size 100000

# 6. Concurrent stability
for i in $(seq 1 30); do
  cargo test -p ferrisdb-storage --test btree_tests test_btree_concurrent_insert 2>&1 | grep -q FAILED && echo "FAIL $i"
done

# 7. Sanitizer suite
bash scripts/sanitizer_check.sh
```

---

## 5. Progress Tracking

| Phase | Item | Status | Date |
|-------|------|--------|------|
| G1 | WAL group commit | PENDING | |
| G2 | Long-run stability fix | PENDING | |
| G3 | Insert WAL ordering | PENDING | |
| G4 | BTree WAL propagation | PENDING | |
| G5 | Error message context | PENDING | |
| G6 | BTree free page persist | PENDING | |
| T1 | WAL mode TPCC > 2,000 TPS | PENDING | |
| T2 | 5-min stability < 20% drop | PENDING | |
| T3 | 50W scalability | Deferred | |
| T4 | Deadlock under load | PENDING | |
| T5 | WAL crash recovery e2e | PENDING | |
