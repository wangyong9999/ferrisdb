# FerrisDB Single-Node MVP Commercial Readiness Plan

> Last updated: 2026-04-06
> Latest commit: cc077aa (1,021 tests, 0 failures)

---

## 1. Current State Assessment

### 1.1 Sanitizer Results

| Sanitizer | Scope | Result | Notes |
|-----------|-------|--------|-------|
| **ASan** (memory) | storage + transaction 818 tests | **CLEAN** | 0 errors |
| **TSan** (data race) | BTree + Heap concurrent tests | **2 known FPs** | parking_lot locks invisible to TSan |
| **UBSan** | N/A | Not available | Rust nightly `-Zsanitizer=undefined` not supported on this target |
| **Miri** | N/A | Cannot run | Miri doesn't support FFI/syscalls |

**TSan False Positives** (documented in `scripts/sanitizer_check.sh`):
- `btree.rs` `insert_item_sorted` — write to page data protected by `pinned.lock_exclusive()`
- `heap_page.rs` `insert_tuple_from_parts` — write to page data protected by `pinned.lock_exclusive()`
- Root cause: TSan cannot track `parking_lot` futex-based synchronization (parking_lot#270)

### 1.2 Unsafe Code Audit

120 unsafe blocks total. All audited:

| Category | Count | Status |
|----------|-------|--------|
| Page ptr cast (`from_ptr`/`from_bytes`) | 58 | **Reviewed** — protected by buffer pool pin + page lock |
| WAL serialization (`ptr::read`/`from_raw_parts`) | 17 | **Reviewed** — repr(C) packed struct, standard pattern |
| `Send`/`Sync` impl | 26 | **Fixed** — TransactionManagerRef raw ptr eliminated, BufTable/WalBuffer audited |
| `UnsafeCell` | 21 | **Reviewed** — BufTable tag protected by RwLock, WalBuffer by atomic regions |
| `parking_lot_core` | 6 | **Reviewed** — ContentLock park/unpark, standard pattern |

**Eliminated risks:**
- ~~`TransactionManagerRef` raw `*const` pointer~~ → replaced with `Arc<TransactionManager>` ✅
- ~~Page checksum not verified on read~~ → returns `Err` on CRC mismatch ✅
- ~~HeapTable page_no overflow~~ → checked against `u32::MAX` ✅

### 1.3 Capability Matrix (MVP)

| Capability | Required | FerrisDB | Status |
|-----------|----------|----------|--------|
| WAL durability + CRC32 | Yes | ✅ | **DONE** |
| Crash recovery (redo + undo) | Yes | ✅ e2e verified (3 tests) | **DONE** |
| MVCC snapshot isolation | Yes | ✅ CSN-based, `is_visible()` | **DONE** |
| Auto checkpoint + WAL truncation | Yes | ✅ 60s interval, WAL segment cleanup | **DONE** |
| AutoVacuum / dead tuple reclaim | Yes | ✅ 10s interval, background thread | **DONE** |
| Page checksum on write + read | Yes | ✅ CRC32C set/verify | **DONE** |
| Transaction timeout enforcement | Yes | ✅ 30s default, bg forced abort | **DONE** |
| Graceful shutdown | Yes | ✅ wait txns → checkpoint → fsync all | **DONE** |
| Engine.begin() shutdown guard | Yes | ✅ is_shutdown check | **DONE** |
| EngineStats monitoring API | Yes | ✅ hit rate, WAL offset, active txns, dirty pages, checkpoint LSN | **DONE** |
| WAL error propagation | Yes | ✅ `wal_write()` returns `Result<()>` | **DONE** |
| Control file fsync | Yes | ✅ write → fsync → rename → fsync parent | **DONE** |
| BTree concurrent correctness | Yes | ✅ split_mutex + right_link lookup, 30x stable | **DONE** |
| LockManager no livelock | Yes | ✅ parking_lot Condvar + exp backoff, 30x stable | **DONE** |
| Connection slot limits | Partial | ✅ max_slots with clear error | Acceptable |
| Full-page write (FPW) | Yes | ✅ FPW before first dirty flush | **DONE** |
| WAL archiving | No | ❌ | Not MVP |
| Online DDL | No | ❌ | Not MVP |
| ANALYZE / stats | No | ❌ | Not MVP |
| Replication | No | ❌ | Not MVP |
| Connection queuing | No | ❌ | Not MVP |

### 1.4 Performance Baseline

| Config | TPS | Hit Rate | Notes |
|--------|-----|----------|-------|
| 1W/1T | 3,473 | ~100% | Single-thread baseline |
| 5W/16T | 4,583 | ~100% | Peak concurrent throughput |
| 20W/20T | 2,700-3,300 | 99.8% | Standard config (WSL2 variance) |

No regression from any fix applied.

### 1.5 Test Coverage

| Category | Count | Status |
|----------|-------|--------|
| Total tests | 1,024 | 0 failures |
| Crash recovery e2e | 3 | committed/uncommitted/mixed |
| BTree concurrent (30x stable) | 3 | insert, insert+lookup, insert+lookup+delete |
| LockManager concurrent (30x stable) | 1 | 8-thread exclusive contention |
| Production hardening | 5 | checksum corruption, pool exhaustion, boundary keys, WAL |
| Audit fix tests | 11 | CRC, BufTable, undo types, Drop safety, ParallelRedo |
| ASan clean | 818 | storage + transaction |

---

## 2. Fix Plan

### Phase 1: P0 — Data Integrity ✅ ALL DONE

| # | Item | Status | Date |
|---|------|--------|------|
| P0-1 | Page checksum verify on read | **DONE** | 2026-04-06 |
| P0-2 | Eliminate raw ptr → `Arc<TransactionManager>` | **DONE** | 2026-04-06 |
| P0-3 | WalBuffer UnsafeCell safety audit | **DONE** (correct pattern, SAFETY docs) | 2026-04-06 |

### Phase 2: P1 — Boundary Safety + Sanitizer CI ✅ ALL DONE

| # | Item | Status | Date |
|---|------|--------|------|
| P1-1 | Page number overflow check | **DONE** | 2026-04-06 |
| P1-2 | UBSan pass | N/A (not supported) | 2026-04-06 |
| P1-3 | Sanitizer regression script | **DONE** (`scripts/sanitizer_check.sh`) | 2026-04-06 |

### Phase 3: P2 — Test Hardening ✅ ALL DONE

| # | Item | Status | Date |
|---|------|--------|------|
| P2-1 | Page CRC corruption test | **DONE** | 2026-04-06 |
| P2-2 | Engine checkpoint e2e test | **DONE** (`test_engine_checkpoint_crash_recovery_e2e`) | 2026-04-06 |
| P2-3 | Transaction timeout test | **DONE** | 2026-04-06 |
| P2-4 | Buffer pool exhaustion test | **DONE** | 2026-04-06 |

### Phase 4: Final Regression ✅ DONE

| Check | Result |
|-------|--------|
| Full test suite | 1,024 tests, 0 failures |
| ASan clean | ✅ 818 tests |
| TSan | ✅ Known FPs only |
| TPCC 20W/20T/30s | ~3,100 TPS (baseline) |
| BTree concurrent 30x | 0 failures |
| LockManager concurrent 30x | 0 failures |

---

## 3. Regression Checklist (Per Fix)

```bash
# 1. Full test suite
cargo test --all

# 2. ASan
RUSTFLAGS="-Z sanitizer=address -Cunsafe-allow-abi-mismatch=sanitizer" \
  cargo +nightly test -p ferrisdb-storage -p ferrisdb-transaction \
  --target x86_64-unknown-linux-gnu -- --test-threads=1

# 3. TPCC performance (no regression)
cargo run --release --bin tpcc -- \
  --warehouses 20 --threads 20 --duration 30 --buffer-size 300000

# 4. Concurrent stability (30x)
for i in $(seq 1 30); do
  cargo test -p ferrisdb-storage --test btree_tests test_btree_concurrent_insert 2>&1 | grep -q FAILED && echo "FAIL $i"
done

# 5. One-click sanitizer suite
bash scripts/sanitizer_check.sh
```
