# FerrisDB

A high-performance transactional storage engine written in Rust.

## Overview

FerrisDB is a single-node OLTP storage engine providing full ACID transactions with MVCC snapshot isolation. The architecture draws inspiration from [openGauss DStore](https://gitee.com/opengauss) — a C++ storage engine developed by Huawei under GPL v2+ — but is an **independent Rust implementation written from scratch**. No C++ source code was translated, copied, or incorporated.

FerrisDB achieves **3x+ throughput** over the reference C++ implementation on standard TPC-C benchmarks, while delivering memory safety and crash recovery guarantees through Rust's ownership model and a carefully designed WAL subsystem.

### Key Features

- **Full ACID Transactions** — WAL-based durability with synchronous commit and group commit
- **MVCC Snapshot Isolation** — Lock-free reads with point-in-time consistent snapshots
- **Crash Recovery** — WAL redo + automatic undo rollback of uncommitted transactions
- **B+Tree Indexes** — Concurrent insert/delete/scan with page split, merge, and recycling
- **Data Integrity** — Page CRC32C, WAL CRC, torn page detection, full-page writes
- **Production Infrastructure** — Runtime configuration (GUC), structured logging, performance statistics, numeric error codes

## Performance

Full TPC-C benchmark with all 5 transaction types (NewOrder 45%, Payment 43%, OrderStatus 4%, Delivery 4%, StockLevel 4%):

| Workload | FerrisDB | Reference C++ | Ratio |
|----------|----------|---------------|-------|
| TPC-C 20W / 16 threads | 4,595 TPS | ~1,451 TPS | **3.2x** |
| TPC-C 5W / 8 threads | 7,395 TPS | — | Peak throughput |
| TPC-C 5W / 4 threads | 5,873 TPS | — | — |

*Measured on WSL2 / x86_64 / 16 GB RAM. Buffer pool 100K pages, 10s duration.*

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   ferrisdb-bench                     │
│              TPC-C Benchmark (5 txn types)           │
├─────────────────────────────────────────────────────┤
│                ferrisdb-transaction                   │
│    HeapTable · Transaction · MVCC · Undo · Lock Mgr  │
├─────────────────────────────────────────────────────┤
│                  ferrisdb-storage                     │
│  BufferPool · B+Tree · WAL · Pages · Smgr · LOB     │
│  Checkpoint · Recovery · Control File · Tablespace   │
├─────────────────────────────────────────────────────┤
│                   ferrisdb-core                       │
│  Types · Locks · Atomics · Config · Logging · Stats  │
└─────────────────────────────────────────────────────┘
```

| Crate | Lines | Description |
|-------|-------|-------------|
| `ferrisdb-core` | 5.1K | Primitive types (Xid, CSN, LSN), lock primitives (LWLock, ContentLock, SpinLock), GUC configuration, logging framework, statistics collection |
| `ferrisdb-storage` | 14.5K | Buffer pool with LRU eviction, B+Tree index with WAL integration, write-ahead log with recovery, page management with checksums, storage manager, tablespace/segment, LOB, parallel scan |
| `ferrisdb-transaction` | 4.7K | Transaction lifecycle, MVCC visibility, undo-based rollback, savepoints, heap table DML, deadlock detection, lock manager with Condvar signaling |
| `ferrisdb-bench` | 2.0K | Full TPC-C implementation matching the reference C++ benchmark |

## Quick Start

```bash
# Build
cargo build --release

# Run all 916 tests
cargo test --all

# TPC-C benchmark (in-memory mode)
cargo run --release --bin tpcc -- \
    --warehouses 5 --threads 4 --duration 30

# TPC-C with WAL persistence
cargo run --release --bin tpcc -- \
    --warehouses 5 --threads 4 --duration 30 --wal

# TPC-C matching the reference C++ default configuration
cargo run --release --bin tpcc -- \
    --warehouses 20 --threads 20 --duration 120 \
    --buffer-size 300000 --wal
```

## Storage Engine Internals

### Buffer Pool

- Pin / unpin with atomic reference counting
- Clock-based victim selection for page eviction
- Background dirty page writer with **LSN-ordered flush** (older pages first)
- **WAL-before-data enforcement** — verifies WAL fsync before writing data pages
- CRC32C checksum computed on write, verified on read
- Page magic number for torn page detection

### B+Tree Index

- Insert, delete, lookup, prefix scan, reverse scan, range scan
- Page split with nkeys validation to detect concurrent modification
- Page merge when key count drops below threshold
- Free page list for recycled empty pages
- Unique constraint enforcement (`insert_unique`)
- WAL record generation for crash recovery of index operations

### Write-Ahead Logging

- File-based WAL with 16 MB segment rotation
- Background WAL flusher using `parking_lot_core` park/unpark (group commit)
- Synchronous commit via `wait_for_lsn` with Condvar notification
- **Full-page writes** — complete page image in WAL before first dirty flush
- CRC32C on every record; torn record detection on recovery
- Undo record serialization for crash rollback of uncommitted transactions

### Transaction Management

- Begin / Commit / Abort with CSN (Commit Sequence Number) allocation
- MVCC snapshot isolation with `is_visible(xmin, xmax, snapshot)`
- `fetch_visible` and `next_visible` for snapshot-filtered reads
- Savepoint with `rollback_to_savepoint` (partial undo)
- Deadlock detection via BFS cycle detection on wait-for graph
- Configurable transaction timeout

### Crash Recovery

1. **Scan** — discover WAL files, locate last checkpoint
2. **Redo** — replay heap and B-Tree WAL records (LSN-based idempotency)
3. **Undo** — collect uncommitted transactions from WAL, rollback via undo actions
4. **Flush** — write recovered pages to disk
5. **Control file** — persist checkpoint LSN with CRC for next startup

### Configuration (GUC)

All parameters are runtime-configurable via atomic reads:

```ini
shared_buffers = 50000           # Buffer pool size in pages (400 MB)
wal_buffers = 16384              # WAL buffer size in bytes
synchronous_commit = true        # Wait for WAL fsync on commit
checkpoint_interval_ms = 60000   # Checkpoint interval
transaction_timeout_ms = 0       # Transaction timeout (0 = disabled)
log_level = 3                    # 0=OFF, 1=ERROR, 2=WARN, 3=INFO, 4=DEBUG
bgwriter_delay_ms = 200          # Background writer interval
bgwriter_max_pages = 100         # Max pages per background write round
wal_flusher_delay_ms = 5         # WAL flusher interval
deadlock_timeout_ms = 1000       # Deadlock detection interval
max_connections = 64             # Max concurrent transaction slots
```

## Testing

916 tests organized by subsystem:

```bash
cargo test -p ferrisdb-core         # 117 tests: locks, atomics, config, logging, stats
cargo test -p ferrisdb-storage      # 560 tests: buffer, B-Tree, WAL, pages, recovery, segments
cargo test -p ferrisdb-transaction  # 239 tests: transactions, heap, MVCC, undo, savepoints
```

Test categories include:
- Unit tests for every module (inline `#[cfg(test)]`)
- Integration tests for DML, crash recovery, WAL roundtrip
- Concurrent stress tests (4–16 threads)
- Fault injection (checksum corruption, torn pages, pool exhaustion)
- Edge cases (empty keys, max-size tuples, boundary conditions)

## Design Decisions

| Decision | Rationale |
|----------|-----------|
| 128-byte `BufferDesc` (vs 256-byte in C++) | 2x better L3 cache utilization; 24x perf improvement measured |
| `parking_lot_core` park/unpark for lock waiting | Eliminates pure-spin livelock under 16+ thread contention |
| Atomic `AtomicU8` array for Free Space Map | Zero-lock overhead on insert hot path |
| WAL writes to OS page cache + background fsync | Matches production group commit pattern; 11% overhead vs no-WAL |
| `crc32fast` for page and WAL checksums | Hardware-accelerated CRC32C; zero measurable overhead |

## Relationship to DStore

FerrisDB's architecture is inspired by studying the [openGauss DStore](https://gitee.com/opengauss) storage engine, which is a C++ implementation released under GPL v2+ by Huawei Technologies. Key architectural patterns adopted include:

- Buffer pool with LWLock-based content locking
- WAL-before-data protocol with background WAL writer
- MVCC using CSN (Commit Sequence Number) for visibility ordering
- Undo-based transaction rollback
- B+Tree with page split and right-link chain

**FerrisDB is an independent implementation.** It was written entirely in Rust without translating, copying, or incorporating any C++ source code from DStore or openGauss. The design patterns listed above are well-established database engineering techniques used across PostgreSQL, MySQL, Oracle, and other systems.

FerrisDB is licensed under **Apache License 2.0**, which permits commercial use, modification, and distribution.

## License

```
Copyright 2026 FerrisDB Contributors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
```

See [LICENSE](LICENSE) for the full license text.
