# FerrisDB SQL 对接实施方案

## 目标

让用户通过 `psql` 连接 FerrisDB，执行标准 SQL：

```
$ cargo run -p ferrisdb-sql
FerrisDB SQL Server listening on 0.0.0.0:5432

$ psql -h 127.0.0.1 -p 5432
ferrisdb=# CREATE TABLE users (id INT, name TEXT);
ferrisdb=# INSERT INTO users VALUES (1, 'Alice');
ferrisdb=# SELECT * FROM users WHERE id = 1;
 id | name
----+-------
  1 | Alice
```

## 架构

```
psql / JDBC / 应用
      │
      │ PostgreSQL Wire Protocol (TCP)
      ▼
┌──────────────┐
│   pgwire      │  Step 5: 网络协议层
│  (协议解析)   │  接收连接 → 提取 SQL → 返回结果
└──────┬───────┘
       │ SQL 字符串
       ▼
┌──────────────┐
│  DataFusion   │  Step 4: SQL 执行引擎
│ (解析+优化+  │  SQL → AST → 逻辑计划 → 物理计划 → 执行
│  执行)       │
└──────┬───────┘
       │ TableProvider trait 调用
       ▼
┌──────────────┐
│ ferrisdb-sql  │  Step 3: 胶水层
│ (适配器)     │  Arrow ↔ Row 转换, 事务管理, DML 执行
└──────┬───────┘
       │ Engine / HeapTable / Transaction API
       ▼
┌──────────────┐
│  FerrisDB     │  Step 1-2: 存储引擎 (已完成 + Schema 扩展)
│ (存储+事务)  │
└──────────────┘
```

## 执行步骤

### Step 1: 扩展 SystemCatalog 支持 Schema（在存储层）

**改动位置**: `crates/ferrisdb-storage/src/catalog.rs`
**工作量**: ~150 行
**依赖**: 无新依赖

```rust
// 新增列定义
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,      // Int32 / Int64 / Float64 / Text / Boolean / Bytes
    pub nullable: bool,
    pub position: u16,            // 列在 tuple 中的位置
}

pub enum DataType {
    Int32, Int64, Float64, Text, Boolean, Bytes,
}

// RelationMeta 扩展
pub struct RelationMeta {
    pub oid: u32,
    pub name: String,
    pub rel_type: RelationType,
    pub columns: Vec<ColumnDef>,   // 🆕 列定义
    // ... 现有字段
}
```

**具体任务**:
- [ ] 定义 `ColumnDef` 和 `DataType` 结构（在 ferrisdb-core 或 ferrisdb-storage）
- [ ] 在 `RelationMeta` 加 `columns` 字段
- [ ] 更新 `to_bytes()` / `from_bytes()` 序列化（向后兼容：columns 为空 = 旧格式）
- [ ] `create_table` 时接受列定义参数
- [ ] 测试 Schema 持久化和恢复

**验收**: `catalog.create_table_with_schema("users", vec![col("id", Int32), col("name", Text)])` 成功，重启后 Schema 仍在。

---

### Step 2: 行数据编码/解码器

**改动位置**: 新文件 `crates/ferrisdb-storage/src/row_codec.rs`
**工作量**: ~200 行
**依赖**: 无新依赖

FerrisDB 的 HeapTable 存 `Vec<u8>`，需要定义从 typed values → bytes → typed values 的编解码：

```rust
// 编码：typed values → bytes（存入 HeapTable）
pub fn encode_row(schema: &[ColumnDef], values: &[Value]) -> Vec<u8>;

// 解码：bytes → typed values（从 HeapTable 读出）
pub fn decode_row(schema: &[ColumnDef], data: &[u8]) -> Vec<Value>;

pub enum Value {
    Null,
    Int32(i32),
    Int64(i64),
    Float64(f64),
    Text(String),
    Boolean(bool),
    Bytes(Vec<u8>),
}
```

编码格式（简单紧凑）:
```
[null_bitmap: N/8 bytes][col1_data][col2_data]...
  - Int32: 4 bytes LE
  - Int64: 8 bytes LE
  - Float64: 8 bytes LE
  - Text: [len:4 LE][utf8 bytes]
  - Boolean: 1 byte
  - Bytes: [len:4 LE][raw bytes]
```

**具体任务**:
- [ ] 定义 Value 枚举 + encode_row / decode_row
- [ ] 空值处理（null bitmap）
- [ ] 测试各类型编解码 roundtrip
- [ ] 测试混合类型行

**验收**: `encode → decode` 对所有类型 roundtrip 正确，包括 NULL。

---

### Step 3: 创建 ferrisdb-sql crate（胶水层骨架）

**改动位置**: 新 crate `crates/ferrisdb-sql/`
**工作量**: ~100 行（骨架）
**依赖**: datafusion, pgwire, tokio, arrow

```
crates/ferrisdb-sql/
├── Cargo.toml
├── src/
│   ├── main.rs           # 服务启动入口
│   ├── lib.rs            # 模块导出
│   ├── catalog.rs        # DataFusion CatalogProvider
│   ├── table.rs          # DataFusion TableProvider（核心）
│   ├── executor.rs       # DML 执行器
│   ├── session.rs        # 会话+事务管理
│   ├── row_to_arrow.rs   # FerrisDB Row → Arrow RecordBatch
│   └── server.rs         # pgwire 服务端
└── tests/
    └── sql_tests.rs      # SQL 集成测试
```

**具体任务**:
- [ ] workspace Cargo.toml 加 member + 依赖
- [ ] 创建 ferrisdb-sql/Cargo.toml
- [ ] main.rs: tokio::main + 启动占位
- [ ] lib.rs: 模块声明
- [ ] 编译通过

**验收**: `cargo build -p ferrisdb-sql` 成功，`cargo run -p ferrisdb-sql` 打印 "FerrisDB SQL Server starting..."。

---

### Step 4: 实现 DataFusion TableProvider

**改动位置**: `crates/ferrisdb-sql/src/catalog.rs`, `table.rs`, `row_to_arrow.rs`
**工作量**: ~500 行
**依赖**: Step 1 + Step 2

这是核心——让 DataFusion 能读写 FerrisDB 的数据：

```rust
// table.rs
struct FerrisTable {
    engine: Arc<Engine>,
    table_name: String,
    schema: Arc<ArrowSchema>,  // Arrow 格式的 Schema
}

impl TableProvider for FerrisTable {
    fn schema(&self) -> SchemaRef { self.schema.clone() }

    fn scan(&self, projection, filters, limit) -> Result<Arc<dyn ExecutionPlan>> {
        // 返回 FerrisScanExec，它在 execute() 时调 HeapScan
    }

    fn insert_into(&self, input) -> Result<Arc<dyn ExecutionPlan>> {
        // 返回 FerrisInsertExec，它从 Arrow 转行再调 HeapTable::insert
    }
}
```

```rust
// row_to_arrow.rs
fn rows_to_record_batch(schema, rows: Vec<Vec<Value>>) -> RecordBatch;
fn arrow_row_to_bytes(schema, batch: &RecordBatch, row_idx: usize) -> Vec<u8>;
```

**具体任务**:
- [ ] `row_to_arrow.rs`: Value → Arrow Array 批量转换
- [ ] `catalog.rs`: CatalogProvider + SchemaProvider 实现
- [ ] `table.rs`: FerrisTable + TableProvider::schema + scan
- [ ] `table.rs`: FerrisScanExec + ExecutionPlan::execute → RecordBatchStream
- [ ] `table.rs`: TableProvider::insert_into + FerrisInsertExec
- [ ] `session.rs`: SessionContext 创建 + 注册 catalog
- [ ] 测试: DataFusion 直接查询（无 pgwire）

**验收**:
```rust
let ctx = create_session(engine);
let df = ctx.sql("SELECT * FROM users WHERE id > 0").await?;
df.show().await?;  // 打印 Arrow 表格
```

---

### Step 5: pgwire 服务端

**改动位置**: `crates/ferrisdb-sql/src/server.rs`, `main.rs`
**工作量**: ~300 行
**依赖**: Step 4

```rust
// server.rs
struct FerrisQueryHandler {
    engine: Arc<Engine>,
}

impl SimpleQueryHandler for FerrisQueryHandler {
    async fn do_query(&self, query: &str) -> Vec<Response> {
        // 1. 创建 DataFusion SessionContext
        // 2. ctx.sql(query).await
        // 3. 收集结果 → pgwire Response
    }
}
```

```rust
// main.rs
#[tokio::main]
async fn main() {
    let engine = Engine::open(config)?;
    let handler = Arc::new(FerrisQueryHandler::new(engine));
    pgwire::tokio::process_socket(listener, handler).await;
}
```

**具体任务**:
- [ ] `server.rs`: SimpleQueryHandler 实现
- [ ] `server.rs`: Arrow RecordBatch → pgwire 结果行转换
- [ ] `main.rs`: TCP 监听 + 连接处理
- [ ] 事务管理: BEGIN/COMMIT/ABORT 拦截
- [ ] 错误处理: SQL 错误 → PG 错误码

**验收**:
```bash
$ cargo run -p ferrisdb-sql
$ psql -h 127.0.0.1 -p 5432
ferrisdb=# CREATE TABLE t1 (id INT, name TEXT);
CREATE TABLE
ferrisdb=# INSERT INTO t1 VALUES (1, 'hello');
INSERT 0 1
ferrisdb=# SELECT * FROM t1;
 id | name
----+-------
  1 | hello
```

---

### Step 6: DML 完善 + 事务

**改动位置**: `crates/ferrisdb-sql/src/executor.rs`, `session.rs`
**工作量**: ~300 行
**依赖**: Step 5

- [ ] UPDATE 执行: scan + filter → 逐行 update
- [ ] DELETE 执行: scan + filter → 逐行 delete
- [ ] BEGIN/COMMIT/ROLLBACK 会话级事务
- [ ] 自动事务: 单条 SQL 默认 auto-commit

**验收**: 完整 CRUD + 事务

---

### Step 7: 集成测试 + 稳定性

**工作量**: ~200 行测试
- [ ] SQL 语法测试（CREATE/INSERT/SELECT/UPDATE/DELETE）
- [ ] 事务测试（BEGIN/COMMIT/ROLLBACK/并发）
- [ ] 错误处理（无效 SQL、表不存在、类型不匹配）
- [ ] psql 兼容性（\dt, \d table_name）
- [ ] JDBC 连接测试

---

## 里程碑

```
Step 1 (Schema)        ██ 1天    → catalog 理解列类型
Step 2 (Row Codec)     ██ 1天    → typed values ↔ bytes
Step 3 (Crate 骨架)    █ 0.5天   → cargo build 通过
Step 4 (TableProvider)  █████ 3天 → DataFusion 能读写 FerrisDB
Step 5 (pgwire)         ███ 2天   → psql 连上执行 SQL
Step 6 (DML+事务)       ███ 2天   → 完整 CRUD + 事务
Step 7 (测试+稳定)      ██ 1天    → 集成测试通过
                        ──────────
                        总计 ~10天
```

## 风险

| 风险 | 影响 | 缓解 |
|------|------|------|
| DataFusion INSERT/UPDATE API 不成熟 | DML 需自建 | 用 sqlparser 直接解析 DML，旁路 DataFusion |
| async/sync 桥接性能 | OLTP 延迟增加 | spawn_blocking + 连接池 |
| Arrow 转换开销 | 单行操作变慢 | OLTP 小查询走旁路不经 Arrow |
| DataFusion 版本升级 | trait 接口变化 | 锁定大版本号 |
