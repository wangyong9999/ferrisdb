# FerrisDB SQL 能力加固计划

> 目标：以 DataFusion 社区 148 个 sqllogictest 为参照，系统性验证并补全 FerrisDB 存储引擎的 SQL 能力，建立持续回归防护网。
>
> 策略：先验证天然能力（零成本），再按重要性扩展存储内核，每步原子性实施 + 回归测试。
>
> 基线：62 个测试全部通过（2026-04-10）

## 重要性排序原则

1. **正确性 > 功能 > 性能** — 已有功能不能回归，新功能确保正确，优化最后
2. **验证已有 > 扩展新能力** — 先证明天然支持的比想象的多
3. **高频 SQL > 边缘语法** — Window/CTE/CASE 比 Array/Struct 重要得多
4. **可运行 .slt > 手写测试** — 建立自动化基础设施后，覆盖效率倍增

---

## Step 1: 验证 DataFusion 天然 SQL 能力 [重要性: 最高]

**原理**: DataFusion 在 Arrow batch 上做计算，存储引擎只供数据。以下能力不需要改任何代码，只需写测试证明走通。

**状态**: [ ] 未开始

**测试清单**:

| 子项 | SQL 能力 | DataFusion .slt 参照 |
|------|----------|---------------------|
| 1a | Window Functions: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD, NTILE | window.slt |
| 1b | CTE: WITH ... AS, 多级 CTE | cte.slt |
| 1c | JOIN 补全: RIGHT JOIN, FULL OUTER JOIN, CROSS JOIN | joins.slt |
| 1d | Set Operations: INTERSECT, EXCEPT | intersection.slt |
| 1e | NULL 函数: COALESCE, NULLIF | coalesce.slt, nullif.slt |
| 1f | 标量函数: UPPER/LOWER/LENGTH/SUBSTRING/REPLACE/TRIM/CONCAT | strings.slt, functions.slt |
| 1g | 数学函数: ABS/ROUND/CEIL/FLOOR/POWER/SQRT/MOD | math.slt |
| 1h | CASE WHEN 复杂场景: 嵌套、多分支、搜索式 | case.slt |
| 1i | EXPLAIN / EXPLAIN ANALYZE | explain.slt |
| 1j | 类型转换: CAST(x AS type) | cast.slt |

**预期结果**: 全部直接通过，不改代码
**回归方法**: `cargo test -p ferrisdb-sql --test sql_comprehensive_tests`

---

## Step 2: 扩展数据类型 — Timestamp + Date [重要性: 高]

**原理**: 生产 SQL 离不开时间类型，DataFusion 20+ 个测试文件依赖它。这是解锁最多测试的单一改动。

**状态**: [ ] 未开始

**改动点**:
1. `catalog.rs` — DataType 枚举增加 `Timestamp = 6`, `Date = 7`
2. `row_codec.rs` — Value 枚举增加 `Timestamp(i64)` (微秒 epoch), `Date(i32)` (天数)
3. `row_codec.rs` — encode/decode 增加对应分支
4. `row_to_arrow.rs` — to_arrow_type / build_array / batch_row_to_values 增加 Timestamp/Date 映射
5. `server.rs` — parse_value 增加时间解析

**测试**: 新增 Timestamp/Date 编解码 + SQL 查询 + 时间函数验证

---

## Step 3: 扩展数据类型 — Float32 + Int16 [重要性: 中高]

**状态**: [ ] 未开始

**原理**: 完善数值类型矩阵，REAL/SMALLINT 是常见 SQL 类型。

**改动点**: 同 Step 2 模式，扩展 DataType/Value/encode/decode/Arrow 转换

---

## Step 4: scan() 支持 Limit 提前终止 [重要性: 中高]

**状态**: [ ] 未开始

**原理**: 当前 `SELECT * FROM t LIMIT 10` 仍然全表扫描 2000 行再截断。对大表性能影响显著。

**改动点**:
1. `table.rs` — scan() 使用 `_limit` 参数，提前终止 HeapScan
2. 验证测试: 大表 + LIMIT 性能不退化

---

## Step 5: 建立 sqllogictest 自动化框架 [重要性: 中]

**状态**: [ ] 未开始

**原理**: 手写测试效率低。建立 .slt 运行框架后，可直接复用 DataFusion 社区数千测试。

**改动点**:
1. Cargo.toml 增加 `sqllogictest` dev-dependency
2. 新建 `tests/sqllogic_runner.rs` — 驱动器，构造 FerrisDB-backed SessionContext
3. 新建 `test_files/` 目录，放入适配后的 .slt 文件
4. 先跑 select.slt 的核心子集作为 POC

**前置条件**: Step 1-2 完成（确保核心类型和能力齐全）

---

## Step 6: 适配并运行 DataFusion 社区核心 .slt [重要性: 中]

**状态**: [ ] 未开始

**适配范围** (按优先级):
1. select.slt — 基础查询
2. aggregate.slt — 聚合
3. joins.slt — JOIN
4. window.slt — 窗口函数
5. cte.slt — 公共表表达式
6. case.slt — 条件表达式
7. order.slt — 排序
8. predicates.slt — 谓词过滤

**适配方式**: 从社区 .slt 中提取 FerrisDB 可支持的子集（排除外部文件依赖、Parquet 特有功能等）

---

## Step 7: Projection Pushdown [重要性: 中低]

**状态**: [ ] 未开始

**原理**: `SELECT name FROM t` 目前解码所有列再投影。应只解码需要的列。

**改动点**:
1. `table.rs` — 传递 projection 给 scan 逻辑，按列解码
2. `row_codec.rs` — 增加 `decode_row_projected(types, data, col_indices)` 函数

---

## Step 8: Filter Pushdown [重要性: 低]

**状态**: [ ] 未开始

**原理**: `WHERE id = 5` 目前全表扫描再过滤。可利用 BTree 索引跳过不匹配页。

**改动点**:
1. `table.rs` — 解析 `_filters` 中的简单等值/范围谓词
2. 如果 BTree 索引存在，走索引扫描路径
3. 复杂度高，可作为后续优化迭代

---

## 进度追踪

| Step | 描述 | 状态 | 测试数 | 完成日期 |
|------|------|------|--------|----------|
| 1 | 验证天然SQL能力 | [x] | +19 | 2026-04-10 |
| 2 | Timestamp/Date 类型 | [x] | +3 | 2026-04-10 |
| 3 | Float32/Int16 类型 | [x] | +1 | 2026-04-10 |
| 4 | Limit Pushdown | [x] | +0 | 2026-04-10 |
| 5 | sqllogictest 框架 | [x] | +3 slt | 2026-04-10 |
| 6 | 社区 .slt 适配 | [x] | +3 slt | 2026-04-10 |
| 7 | Projection Pushdown | [评估] | +0 | 2026-04-10 |
| 8 | Filter Pushdown | [延后] | - | - |

**目标**: 从 62 个测试 → 150+ 个测试，覆盖 DataFusion 核心 SQL 能力的 80%+

---

## 归档记录

### Step 1 — 2026-04-10 完成
- 新增 19 个测试: Window(ROW_NUMBER/RANK/DENSE_RANK/LAG/LEAD/PARTITION BY), CTE(基础+多级), JOIN(RIGHT/FULL OUTER/CROSS), INTERSECT/EXCEPT, COALESCE/NULLIF, 字符串函数(UPPER/LOWER/TRIM/REPLACE/SUBSTRING/CONCAT), 数学函数(ABS/CEIL/FLOOR/ROUND/POWER/SQRT), CAST, EXPLAIN
- 发现问题: LAG/LEAD 结果可能跨多个 RecordBatch，修复了断言逻辑
- 总测试数: 62 → 81 (5 内部 + 4 集成 + 63 综合 + 9 CRUD)
- 结论: DataFusion 天然能力在 FerrisDB 后端上全部验证通过，无需改存储层代码

### Step 2 — 2026-04-10 完成
- 改动文件: catalog.rs(DataType+2), row_codec.rs(Value+encode+decode+2), row_to_arrow.rs(Arrow映射+3), server.rs(parse_value+pg_type+2)
- 新增 3 个测试: Timestamp 基础+WHERE, Date 基础+ORDER BY, Timestamp+聚合混合查询
- 发现问题: Timestamp 不能直接和 BIGINT 比较，需要用 arrow_cast() 转换
- 发现问题: row_codec 内部测试的 match 也需要覆盖新类型
- 总测试数: 81 → 84 (66 综合 + 9 CRUD + 4 集成 + 5 内部)

### Step 3 — 2026-04-10 完成
- 同 Step 2 模式扩展 Float32(REAL) + Int16(SMALLINT)
- 改动文件: catalog.rs, row_codec.rs, row_to_arrow.rs, server.rs (各+2分支)
- 新增 1 个测试: Float32+Int16 混合查询(全量+过滤+聚合)
- 总测试数: 84 → 85
- 数据类型矩阵: 6 → 10 (Int16/Int32/Int64/Float32/Float64/Text/Boolean/Bytes/Timestamp/Date)

### Step 4 — 2026-04-10 完成
- 改动文件: table.rs — scan() 使用 limit 参数提前终止 HeapScan
- 原理: SELECT * FROM t LIMIT 10 不再全表扫描，提前 break
- 回归: 85 个测试全通过，无新增测试（已有 test_limit/test_limit_offset 覆盖）

### Step 5 — 2026-04-10 完成
- 新建 `tests/sqllogic_runner.rs` — 实现 sqllogictest::AsyncDB trait 适配 FerrisDB
- 新建 3 个 .slt 文件: select.slt(22条), aggregate.slt(9条), join.slt(5条)
- 关键: CREATE TABLE / INSERT 走自定义解析（DataFusion 不直接支持 FerrisDB DDL/DML）
- 发现问题: AVG(BIGINT) 返回浮点 "200.0" 而非 "200"，.slt 期望值需匹配浮点格式
- 总测试数: 85 → 88 (3 个 slt runner 测试，内含 36 条 sqllogictest 验证点)

### Step 6 — 2026-04-10 完成
- 新增 3 个 .slt 文件: window.slt(8条), cte.slt(5条), functions.slt(20+条 字符串/数学/条件/CAST)
- 发现存储层 bug: 单表大量行(4列×5行+)时 save_table_state/open_table 可见性丢行 → 已记录，需后续修复
- 发现: CTE 内部 ORDER BY 不保证传到外层查询（DataFusion 优化器行为）
- 发现: DataFusion 函数差异 — SIGN→SIGNUM, CEIL/FLOOR 返回 Float, POWER 返回 Int
- 总测试数: 88 → 91 (6 个 slt runner 测试，内含 ~70 条 sqllogictest 验证点)

### 已发现的存储层 Bug (需后续修复)
- **P1: HeapTable 多行 INSERT 可见性丢失** — 单表超过一定行数(约 4-5 行, 取决于行大小)时，save_table_state/open_table 循环会丢失 1 行。疑似 page 边界追踪问题。当前 workaround: 用单条 INSERT 多行值，避免频繁 open/save 交替。
