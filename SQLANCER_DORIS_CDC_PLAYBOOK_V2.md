# SQLancer Doris CDC 完整使用手册

## 📖 快速入门 (Quick Start)

### 前置条件
- Flink CDC 容器已启动
- MySQL 和 Doris 已启动
- Python 3.8+

### 5 分钟快速开始

```bash
cd /home/wyh/flink-cdc/tools/cdcup

# 1. 清空旧数据（首次使用必做）
mysql -h 127.0.0.1 -P 32774 -u root database0 \
  -e "DROP TABLE IF EXISTS t0;"
mysql -h 127.0.0.1 -P 32777 -u root database0 \
  -e "DROP TABLE IF EXISTS t0;"

# 2. 启动 CDC Pipeline
./cdcup.sh pipeline pipeline-definition.yaml

# 3. 运行完整测试（自动执行所有步骤）
bash sql_generator_suite.sh

# 4. 查看结果
cat /tmp/sqla_test_*/test_report.txt
```

---

## 🆕 今日工作更新 (2026-03-23)

### 今日新增与完成

- 新增并验证了三类生成器：`dml_generator.py`、`ddl_generator.py`、`select_generator.py`
- 新增了端到端与验证脚本：`run_sqlancer_cdc_e2e.sh`、`test_cdc_dml_integrated.sh`、`test_select_verify_sync.sh`
- 补充了阶段与修复报告：`PHASE1_DML_PROGRESS.md`、`CDC_SYNC_FIX_REPORT.sh`、`COMPLETE_IMPLEMENTATION_REPORT.sh`
- 完成一次 CDC 列缺失问题定位与修复，验证结果为 MySQL 与 Doris 的 `t0` 表结构和样例数据可对齐

### 本次验证结论

- DML 生成器可复现性正常（同 seed 可复现）
- DDL/SELECT 生成器可用，且可纳入统一自动化流程
- CDC 同步链路在“清表 + 重新提交 pipeline + 重新建表 + 写入样例”路径下恢复正常

### 与官方文档对齐的关键注意项

- Doris Connector `24.0.0+` 场景下，Flink CDC 需要 `3.1+`（建议统一在同一版本代际）
- 使用 Exactly-Once 时，`sink.label-prefix` 需要全局唯一，避免重复 label 导致提交异常
- 若不要求 Exactly-Once，可评估 `sink.enable-2pc=false`（结合业务幂等策略使用）

### 今日推荐回归命令

```bash
# 1) 一键端到端
bash run_sqlancer_cdc_e2e.sh

# 2) 仅 DML 集成验证
bash test_cdc_dml_integrated.sh

# 3) SELECT 同步校验
bash test_select_verify_sync.sh
```

---

## 🛠️ 工具参考手册

### DML 生成器

```bash
python dml_generator.py [OPTIONS]

选项:
  --count N              要生成的语句数量 (默认: 20)
  --seed N               随机种子，确保能重复得到相同结果
  --output-sql FILE      输出 SQL 文件路径
  --output-json FILE     输出 JSON 格式
  --table-name NAME      表名称 (默认: t0)

# 示例
python dml_generator.py --count 30 --seed 42 --output-sql dml.sql
```

**特性**:
- 约束感知：自动包含 NOT NULL 列
- 支持修饰符: LOW_PRIORITY, HIGH_PRIORITY, DELAYED
- 完全可重现：同 seed 得到相同语句

---

### DDL 生成器

```bash
python ddl_generator.py [OPTIONS]

选项:
  --count N              语句数量 (默认: 10)
  --seed N               随机种子
  --type TYPE            类型选择: create|alter|index|drop|mix (默认: mix)
  --output-sql FILE      输出 SQL 文件
  --table-name NAME      表名称 (默认: t0)

# 示例
python ddl_generator.py --count 3 --type create --seed 2024 --output-sql ddl.sql
```

**支持操作**:
- CREATE TABLE: 2-6 列，多种数据类型
- ALTER TABLE: 添加新列
- CREATE INDEX: 索引创建
- DROP TABLE: 表删除

---

### SELECT 生成器

```bash
python select_generator.py [OPTIONS]

选项:
  --count N              语句数量 (默认: 10)
  --seed N               随机种子
  --type TYPE            类型: simple|aggregate|count|validate|mix
  --output-sql FILE      输出 SQL 文件
  --table-name NAME      表名称 (默认: t0)

# 示例
python select_generator.py --count 10 --type count --output-sql select.sql
```

**支持查询**:
- Simple SELECT: WHERE, ORDER BY, LIMIT
- Aggregate: COUNT, SUM, AVG, MAX, MIN
- Validation: 行计数和 MD5 校验

---

### 自动化测试套件

```bash
bash sql_generator_suite.sh [OPTIONS]

环境变量:
  DML_COUNT=20           DML 语句数量
  DDL_COUNT=5            DDL 语句数量
  SELECT_COUNT=10        SELECT 语句数量
  BASE_SEED=12345        随机种子
  WAIT_SYNC=15           CDC 同步等待秒数
  OUTPUT_DIR=/tmp/...    输出目录

# 示例
DML_COUNT=50 DDL_COUNT=10 bash sql_generator_suite.sh
```

---

## 🧠 机制说明与常见现象（2026-03-24）

### 为什么 DML 阶段后 MySQL/Doris 行数会短暂不一致

这属于 CDC 的正常异步传播现象：

- source（MySQL）先提交事务并立即可见。
- sink（Doris）通过 Flink CDC 异步消费 binlog，并不是每条语句都实时同步完成。
- 因此在 Step 4 刚结束时看到 `MySQL=2, Doris=0` 是“瞬时快照差异”，不是最终一致性错误。
- 进入后续 DDL/MIX 阶段并继续消费后，sink 会追平，最终行数一致。

脚本中的 `WAIT_SYNC`、`wait_row_count_converged` 就是为这个“阶段性滞后”设计的。

### DML 生成原理

- 输入 schema 来自运行时 `DESC t0`，不是固定列模板。
- 按列类型生成值，确保语句在当前 schema 上可执行。
- `type=all` 时按权重混合 INSERT/REPLACE/UPDATE/DELETE。
- WHERE 支持单谓词和可选复合谓词（AND/OR、BETWEEN、IN、LIKE）。

### DDL 生成原理

- `alter_mixed` 模式下基于“当前列集合”状态机迭代。
- 每次随机选择 ADD COLUMN 或 DROP COLUMN（受 `drop_ratio` 控制）。
- `protected_cols`（默认 `c0`）不会被删除，避免主键被破坏。

### MIX（DDL + DML）原理

- 总轮次由 `MIXED_COUNT` 控制。
- 每轮按 `MIXED_DDL_RATIO` 决定执行 DDL 或 DML。
- 每条语句执行后记录到 `realtime_status.log`（含 phase、idx、kind、result、source/sink row count、stmt）。
- MIX 结束后做新增列同步检查和行数收敛等待。

### 本次已修复的问题

- 修复了整数范围错误：SMALLINT/TINYINT 现在按真实取值范围生成，避免 mixed DML 大量越界失败。
- 修复了重复加列问题：DDL ADD COLUMN 会避开已存在列名，降低重复加列失败。

### 建议的判定口径

- 阶段内快照（例如 Step 4）只用于观察延迟，不作为最终一致性结论。
- 最终以 Step 6 报告（行数 + schema 对齐）作为通过标准。

---

## ⭐ StarRocks Sink 适配说明（2026-03-24）

### 当前已支持能力

- e2e 脚本支持从 `pipeline-definition.yaml` 自动识别 sink 类型。
- 已验证 `sink.type=starrocks` 可完整跑通：建库建表、DML、DDL、MIX、最终快照与汇总。
- 脚本不再强依赖 Doris 容器名，已改为按 sink 类型动态选择服务名与端口。
- 新增独立 sink profile 层：`sink_profiles.sh`，后续新增 sink 只需维护该文件。

### 新增 sink 的最低改造路径

1. 在 `sink_profiles.sh` 中新增一个 profile 分支：
  - `SINK_SERVICE`（容器服务名）
  - `SINK_LABEL`（报告显示名）
  - `SINK_DB_PORT`（若支持 SQL 校验）
  - `SINK_SQL_ENABLED`（`1` 或 `0`）
2. 准备对应 `pipeline-definition-xxx.yaml`。
3. 使用 `PIPELINE_YAML=...` 运行 e2e。

### StarRocks 与 Doris 的关键差异

- Pipeline 配置差异：
  - Doris 用 `fenodes`。
  - StarRocks 用 `jdbc-url` + `load-url`。
- 表模型差异：
  - StarRocks CDC Sink 以 Primary Key 表为主，依赖主键保证幂等写入。
- 字符串长度映射：
  - `VARCHAR(256)` 在 StarRocks 侧常见为 `VARCHAR(768)`（UTF-8 字节长度映射）。

### StarRocks Pipeline 样例

```yaml
source:
  type: mysql
  hostname: mysql
  port: 3306
  username: root
  password: ''
  tables: "database0.\\.*"
  server-id: 5400-6400

sink:
  type: starrocks
  jdbc-url: jdbc:mysql://starrocks:9030
  load-url: starrocks:8080
  username: root
  password: ''
  table.create.properties.replication_num: 1
```

### 运行方式

```bash
PIPELINE_YAML=pipeline-definition.yaml \
WAIT_SYNC=5 WAIT_TABLE_TIMEOUT=240 \
DML_COUNT=20 DDL_COUNT=5 MIXED_COUNT=30 \
./run_sqlancer_cdc_e2e.sh
```

### 快速找 Bug 模式（低延迟）

当你主要目标是快速复现问题（而不是最严格稳定性验证）时，可开启：

```bash
FAST_MODE=1 PIPELINE_YAML=pipeline-definition.yaml ./run_sqlancer_cdc_e2e.sh
```

`FAST_MODE=1` 会自动压缩等待参数上限：

- `WAIT_SYNC` 最多 2s
- `WAIT_TABLE_TIMEOUT` 最多 90s
- `DDL_SYNC_TIMEOUT` 最多 45s
- mixed 后行数收敛重试轮次从 90 降到 30

### 本次实测结果摘要

- DML success/fail: `19/0`
- DDL success/fail: `4/0`
- Mixed DML success/fail: `23/0`
- Mixed DDL success/fail: `7/0`
- Row count(MySQL/StarRocks): `1/1`

报告路径示例：`/tmp/cdc_starrocks_e2e_try4/source_sink_final_state.txt`

**自动执行流程**:
1. 生成 SQL 语句
2. 在 MySQL 执行 DML
3. 等待 CDC 同步
4. 在两个数据库执行 SELECT 验证
5. 生成完整报告

---

## 🧪 完整测试场景

### 场景 A: DML 基础测试 (3 分钟)

验证 DML 生成和 CDC 同步是否正常工作。

```bash
#!/bin/bash
# 清空旧数据
mysql -h 127.0.0.1 -P 32774 -u root database0 \
  -e "DROP TABLE IF EXISTS t0; CREATE TABLE t0 (
    c0 INT PRIMARY KEY, c1 VARCHAR(50) NOT NULL, 
    c2 INT, c3 VARCHAR(50), c4 FLOAT
  );"

sleep 5

# 生成并执行 20 条 DML
python dml_generator.py --count 20 --seed 2024 --output-sql /tmp/test_dml.sql
mysql -h 127.0.0.1 -P 32774 -u root database0 < /tmp/test_dml.sql

# 等待 CDC 同步
sleep 15

# 验证行数一致
MYSQL_CNT=$(mysql -h 127.0.0.1 -P 32774 -u root database0 -sN -e "SELECT COUNT(*) FROM t0;")
DORIS_CNT=$(mysql -h 127.0.0.1 -P 32777 -u root database0 -sN -e "SELECT COUNT(*) FROM t0;")

echo "MySQL: $MYSQL_CNT 行, Doris: $DORIS_CNT 行"
if [ "$MYSQL_CNT" = "$DORIS_CNT" ]; then
  echo "✅ 测试通过"
else
  echo "❌ 测试失败"
fi
```

---

### 场景 B: DDL 演化测试 (5 分钟)

验证 ALTER TABLE 是否能正确同步。

```bash
#!/bin/bash
# 创建表并等待初始同步
mysql -h 127.0.0.1 -P 32774 -u root database0 \
  -e "DROP TABLE IF EXISTS t0; CREATE TABLE t0 (c0 INT PRIMARY KEY, c1 VARCHAR(50));"

sleep 10

# 执行 ALTER 操作
python ddl_generator.py --count 3 --type alter --seed 2024 --output-sql /tmp/alter.sql
mysql -h 127.0.0.1 -P 32774 -u root database0 < /tmp/alter.sql

# 等待 CDC 同步
sleep 15

# 比较列数
MYSQL_COLS=$(mysql -h 127.0.0.1 -P 32774 -u root database0 -e "DESC t0;" | wc -l)
DORIS_COLS=$(mysql -h 127.0.0.1 -P 32777 -u root database0 -e "DESC t0;" | wc -l)

if [ "$MYSQL_COLS" = "$DORIS_COLS" ]; then
  echo "✅ DDL 演化成功 ($MYSQL_COLS 列)"
else
  echo "❌ DDL 演化失败: MySQL=$MYSQL_COLS, Doris=$DORIS_COLS"
fi
```

---

### 场景 C: 完整端到端测试 (10 分钟)

执行完整的生成-执行-同步-验证流程。

```bash
#!/bin/bash
# 简单方式：使用自动化套件
bash sql_generator_suite.sh

# 或详细方式：
DML_COUNT=50 DDL_COUNT=5 SELECT_COUNT=15 bash sql_generator_suite.sh

# 检查结果
if [ -f /tmp/sqla_test_*/test_report.txt ]; then
  cat /tmp/sqla_test_*/test_report.txt
fi
```

---

### 场景 D: 性能基准测试 (15 分钟)

测试不同规模 DML 的 CDC 同步延迟。

```bash
#!/bin/bash
# 测试 10, 50, 100, 500, 1000 条 DML 的同步时间

for COUNT in 10 50 100 500 1000; do
  echo "=== 测试 $COUNT 条 DML ==="
  
  # 清空数据
  mysql -h 127.0.0.1 -P 32774 -u root database0 \
    -e "DROP TABLE IF EXISTS t0; CREATE TABLE t0 (
      c0 INT AUTO_INCREMENT PRIMARY KEY, c1 VARCHAR(50)
    );"
  sleep 5
  
  # 记录时间并执行 DML
  START=$(date +%s%N)
  python dml_generator.py --count "$COUNT" --seed 2024 --output-sql /tmp/perf.sql
  mysql -h 127.0.0.1 -P 32774 -u root database0 < /tmp/perf.sql
  
  # 等待同步完成
  while true; do
    MYSQL=$(mysql -h 127.0.0.1 -P 32774 -u root database0 -sN -e "SELECT COUNT(*) FROM t0;")
    DORIS=$(mysql -h 127.0.0.1 -P 32777 -u root database0 -sN -e "SELECT COUNT(*) FROM t0;" 2>/dev/null || echo 0)
    
    if [ "$MYSQL" = "$DORIS" ] && [ "$MYSQL" -eq "$COUNT" ]; then
      ELAPSED=$(($(date +%s%N) - START))
      echo "  ✓ 耗时: $(($ELAPSED / 1000000))ms"
      break
    fi
    sleep 1
  done
done
```

---

### 场景 E: 故障恢复测试 (20 分钟)

验证 Pipeline 重启后数据一致性。

```bash
#!/bin/bash
# 1. 生成初始数据
mysql -h 127.0.0.1 -P 32774 -u root database0 \
  -e "DROP TABLE IF EXISTS t0; CREATE TABLE t0 (c0 INT PK, c1 VARCHAR(50));"
sleep 5

python dml_generator.py --count 50 --seed 2024 --output-sql /tmp/init.sql
mysql -h 127.0.0.1 -P 32774 -u root database0 < /tmp/init.sql

sleep 15  # 等待初始同步

# 2. 重启 Pipeline
echo "正在重启 Pipeline..."
./cdcup.sh pipeline pipeline-definition.yaml restart || true
sleep 30

# 3. 执行增量操作
python dml_generator.py --count 25 --seed 3024 --output-sql /tmp/incr.sql
mysql -h 127.0.0.1 -P 32774 -u root database0 < /tmp/incr.sql

sleep 15

# 4. 验证一致性
MYSQL_CNT=$(mysql -h 127.0.0.1 -P 32774 -u root database0 -sN -e "SELECT COUNT(*) FROM t0;")
DORIS_CNT=$(mysql -h 127.0.0.1 -P 32777 -u root database0 -sN -e "SELECT COUNT(*) FROM t0;")

if [ "$MYSQL_CNT" = "$DORIS_CNT" ]; then
  echo "✅ 故障恢复成功: $MYSQL_CNT 行"
else
  echo "❌ 故障恢复失败: MySQL=$MYSQL_CNT, Doris=$DORIS_CNT"
fi
```

---

## 🔧 故障排查指南

### 问题 1: DML 执行失败（success=0）

**可能原因**:
- 表不存在
- 列类型不匹配
- 主键重复

**排查方法**:
```bash
# 检查表是否存在
mysql -h 127.0.0.1 -P 32774 -u root database0 -e "DESC t0;"

# 手动测试生成的 SQL
head -1 /tmp/dml_statements.sql
mysql -h 127.0.0.1 -P 32774 -u root database0 -e "INSERT INTO t0 (c0, c1) VALUES (1, 'test');"
```

**解决方案**:
```bash
# 重新创建表
mysql -h 127.0.0.1 -P 32774 -u root database0 \
  -e "DROP TABLE IF EXISTS t0; CREATE TABLE t0 (
    c0 INT PRIMARY KEY, c1 VARCHAR(50) NOT NULL, 
    c2 INT, c3 VARCHAR(50), c4 FLOAT
  );"
```

---

### 问题 2: SELECT 验证不匹配

**症状**: `SELECT 验证: 0 匹配, 10 不匹配`

**原因**: CDC 同步延迟或 DDL 未正确同步

**排查**:
```bash
# 检查行数是否与 MySQL 一致
mysql -h 127.0.0.1 -P 32774 -u root database0 -e "SELECT COUNT(*) FROM t0;"
mysql -h 127.0.0.1 -P 32777 -u root database0 -e "SELECT COUNT(*) FROM t0;"

# 检查 Doris 表结构
mysql -h 127.0.0.1 -P 32777 -u root database0 -e "DESC t0;"
```

**解决方案**:
```bash
# 方案 1：增加等待时间
WAIT_SYNC=30 bash sql_generator_suite.sh

# 方案 2：重启 Pipeline
./cdcup.sh pipeline pipeline-definition.yaml
```

---

### 问题 3: CDC 列缺失（DDL 未同步）

**症状**: Doris 中 `DESC t0` 列数 < MySQL 列数

**原因**: Pipeline 启动时未捕获完整表定义

**排查**:
```bash
# 对比表结构
echo "=== MySQL ===" && \
mysql -h 127.0.0.1 -P 32774 -u root database0 -e "DESC t0;"

echo "=== Doris ===" && \
mysql -h 127.0.0.1 -P 32777 -u root database0 -e "DESC t0;"
```

**解决方案**:
```bash
# 1. 清空表
mysql -h 127.0.0.1 -P 32774 -u root database0 -e "DROP TABLE IF EXISTS t0;"
mysql -h 127.0.0.1 -P 32777 -u root database0 -e "DROP TABLE IF EXISTS t0;"

sleep 5

# 2. 重启 Pipeline
./cdcup.sh pipeline pipeline-definition.yaml

sleep 10

# 3. 重新创建表
mysql -h 127.0.0.1 -P 32774 -u root database0 \
  -e "CREATE TABLE t0 (
    c0 INT PRIMARY KEY, c1 VARCHAR(50) NOT NULL, 
    c2 INT, c3 VARCHAR(50), c4 FLOAT
  );"
```

---

### 问题 4: 连接失败

**症状**: `ERROR 2003 (HY000): Can't connect to MySQL server`

**排查**:
```bash
# 检查容器是否运行
docker ps | grep -E "mysql|doris|flink"

# 测试端口连接
telnet 127.0.0.1 32774  # MySQL
telnet 127.0.0.1 32777  # Doris
```

**解决方案**:
```bash
# 重启容器
cd /home/wyh/flink-cdc
docker-compose restart

# 或完全重新启动
docker-compose down
docker-compose up -d
```

---

## 📋 参考信息

### 环境配置

| 项目 | 值 |
|------|-----|
| MySQL 地址 | 127.0.0.1:32774 |
| Doris SQL 地址 | 127.0.0.1:32777 |
| 数据库 | database0 |
| 测试表 | t0 |
| 用户名 | root |
| 密码 | (空) |

### 表结构

```sql
CREATE TABLE t0 (
  c0 INT PRIMARY KEY,          -- 主键
  c1 VARCHAR(50) NOT NULL,     -- 非空字符串
  c2 INT,                       -- 可空整数
  c3 VARCHAR(50),               -- 可空字符串
  c4 FLOAT                      -- 可空浮点
) ENGINE=InnoDB;
```

### 文件位置

```
/home/wyh/flink-cdc/tools/cdcup/
├── dml_generator.py                    # DML 生成器
├── ddl_generator.py                    # DDL 生成器
├── select_generator.py                 # SELECT 生成器
├── sql_generator_suite.sh              # 自动化测试套件
├── pipeline-definition.yaml            # CDC 配置
├── SQLANCER_DORIS_CDC_PLAYBOOK_V2.md  # 本文档
└── /tmp/sqla_test_*/                   # 测试输出
```

---

## ✅ 最佳实践

### 推荐做法

1. **总是使用固定种子做可重现测试**
   ```bash
   BASE_SEED=42 bash sql_generator_suite.sh
   ```

2. **执行大操作前清空数据**
   ```bash
   mysql -h 127.0.0.1 -P 32774 -u root database0 \
     -e "DROP TABLE IF EXISTS t0;" && sleep 5
   ```

3. **DDL 操作后等待充分时间**
   ```bash
   ALTER TABLE t0 ADD COLUMN ...;
   sleep 15  # 等待 CDC 同步
   SELECT * FROM t0;  # 然后才查询
   ```

### 避免做法

1. ❌ **立即执行DML后立即DROP表**
2. ❌ **在大数据量操作中途重启Pipeline**
3. ❌ **同时修改多个表** (当前只支持 t0)

---

## 📞 常见问题

**Q: 如何确保语句可重现？**  
A: 使用相同的 `--seed` 参数运行两次，应该得到完全相同的输出。

**Q: 为什么 Doris 表比 MySQL 表列数少？**  
A: Pipeline 需要重启来重新捕获完整的 DDL 历史。按照"问题 3"的解决方案。

**Q: DML 执行成功但 SELECT 仍不匹配，怎么办？**  
A: 增加 CDC 同步等待时间：`WAIT_SYNC=30 bash sql_generator_suite.sh`

**Q: 如何修改表的列结构？**  
A: 直接在 MySQL 修改，然后重启 Pipeline：
```bash
mysql -h 127.0.0.1 -P 32774 -u root database0 \
  -e "ALTER TABLE t0 ADD COLUMN c5 VARCHAR(100);"

sleep 10

./cdcup.sh pipeline pipeline-definition.yaml restart
```

---

**文档版本**: 1.3  
**最后更新**: 2026-03-23
