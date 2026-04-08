# FLOOR(c4) — 问题报告

**环境**
- **Flink CDC（pipeline 连接器）**: flink-cdc-pipeline-connector-mysql:3.2.1（source），flink-cdc-pipeline-connector-doris:3.2.1（sink）
- **Flink 运行时（镜像）**: flink:1.20.3-scala_2.12
- **MySQL（镜像）**: mysql:8.0
- **Doris（镜像）**: apache/doris:doris-all-in-one-2.1.0
- **JDBC / 其它依赖**: mysql-connector-java:8.0.27，flink-shaded-hadoop-2-uber:2.8.3-10.0
- **测试运行器**: /home/wyh/flink-cdc/tools/cdcup/sqlancer_cdc 中的 `run_sqlancer_cdc_e2e.sh`

**执行脚本**
- **单次重跑（示例）**:

```bash
cd /home/wyh/flink-cdc/tools/cdcup/sqlancer_cdc && \
./run_sqlancer_cdc_e2e.sh --pipeline-yaml pipeline-definition-doris.yaml \
  --sink-type doris --rounds 1 --base-seed 50909 --wait-sync 1 \
  --dml-count 6 --ddl-count 2 --mixed-count 6 \
  --transform-source-table "database0.t0" \
  --transform-projection "c0, FLOOR(c4) as c4_floor" \
  --transform-projection-mode expand-all \
  --report-dir /tmp/cdc_case9_rerun
```

- **对比跑（相同 seed 的 FLOOR 与 ROUND+CAST）**:

```bash
# FLOOR run
cd /home/wyh/flink-cdc/tools/cdcup/sqlancer_cdc && \
./run_sqlancer_cdc_e2e.sh --pipeline-yaml pipeline-definition-doris.yaml \
  --sink-type doris --rounds 1 --base-seed 88001 --wait-sync 1 \
  --dml-count 12 --ddl-count 2 --mixed-count 6 \
  --transform-source-table "database0.t0" \
  --transform-projection "c0, c4, FLOOR(c4) as c4_floor" \
  --transform-projection-mode expand-all \
  --report-dir /tmp/cdc_cmp_floor

# ROUND+CAST run（同 seed）
cd /home/wyh/flink-cdc/tools/cdcup/sqlancer_cdc && \
./run_sqlancer_cdc_e2e.sh --pipeline-yaml pipeline-definition-doris.yaml \
  --sink-type doris --rounds 1 --base-seed 88001 --wait-sync 1 \
  --dml-count 12 --ddl-count 2 --mixed-count 6 \
  --transform-source-table "database0.t0" \
  --transform-projection "c0, c4, CAST(ROUND(c4,0) AS INT) as c4_int_probe" \
  --transform-projection-mode expand-all \
  --report-dir /tmp/cdc_cmp_round_cast
```

**问题描述**
- **症状**: 在多次独立重跑中，`FLOOR(c4) as c4_floor` 的预言行未出现在 sink（summary: `pass=0, fail=3`），但没有看到与 CAST 场景相同的 `NumberFormatException`。

**预期结果**
- **预期**: sink 表中 `c4_floor` 应为 `FLOOR(c4)` 的数值（整数）且 transform prophecy `pass=3`。

**实际结果**
- **FLOOR**: 多次重跑均 `pass=0, fail=3`，预言行缺失。
- **对比**: 使用相同 seed 的 `CAST(ROUND(c4,0) AS INT)` 同次运行 `pass=3, fail=0`（说明相同输入在另一表达式下能正确到达 sink）。

**初步分析**
- **可能性一（类型/序列化）**: `FLOOR` 返回的数值类型在 transform/sink 链路中被错误处理或序列化，导致行被丢弃或写入失败。
- **可能性二（表达式评估）**: 表达式在运行时产生 `null` 或被优化掉，导致预言行不满足匹配条件。
- **证据差异**: FLOOR 场景缺少明显异常，暗示是“静默丢弃”或兼容性问题，而非立即抛出可见的运行时异常。

**短期缓解 & 建议**
- 使用 `CAST(ROUND(c4,0) AS INT)` 作为替代（已验证通过）。
- 收集最小复现（单行）并抓取 Flink TaskManager/JobManager 日志以查找被吞掉的错误或 WARN。检查 sink（Doris）端是否有被拒绝或部分写入的记录。
- 若最小复现可复现，准备上游 bug 报告并附上 pipeline yaml、最小输入、以及 job logs。

**证据摘录（概览）**
```
Transform prophecy probe FAILED: expected row c0=... to appear in sink, but not found
Transform prophecy probe summary: pass=0, fail=3

# 对比运行（同 seed）
ROUND+CAST run -> Transform prophecy probe summary: pass=3, fail=0
```
