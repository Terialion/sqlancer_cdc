# CAST 直接转 INT — 问题报告

**环境**
- **Flink CDC（pipeline 连接器）**: flink-cdc-pipeline-connector-mysql:3.2.1（source），flink-cdc-pipeline-connector-doris:3.2.1（sink）
- **Flink 运行时（镜像）**: flink:1.20.3-scala_2.12
- **MySQL（镜像）**: mysql:8.0
- **Doris（镜像）**: apache/doris:doris-all-in-one-2.1.0
- **JDBC / 其它依赖**: mysql-connector-java:8.0.27，flink-shaded-hadoop-2-uber:2.8.3-10.0
- **测试运行器**: /home/wyh/flink-cdc/tools/cdcup/sqlancer_cdc 中的 `run_sqlancer_cdc_e2e.sh`

**执行脚本**
- **安全写法（通过）**:

```bash
cd /home/wyh/flink-cdc/tools/cdcup/sqlancer_cdc && \
./run_sqlancer_cdc_e2e.sh --pipeline-yaml pipeline-definition-doris.yaml \
  --sink-type doris --rounds 1 --base-seed 43 --wait-sync 2 \
  --dml-count 6 --ddl-count 2 --mixed-count 6 \
  --transform-source-table "database0.t0" \
  --transform-projection "c0, CAST(ROUND(c4,0) AS INT) as c4_int_probe" \
  --transform-projection-mode expand-all \
  --report-dir /tmp/cdc_doc_cast_int_safe
```

- **直接 CAST（失败示例）**:

```bash
cd /home/wyh/flink-cdc/tools/cdcup/sqlancer_cdc && \
./run_sqlancer_cdc_e2e.sh --pipeline-yaml pipeline-definition-doris.yaml \
  --sink-type doris --rounds 1 --base-seed 43 --wait-sync 2 \
  --dml-count 6 --ddl-count 2 --mixed-count 6 \
  --transform-source-table "database0.t0" \
  --transform-projection "c0, CAST(c4 AS INT) as c4_int_probe" \
  --transform-projection-mode expand-all \
  --report-dir /tmp/cdc_doc_cast_int_fail
```

**问题描述**
- **症状**: 对含小数的列 `c4` 直接使用 `CAST(c4 AS INT)` 时，transform 在执行时抛出异常，预言行未到达 sink；替代写法 `CAST(ROUND(c4,0) AS INT)` 正常通过。
- **关键日志**: 出现 `java.lang.NumberFormatException: For input string: "161.91"`，堆栈指向 `SystemFunctionUtils.castToInteger()`。

**预期结果**
- **预期**: sink 表中 `c4_int_probe` 列应为整数（根据表达式），transform prophecy 检查应 `pass=3`。

**实际结果**
- **直接 CAST**: `CAST(c4 AS INT)` → `pass=0, fail=3`，并伴随 `NumberFormatException`（示例值 `"161.91"`）。
- **ROUND + CAST**: `CAST(ROUND(c4, 0) AS INT)` → `pass=3, fail=0`，sink 中可见预期整数值（例如 `161.91` -> `162`）。

**初步分析**
- **实现缺陷**: 语法合法，但当前实现中 `SystemFunctionUtils.castToInteger()` 对浮点字符串直接使用整数解析，导致 `NumberFormatException`。更像是实现层未处理带小数字符串的窄化转换。
- **短期缓解**: 在探针中使用 `ROUND` 后再 `CAST`，或直接使用 `DECIMAL` 类型探针以保证稳定。
- **后续建议**: 准备最小复现（1~3 行），收集 Flink job logs（TaskManager / JobManager）并把 pipeline yaml、job 日志与最小输入一并作为上游 bug 报告附件。

**证据摘录**
```
Transform prophecy probe FAILED: expected row c0=100006221 to appear in sink, but not found
Transform prophecy probe summary: pass=0, fail=3

java.lang.NumberFormatException: For input string: "161.91"
at org.apache.flink.cdc.runtime.functions.SystemFunctionUtils.castToInteger(SystemFunctionUtils.java:643)
```
