#!/bin/bash
##############################################################################
# CDC DML 集成测试脚本 (CDC DML Integration Test Script)
# 
# 流程：
#   1. 清理旧数据
#   2. 创建DDL prelude（表结构）
#   3. 随机生成DML语句
#   4. 在source执行DML
#   5. 等待CDC同步到sink
#   6. 比较source vs sink数据一致性
#   7. 生成测试报告
##############################################################################

# 移除 'set -e' 以更好地处理中间错误
# set -e

# ============ 配置 ============
DML_COUNT=${DML_COUNT:-20}          # 生成的DML语句数
DML_SEED=${DML_SEED:-$(date +%s)}   # 随机种子 (可重现)
DATABASE=${DATABASE:-database0}
WAIT_SYNC=${WAIT_SYNC:-10}          # CDC同步等待时间(秒)
OUTPUT_DIR=${OUTPUT_DIR:-/tmp}

# Docker 端口自动检测
MYSQL_PORT=$(docker port cdcup-mysql-1 3306/tcp | awk -F: '{print $2}')
DORIS_PORT=$(docker port cdcup-doris-1 9030/tcp | awk -F: '{print $2}')

echo "[INFO] ================================"
echo "[INFO] CDC DML Integration Test"
echo "[INFO] ================================"
echo "[INFO] DML Count: $DML_COUNT"
echo "[INFO] DML Seed: $DML_SEED"
echo "[INFO] Database: $DATABASE"
echo "[INFO] MySQL Port: $MYSQL_PORT"
echo "[INFO] Doris Port: $DORIS_PORT"
echo "[INFO]"

# ============ Step 1: 清理旧数据 ============
echo "[1/6] 清理旧测试数据..."
mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --password='' -e "DROP DATABASE IF EXISTS $DATABASE;" 2>/dev/null || true
sleep 2

# ============ Step 2: 创建 DDL prelude ============
echo "[2/6] 创建表结构 (DDL prelude)..."
PRELUDE_SQL=$(cat <<'EOF'
CREATE DATABASE IF NOT EXISTS database0;
USE database0;

DROP TABLE IF EXISTS t0;
CREATE TABLE t0 (
    c0 INT PRIMARY KEY,
    c1 VARCHAR(256) NOT NULL,
    c2 INT,
    c3 VARCHAR(256),
    c4 FLOAT
);

-- 可选：插入初始数据
INSERT INTO t0 (c0, c1, c2) VALUES (0, 'init', 0);
EOF
)

echo "$PRELUDE_SQL" | mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --password='' 2>&1 | grep -v "Warning" || true
sleep 2

# ============ Step 3: 生成 DML 语句 ============
echo "[3/6] 生成 $DML_COUNT 条 DML 语句 (seed=$DML_SEED)..."

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DML_SQL_FILE="$OUTPUT_DIR/dml_statements_seed_${DML_SEED}.sql"

python "$SCRIPT_DIR/generators/dml_generator.py" \
    --count "$DML_COUNT" \
    --seed "$DML_SEED" \
    --output-sql "$DML_SQL_FILE"

echo "[INFO] DML 语句保存到: $DML_SQL_FILE"
echo "[INFO] DML 语句预览:"
head -5 "$DML_SQL_FILE" | sed 's/^/  /'
echo "  ..."

# ============ Step 4: 在 Source 执行 DML ============
echo "[4/6] 在 MySQL source 执行 DML 语句..."

EXEC_START_TIME=$(date +%s)
EXEC_SUCCESS=0
EXEC_FAILED=0

while IFS= read -r stmt; do
    # 跳过空行和注释
    [[ -z "$stmt" || "$stmt" =~ ^-- ]] && continue
    
    # 执行语句
    if mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --password='' "$DATABASE" \
        -e "$stmt" >/dev/null 2>&1; then
        ((EXEC_SUCCESS++))
    else
        ((EXEC_FAILED++))
        echo "  [WARN] Failed: $stmt"
    fi
done < "$DML_SQL_FILE"

EXEC_END_TIME=$(date +%s)
EXEC_TIME=$((EXEC_END_TIME - EXEC_START_TIME))

echo "[INFO] DML 执行完成: success=$EXEC_SUCCESS, failed=$EXEC_FAILED, time=${EXEC_TIME}s"

# ============ Step 5: 等待 CDC 同步 ============
echo "[5/6] 等待 CDC 同步 ($WAIT_SYNC 秒)..."
sleep "$WAIT_SYNC"

# ============ Step 6: 生成预期结果 & 比较 ============
echo "[6/6] 生成预期结果并比较..."

EXPECTED_CSV="$OUTPUT_DIR/dml_expected_seed_${DML_SEED}.csv"
COMPARISON_LOG="$OUTPUT_DIR/dml_comparison_seed_${DML_SEED}.log"

python "$SCRIPT_DIR/sqlancer_dml_expected_generator.py" \
    --input-sql "$DML_SQL_FILE" \
    --database "$DATABASE" \
    --output-expected-csv "$EXPECTED_CSV" \
    --source-port "$MYSQL_PORT" \
    --source-password '' \
    2>&1 | tee "$COMPARISON_LOG"

# ============ 生成报告 ============
echo ""
echo "[INFO] ================================"
echo "[INFO] 测试已完成"
echo "[INFO] ================================"
echo "[INFO] DML 语句: $DML_SQL_FILE"
echo "[INFO] 预期结果: $EXPECTED_CSV"
echo "[INFO] 比较日志: $COMPARISON_LOG"
echo "[INFO]"
echo "[INFO] 下一步："
echo "[INFO]   1. 检查预期CSV: cat $EXPECTED_CSV"
echo "[INFO]   2. 运行一致性验证: python sqlancer_dml_expected_generator.py --apply-verify"
echo "[INFO]"

# 最终检查：比较行数
MYSQL_COUNT=$(mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --password='' "$DATABASE" \
    -e "SELECT COUNT(*) FROM t0;" --skip-column-names)
DORIS_COUNT=$(mysql -h 127.0.0.1 -P "$DORIS_PORT" -u root --password='' "$DATABASE" \
    -e "SELECT COUNT(*) FROM t0;" --skip-column-names 2>/dev/null || echo "ERROR")

echo "[INFO] 数据行数对比:"
echo "[INFO]   MySQL: $MYSQL_COUNT rows"
echo "[INFO]   Doris: $DORIS_COUNT rows"

if [ "$MYSQL_COUNT" = "$DORIS_COUNT" ]; then
    echo "[INFO] ✓ 行数一致"
else
    echo "[WARN] ✗ 行数不一致"
fi
