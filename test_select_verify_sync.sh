#!/bin/bash
##############################################################################
# SELECT 验证脚本 - 检查 CDC 同步是否正常
# 
# 流程：
#   1. 生成 DML 语句并在 source 执行
#   2. 等待 CDC 同步
#   3. 生成 SELECT 查询
#   4. 对比 source vs sink 的 SELECT 结果
##############################################################################

# 不设置 set -e，以便更好地处理错误

# 配置
DML_COUNT=${DML_COUNT:-5}
DML_SEED=${DML_SEED:-$(date +%s%N | cut -c1-10)}
DATABASE=${DATABASE:-database0}
WAIT_SYNC=${WAIT_SYNC:-15}

# Docker 端口自动检测
MYSQL_PORT=$(docker port cdcup-mysql-1 3306/tcp 2>/dev/null | awk -F: '{print $2}')
DORIS_PORT=$(docker port cdcup-doris-1 9030/tcp 2>/dev/null | awk -F: '{print $2}')

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

echo "[INFO] ================================"
echo "[INFO] SELECT 验证测试"
echo "[INFO] ================================"
echo "[INFO] DML Count: $DML_COUNT"
echo "[INFO] DML Seed: $DML_SEED"
echo "[INFO] MySQL Port: $MYSQL_PORT"
echo "[INFO] Doris Port: $DORIS_PORT"
echo ""

# ========== Step 1: 清理并创建表 ==========
echo "[1/5] 初始化数据库..."

# 先尝试删除表（不删除数据库）
mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --password='' "$DATABASE" \
    -e "DROP TABLE IF EXISTS t0;" 2>/dev/null || true

# 创建表
mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --password='' <<'EOF' 2>&1 | grep -v Warning || true
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
-- 插入初始数据
INSERT INTO t0 (c0, c1, c2) VALUES (0, 'init_row', 0);
EOF

sleep 3
echo "[INFO] 表已创建"

# ========== Step 2: 生成和执行 DML ==========
echo "[2/5] 生成和执行 $DML_COUNT 条 DML 语句..."

DML_SQL="/tmp/select_verify_dml_${DML_SEED}.sql"
python "$SCRIPT_DIR/generators/dml_generator.py" --count "$DML_COUNT" --seed "$DML_SEED" --output-sql "$DML_SQL"

EXEC_SUCCESS=0
EXEC_FAILED=0

while IFS= read -r stmt; do
    [[ -z "$stmt" || "$stmt" =~ ^-- ]] && continue
    if mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --password='' "$DATABASE" \
        -e "$stmt" >/dev/null 2>&1; then
        ((EXEC_SUCCESS++))
    else
        ((EXEC_FAILED++))
    fi
done < "$DML_SQL"

echo "[INFO] DML 执行完成: success=$EXEC_SUCCESS, failed=$EXEC_FAILED"

# ========== Step 3: 等待 CDC 同步 ==========
echo "[3/5] 等待 CDC 同步 ($WAIT_SYNC 秒)..."
sleep "$WAIT_SYNC"

# ========== Step 4: 生成 SELECT 查询 ==========
echo "[4/5] 生成验证查询..."

SELECT_QUERIES="/tmp/select_verify_queries_${DML_SEED}.sql"
python "$SCRIPT_DIR/generators/select_generator.py" --count 5 --seed "$DML_SEED" --output-sql "$SELECT_QUERIES"

# ========== Step 5: 执行 SELECT 并比较结果 ==========
echo "[5/5] 对比 source vs sink 的 SELECT 结果..."
echo ""

MATCH_COUNT=0
MISMATCH_COUNT=0

# 执行每条查询
QUERY_NUM=1
while IFS= read -r query; do
    [[ -z "$query" || "$query" =~ ^-- ]] && continue
    
    # 在 source (MySQL) 执行
    SOURCE_RESULT=$(mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --password='' "$DATABASE" \
        -e "$query" --batch --skip-column-names 2>/dev/null || echo "ERROR")
    
    # 在 sink (Doris) 执行
    SINK_RESULT=$(mysql -h 127.0.0.1 -P "$DORIS_PORT" -u root --password='' "$DATABASE" \
        -e "$query" --batch --skip-column-names 2>/dev/null || echo "ERROR")
    
    # 比较
    if [ "$SOURCE_RESULT" = "$SINK_RESULT" ]; then
        echo "[✓] Query $QUERY_NUM: MATCH"
        ((MATCH_COUNT++))
    else
        echo "[✗] Query $QUERY_NUM: MISMATCH"
        echo "    Query: $query"
        echo "    Source: $SOURCE_RESULT"
        echo "    Sink:   $SINK_RESULT"
        ((MISMATCH_COUNT++))
    fi
    
    ((QUERY_NUM++))
done < "$SELECT_QUERIES"

echo ""
echo "[INFO] ================================"
echo "[INFO] 验证完成"
echo "[INFO] ================================"
echo "[INFO] 匹配结果: $MATCH_COUNT 个匹配, $MISMATCH_COUNT 个不匹配"
echo "[INFO]"

if [ $MISMATCH_COUNT -eq 0 ]; then
    echo "[✓] CDC 同步正常！"
    exit 0
else
    echo "[✗] CDC 同步有问题，需要更长的等待时间或检查管道配置"
    exit 1
fi
