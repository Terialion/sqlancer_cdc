#!/bin/bash
##############################################################################
# SQL 生成器完整自动化套件 (Complete SQL Generator Suite)
#
# 功能：一键执行 DML 生成 → 数据插入 → CDC 同步 → SELECT 验证 → 完整测试报告
##############################################################################

set -e
set -o pipefail

# ============ 配置 ============
DML_COUNT=${DML_COUNT:-20}
DDL_COUNT=${DDL_COUNT:-5}
SELECT_COUNT=${SELECT_COUNT:-10}
BASE_SEED=${BASE_SEED:-$(date +%s | tail -c 5)}
DATABASE=${DATABASE:-database0}
WAIT_SYNC=${WAIT_SYNC:-15}
OUTPUT_DIR=${OUTPUT_DIR:-/tmp/sqla_test_${BASE_SEED}}

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

resolve_compose_port() {
    local service="$1"
    local container_port="$2"
    local port_info
    port_info=$(docker compose port "$service" "$container_port" 2>/dev/null || true)
    if [ -n "$port_info" ]; then
        echo "$port_info" | awk -F: '{print $NF}' | tail -n1
    fi
}

MYSQL_PORT=${MYSQL_PORT:-$(resolve_compose_port mysql 3306)}
DORIS_PORT=${DORIS_PORT:-$(resolve_compose_port doris 9030)}

if [ -z "$MYSQL_PORT" ] || [ -z "$DORIS_PORT" ]; then
    echo "[ERROR] Unable to resolve MySQL/Doris ports from docker compose." >&2
    echo "        Please ensure containers are up or set MYSQL_PORT/DORIS_PORT manually." >&2
    exit 1
fi

wait_for_mysql() {
    local host="$1"
    local port="$2"
    local retries="${3:-20}"
    local i
    for i in $(seq 1 "$retries"); do
        if mysql -h "$host" -P "$port" -u root --password='' -e "SELECT 1" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# 优先使用项目虚拟环境，其次使用 python3
if [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
    PYTHON_BIN="python3"
else
    echo "[ERROR] Python runtime not found. Please install python3 or create .venv." >&2
    exit 1
fi

if ! wait_for_mysql 127.0.0.1 "$MYSQL_PORT" 30; then
    echo "[ERROR] MySQL is not ready on 127.0.0.1:$MYSQL_PORT" >&2
    exit 1
fi

if ! wait_for_mysql 127.0.0.1 "$DORIS_PORT" 30; then
    echo "[ERROR] Doris SQL endpoint is not ready on 127.0.0.1:$DORIS_PORT" >&2
    exit 1
fi

# ============ 初始化报告 ============
mkdir -p "$OUTPUT_DIR"
REPORT="$OUTPUT_DIR/test_report.txt"

{
    echo "╔════════════════════════════════════════════════════════╗"
    echo "║  SQL 生成器自动化测试套件                               ║"
    echo "╚════════════════════════════════════════════════════════╝"
    echo ""
    echo "开始时间: $(date)"
    echo "测试 ID: $BASE_SEED"
    echo "输出目录: $OUTPUT_DIR"
    echo "MySQL 端口: $MYSQL_PORT"
    echo "Doris 端口: $DORIS_PORT"
    echo ""
} | tee "$REPORT"

# ============ Step 1: 生成 SQL ============
{
    echo "[1/5] 生成 SQL 语句..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
} | tee -a "$REPORT"

"$PYTHON_BIN" "$SCRIPT_DIR/dml_generator.py" \
    --count "$DML_COUNT" --seed "$BASE_SEED" \
    --output-sql "$OUTPUT_DIR/dml_statements.sql" \
    2>&1 | grep "\[INFO\]" | tee -a "$REPORT"

"$PYTHON_BIN" "$SCRIPT_DIR/ddl_generator.py" \
    --count "$DDL_COUNT" --seed "$((BASE_SEED + 1))" \
    --output-sql "$OUTPUT_DIR/ddl_statements.sql" \
    2>&1 | grep "\[INFO\]" | tee -a "$REPORT"

"$PYTHON_BIN" "$SCRIPT_DIR/select_generator.py" \
    --count "$SELECT_COUNT" --seed "$((BASE_SEED + 2))" \
    --output-sql "$OUTPUT_DIR/select_statements.sql" \
    2>&1 | grep "\[INFO\]" | tee -a "$REPORT"

{
    echo "✓ DML 语句: $DML_COUNT 条"
    echo "✓ DDL 语句: $DDL_COUNT 条"
    echo "✓ SELECT 语句: $SELECT_COUNT 条"
    echo ""
} | tee -a "$REPORT"

# ============ Step 2: 执行 DML ============
{
    echo "[2/5] 在 MySQL 中执行 DML..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
} | tee -a "$REPORT"

DML_SUCCESS=0
DML_FAILED=0

while IFS= read -r stmt; do
    [[ -z "$stmt" || "$stmt" =~ ^-- ]] && continue
    if mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --password='' "$DATABASE" \
        -e "$stmt" >/dev/null 2>&1; then
        ((DML_SUCCESS+=1))
    else
        ((DML_FAILED+=1))
    fi
done < "$OUTPUT_DIR/dml_statements.sql"

{
    echo "✓ DML 执行: success=$DML_SUCCESS, failed=$DML_FAILED"
    echo ""
} | tee -a "$REPORT"

# ============ Step 3: 等待 CDC 同步 ============
{
    echo "[3/5] 等待 CDC 同步 (${WAIT_SYNC}s)..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
} | tee -a "$REPORT"

sleep "$WAIT_SYNC"
echo "✓ 同步完成" | tee -a "$REPORT" && echo "" | tee -a "$REPORT"

# ============ Step 4: 执行 SELECT 验证 ============
{
    echo "[4/5] 执行 SELECT 验证..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
} | tee -a "$REPORT"

MATCH_COUNT=0
MISMATCH_COUNT=0

while IFS= read -r query; do
    [[ -z "$query" || "$query" =~ ^-- ]] && continue
    
    SOURCE=$(mysql -h 127.0.0.1 -P "$MYSQL_PORT" -u root --password='' "$DATABASE" \
        -e "$query" --batch --skip-column-names 2>/dev/null || echo "ERROR")
    
    SINK=$(mysql -h 127.0.0.1 -P "$DORIS_PORT" -u root --password='' "$DATABASE" \
        -e "$query" --batch --skip-column-names 2>/dev/null || echo "ERROR")
    
    if [ "$SOURCE" = "$SINK" ]; then
        ((MATCH_COUNT+=1))
    else
        ((MISMATCH_COUNT+=1))
        echo "  [MISMATCH] $query" | tee -a "$REPORT"
    fi
done < "$OUTPUT_DIR/select_statements.sql"

{
    echo "✓ SELECT 验证: $MATCH_COUNT 匹配, $MISMATCH_COUNT 不匹配"
    echo ""
} | tee -a "$REPORT"

# ============ Step 5: 生成报告 ============
{
    echo "[5/5] 生成测试报告..."
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    echo "📊 测试结果总结"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""
    
    if [ "$MISMATCH_COUNT" -eq 0 ]; then
        echo "✅ 测试通过！"
        echo ""
        echo "   DML 执行: $DML_SUCCESS/$((DML_SUCCESS + DML_FAILED)) 成功"
        echo "   SELECT 验证: $MATCH_COUNT/$MATCH_COUNT 全部匹配"
        echo "   CDC 同步: 正常工作"
        echo ""
        echo "📁 测试文件:"
        echo "   - DML SQL: $OUTPUT_DIR/dml_statements.sql"
        echo "   - DDL SQL: $OUTPUT_DIR/ddl_statements.sql"
        echo "   - SELECT SQL: $OUTPUT_DIR/select_statements.sql"
        echo "   - 完整报告: $REPORT"
    else
        echo "⚠️  测试部分失败"
        echo ""
        echo "   DML 执行: $DML_SUCCESS/$((DML_SUCCESS + DML_FAILED)) 成功"
        echo "   SELECT 验证: $MATCH_COUNT 匹配, $MISMATCH_COUNT 不匹配"
        echo "   CDC 同步: 可能需要调查"
        echo ""
        echo "📁 报告位置: $REPORT"
    fi
    
    echo ""
    echo "结束时间: $(date)"
} | tee -a "$REPORT"

# ============ 返回状态 ============
if [ "$MISMATCH_COUNT" -eq 0 ]; then
    exit 0
else
    exit 1
fi
