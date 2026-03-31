#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-cdcup}"
export COMPOSE_PROJECT_NAME

DATABASE="${DATABASE:-database0}"
TABLE="${TABLE:-t0_pqs_pivot}"
WAIT_SECONDS="${WAIT_SECONDS:-1}"
WAIT_RETRIES="${WAIT_RETRIES:-30}"
MYSQL_READY_RETRIES="${MYSQL_READY_RETRIES:-45}"
RUNTIME_YAML="${RUNTIME_YAML:-/tmp/pqs_pivot_pipeline.yaml}"
PIPELINE_LOG="${PIPELINE_LOG:-/tmp/pqs_pivot_pipeline_submit.log}"
CLEAN_RESTART="${CLEAN_RESTART:-0}"

resolve_port() {
  local service="$1"
  local cport="$2"
  local out
  out=$(docker compose port "${service}" "${cport}" 2>/dev/null || true)
  [[ -z "${out}" ]] && return 1
  echo "${out}" | awk -F: '{print $NF}' | tail -n1
}

wait_mysql_ready() {
  local host="$1"
  local port="$2"
  local retries="${3:-60}"
  local i
  for i in $(seq 1 "${retries}"); do
    if mysql -h "${host}" -P "${port}" -u root -e "SELECT 1" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

count_sink_row() {
  local key="$1"
  local out
  out=$(mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE} WHERE c0=${key};" 2>/dev/null || true)
  if [[ -z "${out}" ]]; then
    # Table may not be materialized yet when no rows match transform filter.
    echo "0"
    return 0
  fi
  echo "${out}"
}

wait_sink_row_count() {
  local key="$1"
  local expected="$2"
  local retries="${3:-30}"
  local i
  local c
  for i in $(seq 1 "${retries}"); do
    c=$(count_sink_row "${key}")
    if [[ "${c}" == "${expected}" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

source_row_values() {
  local key="$1"
  mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT c0, c1, c2 FROM ${TABLE} WHERE c0=${key};" 2>/dev/null || echo ""
}

sink_row_values() {
  local key="$1"
  mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -sN -e "SELECT c0, c1, c2 FROM ${TABLE} WHERE c0=${key};" 2>/dev/null || echo ""
}

echo "[PQS-PIVOT] Ensure containers are up"
if [[ "${CLEAN_RESTART}" == "1" ]]; then
  echo "[PQS-PIVOT] Clean restart containers"
  (cd "${SCRIPT_DIR}" && ./cdcup.sh down >/dev/null 2>&1 || true)
fi
(cd "${SCRIPT_DIR}" && ./cdcup.sh up >/dev/null 2>&1)

MYSQL_PORT="$(resolve_port mysql 3306)"
SINK_PORT="$(resolve_port doris 9030)"
if [[ -z "${MYSQL_PORT}" || -z "${SINK_PORT}" ]]; then
  echo "ERROR: failed to resolve mysql/doris ports"
  exit 1
fi

wait_mysql_ready 127.0.0.1 "${MYSQL_PORT}" "${MYSQL_READY_RETRIES}"
wait_mysql_ready 127.0.0.1 "${SINK_PORT}" "${MYSQL_READY_RETRIES}"

echo "[PQS-PIVOT] Prepare source/sink tables"
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root -e "DROP DATABASE IF EXISTS ${DATABASE}; CREATE DATABASE ${DATABASE};"
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "
CREATE TABLE ${TABLE} (
  c0 INT PRIMARY KEY,
  c1 VARCHAR(64),
  c2 INT
);
INSERT INTO ${TABLE}(c0,c1,c2) VALUES (1,'seed',-1);
"

mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root -e "CREATE DATABASE IF NOT EXISTS ${DATABASE};" >/dev/null 2>&1 || true
mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -e "DROP TABLE IF EXISTS ${TABLE};" >/dev/null 2>&1 || true

cat > "${RUNTIME_YAML}" <<EOF
pipeline:
  parallelism: 1
source:
  type: mysql
  hostname: mysql
  port: 3306
  username: root
  password: ''
  tables: "${DATABASE}.${TABLE}"
  server-id: 5400-6400
  server-time-zone: UTC
transform:
  - source-table: ${DATABASE}.${TABLE}
    projection: c0, c1, c2
    filter: c2 > 0
sink:
  type: doris
  fenodes: doris:8030
  benodes: doris:8040
  jdbc-url: jdbc:mysql://doris:9030
  username: root
  password: ''
  sink.ignore.update-before: false
  table.create.properties.light_schema_change: true
  table.create.properties.replication_num: 1
EOF

echo "[PQS-PIVOT] Submit filtered transform pipeline"
set +e
(cd "${SCRIPT_DIR}" && ./cdcup.sh pipeline "${RUNTIME_YAML}") >"${PIPELINE_LOG}" 2>&1
RC=$?
set -e
if [[ ${RC} -ne 0 ]]; then
  echo "ERROR: pipeline submit failed"
  tail -n 80 "${PIPELINE_LOG}" || true
  exit 1
fi

sleep "$((WAIT_SECONDS + 6))"

# Phase A: row starts as c2=-1, should be absent in sink due to filter.
if ! wait_sink_row_count 1 0 "${WAIT_RETRIES}"; then
  echo "ERROR: expected initial sink row count=0 (absent), got $(count_sink_row 1)"
  exit 1
fi

echo "[PQS-PIVOT] A->B: update source row to satisfy filter (c2=10), expect sink appear"
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "UPDATE ${TABLE} SET c1='pivot_on', c2=10 WHERE c0=1;"
if ! wait_sink_row_count 1 1 "${WAIT_RETRIES}"; then
  echo "ERROR: expected sink row count=1 after c2=10, got $(count_sink_row 1)"
  exit 1
fi

echo "[PQS-PIVOT] CORE PASS: transform filter supports pivot row absent->present in sink"

SRC_ROW=$(source_row_values 1)
SINK_ROW=$(sink_row_values 1)
if [[ -z "${SRC_ROW}" || -z "${SINK_ROW}" ]]; then
  echo "ERROR: value-level comparison failed due to empty row snapshot. src='${SRC_ROW}' sink='${SINK_ROW}'"
  exit 1
fi
if [[ "${SRC_ROW}" != "${SINK_ROW}" ]]; then
  echo "ERROR: value-level mismatch after pivot present."
  echo "source='${SRC_ROW}'"
  echo "sink='${SINK_ROW}'"
  exit 1
fi

echo "[PQS-PIVOT] VALUE PASS: source/sink row values are consistent for pivot key c0=1"
echo "[PQS-PIVOT] FINAL PASS: absent->present + value-level comparison validated."
exit 0
