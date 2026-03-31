#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-cdcup}"
export COMPOSE_PROJECT_NAME

PROBE_DATABASE="${PROBE_DATABASE:-flink36864_probe}"
PROBE_TABLE="${PROBE_TABLE:-t_flink36864}"
RUNTIME_YAML="${RUNTIME_YAML:-/tmp/flink36864_probe_pipeline.yaml}"
PIPELINE_LOG="${PIPELINE_LOG:-/tmp/flink36864_probe_submit.log}"
KEEP_JOB_ON_SUCCESS="${KEEP_JOB_ON_SUCCESS:-0}"

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

cancel_job_by_id() {
  local jid="$1"
  [[ -z "${jid}" ]] && return 0
  local jm_port
  jm_port=$(resolve_port jobmanager 8081 || true)
  [[ -z "${jm_port}" ]] && return 0
  curl -s -X PATCH "http://127.0.0.1:${jm_port}/jobs/${jid}?mode=cancel" >/dev/null 2>&1 || true
}

MYSQL_PORT="${MYSQL_PORT:-$(resolve_port mysql 3306 || true)}"
if [[ -z "${MYSQL_PORT}" ]]; then
  echo "[FLINK-36864] ERROR: failed to resolve mysql port"
  exit 2
fi

wait_mysql_ready 127.0.0.1 "${MYSQL_PORT}" 90

# Prepare dedicated source table for the probe.
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root -e "CREATE DATABASE IF NOT EXISTS ${PROBE_DATABASE};"
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${PROBE_DATABASE}" -e "
DROP TABLE IF EXISTS ${PROBE_TABLE};
CREATE TABLE ${PROBE_TABLE} (
  c0 INT PRIMARY KEY,
  c1 VARCHAR(64)
);
INSERT INTO ${PROBE_TABLE}(c0,c1) VALUES (1,'seed') ON DUPLICATE KEY UPDATE c1='seed';
"

cat > "${RUNTIME_YAML}" <<EOF
pipeline:
  parallelism: 1
source:
  type: mysql
  hostname: mysql
  port: 3306
  username: root
  password: ''
  tables: '${PROBE_DATABASE}.${PROBE_TABLE}'
  server-id: 7401
  server-time-zone: UTC
transform:
  - source-table: ${PROBE_DATABASE}.${PROBE_TABLE}
    projection: c0, 2147483649 AS big_number
sink:
  type: doris
  fenodes: doris:8030
  benodes: doris:8040
  jdbc-url: jdbc:mysql://doris:9030
  username: root
  password: ''
  sink.enable-delete: true
  table.create.properties.light_schema_change: true
  table.create.properties.replication_num: 1
EOF

set +e
(cd "${SCRIPT_DIR}" && ./cdcup.sh pipeline "${RUNTIME_YAML}") >"${PIPELINE_LOG}" 2>&1
RC=$?
set -e

if grep -Eqi 'Invalid integer literal "2147483649"|CompileException' "${PIPELINE_LOG}"; then
  echo "[FLINK-36864] REPRODUCED: Invalid integer literal for >Int32 number in transform projection"
  echo "[FLINK-36864] LOG: ${PIPELINE_LOG}"
  exit 0
fi

if [[ ${RC} -ne 0 ]]; then
  echo "[FLINK-36864] NOT_REPRODUCED: pipeline failed for another reason"
  tail -n 80 "${PIPELINE_LOG}" || true
  exit 0
fi

SUBMITTED_JOB_ID=$(grep -Eo 'Job ID: [a-fA-F0-9]+' "${PIPELINE_LOG}" | awk '{print $3}' | tail -n1 || true)
if [[ "${KEEP_JOB_ON_SUCCESS}" != "1" ]]; then
  cancel_job_by_id "${SUBMITTED_JOB_ID}"
fi

echo "[FLINK-36864] NOT_REPRODUCED: pipeline accepted long literal (likely fixed path)"
echo "[FLINK-36864] LOG: ${PIPELINE_LOG}"
exit 0
