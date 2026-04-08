#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-cdcup}"
export COMPOSE_PROJECT_NAME

DATABASE="${DATABASE:-database0}"
TABLE="${TABLE:-t0_cast_probe}"
WAIT_SECONDS="${WAIT_SECONDS:-12}"
RUNTIME_YAML="${RUNTIME_YAML:-/tmp/flink36866_pipeline.yaml}"
PIPELINE_LOG="${PIPELINE_LOG:-/tmp/flink36866_pipeline_submit.log}"
JOB_OVERVIEW_JSON="${JOB_OVERVIEW_JSON:-/tmp/flink36866_jobs_overview.json}"
JOB_LOG="${JOB_LOG:-/tmp/flink36866_job_logs.txt}"
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

echo "[FLINK-36866] Step 1/7: ensure containers are up"
if [[ "${CLEAN_RESTART}" == "1" ]]; then
  (cd "${SCRIPT_DIR}" && ./cdcup.sh down >/dev/null 2>&1 || true)
fi
(cd "${SCRIPT_DIR}" && ./cdcup.sh up >/dev/null 2>&1)

MYSQL_PORT="$(resolve_port mysql 3306)"
DORIS_FE_PORT="$(resolve_port doris 9030)"
if [[ -z "${MYSQL_PORT}" || -z "${DORIS_FE_PORT}" ]]; then
  echo "ERROR: failed to resolve mysql/doris ports"
  exit 1
fi

wait_mysql_ready 127.0.0.1 "${MYSQL_PORT}" 90
wait_mysql_ready 127.0.0.1 "${DORIS_FE_PORT}" 90

echo "[FLINK-36866] Step 2/7: prepare source table with floating values"
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root -e "DROP DATABASE IF EXISTS ${DATABASE}; CREATE DATABASE ${DATABASE};"
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "
CREATE TABLE ${TABLE} (
  c0 INT PRIMARY KEY,
  c1 DOUBLE,
  c2 VARCHAR(32)
);
INSERT INTO ${TABLE}(c0,c1,c2) VALUES
  (1, 3.14, '1.0'),
  (2, 2.71, '2.5');
"

mysql -h 127.0.0.1 -P "${DORIS_FE_PORT}" -u root -e "CREATE DATABASE IF NOT EXISTS ${DATABASE};" >/dev/null 2>&1 || true
mysql -h 127.0.0.1 -P "${DORIS_FE_PORT}" -u root "${DATABASE}" -e "DROP TABLE IF EXISTS ${TABLE};" >/dev/null 2>&1 || true

echo "[FLINK-36866] Step 3/7: build yaml with narrowing casts"
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
  server-id: 6500-7500
  server-time-zone: UTC
transform:
  - source-table: ${DATABASE}.${TABLE}
    projection: c0, CAST(c1 AS INT) as c1_int, CAST(c2 AS INT) as c2_int
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

echo "[FLINK-36866] Step 4/7: submit pipeline"
set +e
(cd "${SCRIPT_DIR}" && ./cdcup.sh pipeline "${RUNTIME_YAML}") >"${PIPELINE_LOG}" 2>&1
SUBMIT_RC=$?
set -e
if [[ ${SUBMIT_RC} -ne 0 ]]; then
  echo "RESULT=SUBMIT_FAILED"
  echo "DETAIL=Pipeline submit failed. See ${PIPELINE_LOG}"
  tail -n 120 "${PIPELINE_LOG}" || true
  exit 2
fi

echo "[FLINK-36866] Step 5/7: wait and inspect flink job status"
sleep "${WAIT_SECONDS}"
docker exec cdcup-jobmanager-1 sh -lc "curl -s http://localhost:8081/jobs/overview" >"${JOB_OVERVIEW_JSON}" || true

echo "[FLINK-36866] Step 6/7: collect job logs and detect cast failure signatures"
# docker logs may miss historical entries after container restarts, so read Flink log files directly.
docker exec cdcup-jobmanager-1 sh -lc "grep -nE 'NumberFormatException|Integer.valueOf|CAST\\(|cannot parse|InvocationTargetException' /opt/flink/log/flink--standalonesession-*-jobmanager.log 2>/dev/null" >"${JOB_LOG}" || true

if grep -q "NumberFormatException" "${JOB_LOG}"; then
  echo "RESULT=REPRODUCED"
  echo "DETAIL=Detected NumberFormatException in job logs, likely hitting FLINK-36866 path"
  tail -n 120 "${JOB_LOG}" || true
  exit 0
fi

if grep -q '"state":"FAILED"' "${JOB_OVERVIEW_JSON}"; then
  echo "RESULT=FAILED_BUT_NO_SIGNATURE"
  echo "DETAIL=Flink job failed but no NumberFormatException signature found. Check ${JOB_OVERVIEW_JSON} and ${PIPELINE_LOG}"
  exit 3
fi

echo "[FLINK-36866] Step 7/7: verify sink rows to infer fix presence"
SINK_ROWS=$(mysql -N -h 127.0.0.1 -P "${DORIS_FE_PORT}" -u root -e "SELECT COUNT(*) FROM ${DATABASE}.${TABLE};" 2>/dev/null || echo 0)
if [[ "${SINK_ROWS}" =~ ^[0-9]+$ ]] && [[ "${SINK_ROWS}" -ge 1 ]]; then
  echo "RESULT=NOT_REPRODUCED"
  echo "DETAIL=Pipeline runs and sink has rows (${SINK_ROWS}); environment likely includes the FLINK-36866 fix"
else
  echo "RESULT=INCONCLUSIVE"
  echo "DETAIL=No explicit failure signature, and sink row count is ${SINK_ROWS}. Inspect ${PIPELINE_LOG}, ${JOB_OVERVIEW_JSON}, ${JOB_LOG}"
fi

exit 0
