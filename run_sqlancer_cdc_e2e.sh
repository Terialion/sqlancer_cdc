#!/usr/bin/env bash

set -euo pipefail

# End-to-end workflow:
# 1) Ensure containers and DB clients are ready.
# 2) Create source database/table (database0.t0).
# 3) Submit Flink CDC pipeline.
# 4) Wait until sink table appears in sink system.
# 5) Generate and execute DML on source, then verify sink sync.
# 6) Generate and execute DDL on source, then verify schema sync.
# 7) Dump final source/sink states into a txt report.

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINK_PROFILES_SH="${SCRIPT_DIR}/sink_profiles.sh"

# Default to the root compose project so callers don't need to pass COMPOSE_PROJECT_NAME each run.
COMPOSE_PROJECT_NAME="${COMPOSE_PROJECT_NAME:-cdcup}"
export COMPOSE_PROJECT_NAME

if [[ ! -f "${SINK_PROFILES_SH}" ]]; then
  echo "ERROR: sink profiles file not found: ${SINK_PROFILES_SH}" >&2
  exit 1
fi

source "${SINK_PROFILES_SH}"

PIPELINE_YAML="${PIPELINE_YAML:-pipeline-definition.yaml}"
DATABASE="${DATABASE:-database0}"
TABLE="${TABLE:-t0}"
SINK_TYPE="${SINK_TYPE:-auto}"
WAIT_SYNC="${WAIT_SYNC:-10}"
ENABLE_STATUS_LOG="${ENABLE_STATUS_LOG:-1}"
STATUS_ROW_SAMPLE_EVERY="${STATUS_ROW_SAMPLE_EVERY:-10}"
STATUS_INCLUDE_SQL="${STATUS_INCLUDE_SQL:-0}"
ENABLE_SELECT_PHASE="${ENABLE_SELECT_PHASE:-0}"
MIXED_RECORD_SQL="${MIXED_RECORD_SQL:-0}"
AGGRESSIVE_BUG_TRIGGER="${AGGRESSIVE_BUG_TRIGGER:-0}"
FLINK36741_MAIN_TRANSFORM="${FLINK36741_MAIN_TRANSFORM:-0}"
PRINT_SCHEMA_SNAPSHOT="${PRINT_SCHEMA_SNAPSHOT:-1}"
# Timeout for waiting sink table creation after pipeline submission
WAIT_TABLE_TIMEOUT="${WAIT_TABLE_TIMEOUT:-60}"
# DML statements to generate and execute
DML_COUNT="${DML_COUNT:-100}"
DML_COMPLEX_WHERE="${DML_COMPLEX_WHERE:-1}"
# DDL statements to generate and execute
DDL_COUNT="${DDL_COUNT:-20}"
# DDL generation mode: alter_add or alter_mixed
DDL_MODE="${DDL_MODE:-alter_mixed}"
DDL_ENABLE_MODIFY="${DDL_ENABLE_MODIFY:-auto}"
# DROP COLUMN ratio in pure DDL phase when DDL_MODE=alter_mixed
DDL_DROP_RATIO="${DDL_DROP_RATIO:-35}"
# Statements for mixed DDL+DML phase after pure DDL phase
MIXED_COUNT="${MIXED_COUNT:-80}"
# Percentage of DDL in mixed phase (0-100)
MIXED_DDL_RATIO="${MIXED_DDL_RATIO:-35}"
MIX_DML_POOL_REFILL_SIZE="${MIX_DML_POOL_REFILL_SIZE:-25}"
DDL_SYNC_TIMEOUT="${DDL_SYNC_TIMEOUT:-60}"
BASE_SEED="${BASE_SEED:-42}"
REPORT_DIR="${REPORT_DIR:-/tmp/cdc_sqlancer_${BASE_SEED}}"
CANCEL_OLD_JOBS="${CANCEL_OLD_JOBS:-1}"
AUTO_RECOVER_CONTAINERS="${AUTO_RECOVER_CONTAINERS:-1}"
ROUNDS="${ROUNDS:-1}"
SEED_STEP="${SEED_STEP:-1}"
SUMMARY_FILE="${SUMMARY_FILE:-/tmp/cdc_sqlancer_batch_${BASE_SEED}.txt}"
IN_BATCH_MODE="${IN_BATCH_MODE:-0}"
SELF_SCRIPT="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"

resolve_pipeline_yaml_path() {
  local yaml_path="$1"
  if [[ "${yaml_path}" = /* ]]; then
    echo "${yaml_path}"
  else
    echo "${SCRIPT_DIR}/${yaml_path}"
  fi
}

PIPELINE_YAML_PATH="$(resolve_pipeline_yaml_path "${PIPELINE_YAML}")"
if [[ ! -f "${PIPELINE_YAML_PATH}" ]]; then
  echo "ERROR: pipeline yaml not found: ${PIPELINE_YAML_PATH}" >&2
  exit 1
fi

mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/source_sink_final_state.txt"
PIPELINE_LOG="${REPORT_DIR}/pipeline_submit.log"
RUNTIME_PIPELINE_YAML="${REPORT_DIR}/pipeline_mainflow_flink36741.yaml"
DML_SQL="${REPORT_DIR}/phase_dml.sql"
DDL_SQL="${REPORT_DIR}/phase_ddl.sql"
SELECT_SQL="${REPORT_DIR}/phase_select.sql"
MIXED_SQL="${REPORT_DIR}/phase_mixed.sql"
STATUS_FILE="${REPORT_DIR}/realtime_status.log"

EFFECTIVE_WAIT_SYNC="${WAIT_SYNC}"
EFFECTIVE_WAIT_TABLE_TIMEOUT="${WAIT_TABLE_TIMEOUT}"
EFFECTIVE_DDL_SYNC_TIMEOUT="${DDL_SYNC_TIMEOUT}"
ROW_CONVERGE_RETRIES="90"

# Keep behavior but reduce waiting overhead for quick bug reproduction loops.
if [[ "${EFFECTIVE_WAIT_SYNC}" -gt 2 ]]; then
  EFFECTIVE_WAIT_SYNC="2"
fi
if [[ "${EFFECTIVE_WAIT_TABLE_TIMEOUT}" -gt 90 ]]; then
  EFFECTIVE_WAIT_TABLE_TIMEOUT="45"
fi
if [[ "${EFFECTIVE_DDL_SYNC_TIMEOUT}" -gt 45 ]]; then
  EFFECTIVE_DDL_SYNC_TIMEOUT="20"
fi
ROW_CONVERGE_RETRIES="20"
if [[ "${STATUS_ROW_SAMPLE_EVERY}" -lt 60 ]]; then
  STATUS_ROW_SAMPLE_EVERY="60"
fi
# implies low-overhead observability to avoid repeatedly passing redundant knobs.
STATUS_INCLUDE_SQL="0"
ENABLE_SELECT_PHASE="0"
MIXED_RECORD_SQL="0"

if [[ "${AGGRESSIVE_BUG_TRIGGER}" == "1" ]]; then
  # Stress profile: favor schema churn + mixed workload to increase bug triggering probability.
  (( DML_COUNT < 100 )) && DML_COUNT=100
  (( DDL_COUNT < 16 )) && DDL_COUNT=16
  (( MIXED_COUNT < 80 )) && MIXED_COUNT=80
  (( MIXED_DDL_RATIO < 35 )) && MIXED_DDL_RATIO=35
  (( MIX_DML_POOL_REFILL_SIZE < 80 )) && MIX_DML_POOL_REFILL_SIZE=80
  DML_COMPLEX_WHERE="1"
  if [[ "${FLINK36741_MAIN_TRANSFORM}" == "0" ]]; then
    FLINK36741_MAIN_TRANSFORM="1"
  fi
fi

detect_sink_type_from_yaml() {
  local yaml_path="$1"
  awk '
    /^sink:[[:space:]]*$/ { in_sink=1; next }
    in_sink && /^[^[:space:]]/ { in_sink=0 }
    in_sink && $1 == "type:" { print $2; exit }
  ' "${yaml_path}" | tr -d "\"'"
}

configure_sink_runtime() {
  local detected
  detected="$(detect_sink_type_from_yaml "${PIPELINE_YAML_PATH}")"
  if [[ "${SINK_TYPE}" == "auto" || -z "${SINK_TYPE}" ]]; then
    SINK_TYPE="${detected:-doris}"
  fi
  SINK_TYPE="$(echo "${SINK_TYPE}" | tr '[:upper:]' '[:lower:]')"

  if ! resolve_sink_profile "${SINK_TYPE}"; then
    log "ERROR: Unsupported sink type: ${SINK_TYPE}"
    log "Add profile in sink_profiles.sh to support new sink quickly."
    exit 1
  fi

  if [[ "${DDL_ENABLE_MODIFY}" == "auto" ]]; then
    if [[ "${SINK_TYPE}" == "doris" ]]; then
      DDL_ENABLE_MODIFY="0"
    else
      DDL_ENABLE_MODIFY="1"
    fi
  fi
}

timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

log() {
  echo "[$(timestamp)] $*"
}

step_timer_start() {
  STEP_TIMER_NAME="$1"
  STEP_TIMER_START_TS=$(date +%s)
}

step_timer_end() {
  local now
  local elapsed
  now=$(date +%s)
  elapsed=$((now - STEP_TIMER_START_TS))
  TOTAL_STEP_SECONDS=$((TOTAL_STEP_SECONDS + elapsed))
  append_report "Timer | ${STEP_TIMER_NAME}: ${elapsed}s"
}

ensure_image_exists() {
  local image="$1"
  docker image inspect "${image}" >/dev/null 2>&1
}

check_required_images() {
  local missing=()
  local image
  for image in "$@"; do
    if ! ensure_image_exists "${image}"; then
      missing+=("${image}")
    fi
  done

  if [[ ${#missing[@]} -gt 0 ]]; then
    log "ERROR: Required images are missing: ${missing[*]}"
    log "Please pull them first (or run ./cdcup.sh up once manually)."
    exit 1
  fi
}

resolve_port() {
  local service="$1"
  local cport="$2"
  local out
  out=$(docker compose port "${service}" "${cport}" 2>/dev/null || true)
  if [[ -z "${out}" ]]; then
    return 1
  fi
  echo "${out}" | awk -F: '{print $NF}' | tail -n1
}

get_jm_port() {
  resolve_port jobmanager 8081 || true
}

flink_api_get() {
  local path="$1"
  local jm_port
  jm_port=$(get_jm_port)
  if [[ -z "${jm_port}" ]]; then
    return 1
  fi
  curl -s "http://127.0.0.1:${jm_port}${path}" || true
}

list_active_job_ids() {
  local jobs_json
  jobs_json=$(flink_api_get "/jobs/overview")
  if [[ -z "${jobs_json}" ]]; then
    return 0
  fi

  # Output one active job id per line.
  printf '%s' "${jobs_json}" | "${PYTHON_BIN}" -c '
import json,sys
active={"INITIALIZING","CREATED","RUNNING","FAILING","RESTARTING","CANCELLING","RECONCILING","SCHEDULED","DEPLOYING"}
try:
    data=json.load(sys.stdin)
except Exception:
    sys.exit(0)
for j in data.get("jobs",[]):
    if j.get("state") in active and j.get("jid"):
        print(j["jid"])
'
}

cancel_job_by_id() {
  local jid="$1"
  local jm_port
  jm_port=$(get_jm_port)
  [[ -z "${jm_port}" || -z "${jid}" ]] && return 1
  curl -s -X PATCH "http://127.0.0.1:${jm_port}/jobs/${jid}?mode=cancel" >/dev/null 2>&1 || true
}

wait_no_active_jobs() {
  local retries="${1:-60}"
  local i
  for i in $(seq 1 "${retries}"); do
    local ids
    ids=$(list_active_job_ids || true)
    if [[ -z "${ids}" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

get_job_state() {
  local jid="$1"
  [[ -z "${jid}" ]] && return 0
  local status_json
  status_json=$(flink_api_get "/jobs/${jid}/status")
  [[ -z "${status_json}" ]] && return 0
  printf '%s' "${status_json}" | "${PYTHON_BIN}" -c '
import json,sys
try:
    data=json.load(sys.stdin)
    print(data.get("state",""))
except Exception:
    pass
'
}

append_job_exception() {
  local jid="$1"
  [[ -z "${jid}" ]] && return 0
  local exc
  exc=$(flink_api_get "/jobs/${jid}/exceptions")
  if [[ -n "${exc}" ]]; then
    append_report "Flink job exceptions (compact):"
    printf '%s\n' "${exc}" | "${PYTHON_BIN}" -c '
import json,sys
try:
    d=json.load(sys.stdin)
except Exception:
    print("<unable to parse exceptions json>")
    sys.exit(0)
hist=d.get("exceptionHistory",{}).get("entries",[])
if hist:
    top=hist[0]
    print(top.get("exceptionName","<unknown exception>"))
    st=top.get("stacktrace","")
    if st:
        print("\\n".join(st.splitlines()[:20]))
else:
    root=d.get("root-exception","")
    print(root if root else "<no exception history>")
' | tee -a "${REPORT_FILE}"
  fi
}

wait_flink_ready() {
  local retries="${1:-60}"
  local i
  local jm_port
  jm_port=$(resolve_port jobmanager 8081 || true)
  if [[ -z "${jm_port}" ]]; then
    return 1
  fi

  for i in $(seq 1 "${retries}"); do
    overview=$(curl -s "http://127.0.0.1:${jm_port}/taskmanagers" || true)
    if echo "${overview}" | grep -q '"taskmanagers"' && echo "${overview}" | grep -q '"id"'; then
      return 0
    fi
    sleep 1
  done
  return 1
}

mysql_ready() {
  local host="$1"
  local port="$2"
  mysql -h "${host}" -P "${port}" -u root -e "SELECT 1" >/dev/null 2>&1
}

wait_mysql_ready() {
  local host="$1"
  local port="$2"
  local retries="${3:-60}"
  local i
  for i in $(seq 1 "${retries}"); do
    if mysql_ready "${host}" "${port}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_row_count_converged() {
  local retries="${1:-60}"
  local i
  for i in $(seq 1 "${retries}"); do
    local s_count
    local d_count
    s_count=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
    if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
      d_count=$(mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
    else
      d_count="NA"
    fi
    if [[ "${s_count}" != "NA" && "${s_count}" == "${d_count}" ]]; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_sink_column_ready() {
  local col_name="$1"
  local retries="${2:-60}"
  local i
  for i in $(seq 1 "${retries}"); do
    if mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -e "DESC ${TABLE};" \
      | awk '{print $1}' | grep -Fxq "${col_name}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_sink_expected_columns() {
  local expected_file="$1"
  local retries="${2:-60}"

  if [[ "${SINK_SQL_ENABLED}" != "1" || ! -f "${expected_file}" ]]; then
    echo "0 0"
    return 0
  fi

  local total=0
  local pending=0
  local i
  local sink_cols
  local col

  total=$(sort -u "${expected_file}" | sed '/^$/d' | wc -l | tr -d ' ')
  if [[ -z "${total}" || "${total}" == "0" ]]; then
    echo "0 0"
    return 0
  fi

  pending="${total}"
  for i in $(seq 1 "${retries}"); do
    sink_cols=$(mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -sN -e "DESC ${TABLE};" 2>/dev/null | awk '{print $1}' || true)
    if [[ -n "${sink_cols}" ]]; then
      pending=0
      while IFS= read -r col; do
        [[ -z "${col}" ]] && continue
        if ! echo "${sink_cols}" | grep -Fxq "${col}"; then
          pending=$((pending + 1))
        fi
      done < <(sort -u "${expected_file}" | sed '/^$/d')

      if [[ "${pending}" -eq 0 ]]; then
        echo "${total} 0"
        return 0
      fi
    fi
    sleep 1
  done

  echo "$((total - pending)) ${pending}"
  return 0
}

append_report() {
  mkdir -p "$(dirname "${REPORT_FILE}")"
  echo "$*" | tee -a "${REPORT_FILE}"
}

append_status() {
  if [[ "${ENABLE_STATUS_LOG}" != "1" ]]; then
    return 0
  fi

  local phase="$1"
  local idx="$2"
  local kind="$3"
  local result="$4"
  local detail="$5"
  local stmt="$6"
  local stmt_text="<omitted>"
  local should_refresh=0

  STATUS_EVENT_COUNTER=$((STATUS_EVENT_COUNTER + 1))
  if [[ "${result}" != "ok" ]]; then
    should_refresh=1
  elif [[ "${STATUS_EVENT_COUNTER}" -eq 1 ]]; then
    should_refresh=1
  elif [[ "${STATUS_ROW_SAMPLE_EVERY}" -gt 0 ]] && (( STATUS_EVENT_COUNTER % STATUS_ROW_SAMPLE_EVERY == 0 )); then
    should_refresh=1
  fi

  if [[ "${should_refresh}" -eq 1 ]]; then
    STATUS_LAST_SOURCE_COUNT=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
    if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
      STATUS_LAST_SINK_COUNT=$(mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
    else
      STATUS_LAST_SINK_COUNT="NA"
    fi
  fi

  if [[ "${STATUS_INCLUDE_SQL}" == "1" ]]; then
    stmt_text="$(echo "${stmt}" | tr '\n' ' ' | tr '\r' ' ')"
  else
    stmt_text="<hidden>"
  fi

  # Keep one-line status records to mimic SQLancer-like statement-by-statement logs.
  mkdir -p "$(dirname "${STATUS_FILE}")"
  printf '%s | phase=%s | idx=%s | kind=%s | result=%s | source_rows=%s | sink_rows=%s | detail=%s | stmt=%s\n' \
    "$(timestamp)" "${phase}" "${idx}" "${kind}" "${result}" "${STATUS_LAST_SOURCE_COUNT}" "${STATUS_LAST_SINK_COUNT}" "${detail}" "${stmt_text}" \
    >> "${STATUS_FILE}"
}

get_source_columns_csv() {
  if [[ -n "${SCHEMA_CACHE_COLUMNS_CSV:-}" ]]; then
    printf '%s\n' "${SCHEMA_CACHE_COLUMNS_CSV}"
    return 0
  fi
  mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "DESC ${TABLE};" 2>/dev/null | awk '{print $1}' | paste -sd, -
}

get_source_column_count() {
  mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='${DATABASE}' AND table_name='${TABLE}';" 2>/dev/null || echo "0"
}

get_sink_column_count() {
  if [[ "${SINK_SQL_ENABLED}" != "1" ]]; then
    echo "NA"
    return 0
  fi
  mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root -sN -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='${DATABASE}' AND table_name='${TABLE}';" 2>/dev/null || echo "0"
}

to_dml_data_type() {
  local t="${1,,}"
  case "${t}" in
    int*|mediumint*) echo "INT" ;;
    bigint*) echo "BIGINT" ;;
    smallint*)
      echo "SMALLINT"
      ;;
    tinyint*)
      if [[ "${t}" == "tinyint(1)"* ]]; then
        echo "BOOLEAN"
      else
        echo "TINYINT"
      fi
      ;;
    varchar*|char*) echo "VARCHAR" ;;
    text*|mediumtext*|longtext*) echo "TEXT" ;;
    float*) echo "FLOAT" ;;
    double*) echo "DOUBLE" ;;
    decimal*) echo "DECIMAL" ;;
    datetime*) echo "DATETIME" ;;
    timestamp*) echo "TIMESTAMP" ;;
    date*) echo "DATE" ;;
    bool*|boolean*) echo "BOOLEAN" ;;
    *) echo "VARCHAR" ;;
  esac
}

build_dml_columns_spec() {
  if [[ -n "${SCHEMA_CACHE_DML_SPEC:-}" ]]; then
    printf '%s\n' "${SCHEMA_CACHE_DML_SPEC}"
    return 0
  fi
  mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "DESC ${TABLE};" 2>/dev/null | while read -r field type null key _; do
    [[ -z "${field}" ]] && continue
    local mapped
    mapped=$(to_dml_data_type "${type}")
    local constraint="NONE"
    if [[ "${key}" == "PRI" ]]; then
      constraint="PK"
    elif [[ "${null}" == "NO" ]]; then
      constraint="NOT_NULL"
    fi
    printf '%s:%s:%s\n' "${field}" "${mapped}" "${constraint}"
  done | paste -sd, -
}

refresh_schema_cache_from_source() {
  local schema_dump
  schema_dump=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "DESC ${TABLE};" 2>/dev/null || true)
  if [[ -z "${schema_dump}" ]]; then
    SCHEMA_CACHE_COLUMNS_CSV=""
    SCHEMA_CACHE_DML_SPEC=""
    return 1
  fi

  SCHEMA_CACHE_COLUMNS_CSV=$(printf '%s\n' "${schema_dump}" | awk '{print $1}' | paste -sd, -)
  SCHEMA_CACHE_DML_SPEC=$(printf '%s\n' "${schema_dump}" | while read -r field type null key _; do
    [[ -z "${field}" ]] && continue
    local mapped
    mapped=$(to_dml_data_type "${type}")
    local constraint="NONE"
    if [[ "${key}" == "PRI" ]]; then
      constraint="PK"
    elif [[ "${null}" == "NO" ]]; then
      constraint="NOT_NULL"
    fi
    printf '%s:%s:%s\n' "${field}" "${mapped}" "${constraint}"
  done | paste -sd, -)
}

extract_added_col_and_type() {
  local stmt="$1"
  local parsed
  parsed=$(echo "${stmt}" | sed -n 's/.*ADD COLUMN `\([^`]*\)` \([A-Za-z0-9_,()]*\).*/\1,\2/p')
  [[ -n "${parsed}" ]] && printf '%s\n' "${parsed}"
}

update_schema_cache_for_ddl() {
  local ddl="$1"
  [[ -z "${SCHEMA_CACHE_COLUMNS_CSV:-}" || -z "${SCHEMA_CACHE_DML_SPEC:-}" ]] && return 0

  local added_col
  local added_type
  local dropped_col
  local mapped
  local new_token

  if echo "${ddl}" | grep -Eqi 'ADD[[:space:]]+COLUMN'; then
    IFS=',' read -r added_col added_type <<< "$(extract_added_col_and_type "${ddl}")"
    if [[ -n "${added_col}" && -n "${added_type}" ]]; then
      if ! echo "${SCHEMA_CACHE_COLUMNS_CSV}" | tr ',' '\n' | grep -Fxq "${added_col}"; then
        SCHEMA_CACHE_COLUMNS_CSV="${SCHEMA_CACHE_COLUMNS_CSV},${added_col}"
      fi
      mapped=$(to_dml_data_type "${added_type}")
      new_token="${added_col}:${mapped}:NONE"
      if [[ -z "${SCHEMA_CACHE_DML_SPEC}" ]]; then
        SCHEMA_CACHE_DML_SPEC="${new_token}"
      elif ! echo "${SCHEMA_CACHE_DML_SPEC}" | tr ',' '\n' | grep -Fqx "${new_token}"; then
        SCHEMA_CACHE_DML_SPEC="${SCHEMA_CACHE_DML_SPEC},${new_token}"
      fi
    fi
    return 0
  fi

  if echo "${ddl}" | grep -Eqi 'DROP[[:space:]]+COLUMN'; then
    dropped_col=$(echo "${ddl}" | sed -n 's/.*DROP COLUMN `\([^`]*\)`.*/\1/p')
    if [[ -n "${dropped_col}" ]]; then
      SCHEMA_CACHE_COLUMNS_CSV=$(echo "${SCHEMA_CACHE_COLUMNS_CSV}" | tr ',' '\n' | grep -vx "${dropped_col}" | paste -sd, -)
      SCHEMA_CACHE_DML_SPEC=$(echo "${SCHEMA_CACHE_DML_SPEC}" | tr ',' '\n' | grep -Ev "^${dropped_col}:" | paste -sd, -)
    fi
  fi
}

refill_mixed_dml_pool() {
  local seed="$1"
  local cols_spec
  cols_spec=$(build_dml_columns_spec)

  mapfile -t MIX_DML_POOL < <(
    "${PYTHON_BIN}" "${SCRIPT_DIR}/dml_generator.py" \
      --count "${MIX_DML_POOL_REFILL_SIZE}" \
      --seed "${seed}" \
      --type all \
      --table-name "${TABLE}" \
      --columns "${cols_spec}" \
      $( [[ "${DML_COMPLEX_WHERE}" = "1" ]] && echo "--allow-complex-where" ) \
      --output-format sql 2>/dev/null | awk 'NF && $0 !~ /^--/'
  )
  MIX_DML_POOL_POS=0
}

run_batch_mode_if_needed() {
  local total_rounds
  total_rounds=$((ROUNDS))

  if [[ "${IN_BATCH_MODE}" = "1" || ${total_rounds} -le 1 ]]; then
    return 0
  fi

  local r
  local round_seed
  local round_report_dir
  local round_rc
  local ok=0
  local fail=0

  : > "${SUMMARY_FILE}"
  : > "${STATUS_FILE}"
  echo "Batch run started at: $(timestamp)" | tee -a "${SUMMARY_FILE}"
  echo "Rounds=${ROUNDS}, seed_start=${BASE_SEED}, seed_step=${SEED_STEP}" | tee -a "${SUMMARY_FILE}"
  echo | tee -a "${SUMMARY_FILE}"

  for ((r = 1; r <= total_rounds; r++)); do
    round_seed=$((BASE_SEED + (r - 1) * SEED_STEP))
    round_report_dir="${REPORT_DIR}_round${r}"

    echo "=== ROUND ${r}/${total_rounds} seed=${round_seed} ===" | tee -a "${SUMMARY_FILE}"
    set +e
    IN_BATCH_MODE=1 \
    ROUNDS=1 \
    BASE_SEED="${round_seed}" \
    REPORT_DIR="${round_report_dir}" \
    PIPELINE_YAML="${PIPELINE_YAML}" \
    DATABASE="${DATABASE}" \
    TABLE="${TABLE}" \
    WAIT_SYNC="${WAIT_SYNC}" \
    WAIT_TABLE_TIMEOUT="${WAIT_TABLE_TIMEOUT}" \
    DML_COUNT="${DML_COUNT}" \
    DDL_COUNT="${DDL_COUNT}" \
    DDL_SYNC_TIMEOUT="${DDL_SYNC_TIMEOUT}" \
    AGGRESSIVE_BUG_TRIGGER="${AGGRESSIVE_BUG_TRIGGER}" \
    PRINT_SCHEMA_SNAPSHOT="${PRINT_SCHEMA_SNAPSHOT}" \
    CANCEL_OLD_JOBS="${CANCEL_OLD_JOBS}" \
    "${SELF_SCRIPT}"
    round_rc=$?
    set -e

    if [[ ${round_rc} -eq 0 ]]; then
      ok=$((ok + 1))
    else
      fail=$((fail + 1))
    fi

    if [[ -f "${round_report_dir}/source_sink_final_state.txt" ]]; then
      grep -E "DML success/fail|DDL success/fail|Mixed DML success/fail|Mixed DDL success/fail|Row count \(MySQL/.+\)|Schema new columns synced" \
        "${round_report_dir}/source_sink_final_state.txt" \
        | tee -a "${SUMMARY_FILE}" || true
    else
      echo "No final report generated for this round." | tee -a "${SUMMARY_FILE}"
    fi

    echo "round_exit_code=${round_rc}" | tee -a "${SUMMARY_FILE}"
    echo | tee -a "${SUMMARY_FILE}"
  done

  echo "Batch run finished at: $(timestamp)" | tee -a "${SUMMARY_FILE}"
  echo "round_ok=${ok}, round_fail=${fail}" | tee -a "${SUMMARY_FILE}"
  echo "summary_file=${SUMMARY_FILE}" | tee -a "${SUMMARY_FILE}"

  if [[ ${fail} -gt 0 ]]; then
    exit 1
  fi
  exit 0
}

run_batch_mode_if_needed

configure_sink_runtime

log "Starting SQLancer CDC E2E workflow"
WORKFLOW_START_TS=$(date +%s)
TOTAL_STEP_SECONDS=0
STEP_TIMER_NAME=""
STEP_TIMER_START_TS=0

check_required_images \
  "flink:1.20.3-scala_2.12" \
  "mysql:8.0"

have_mysql=0
have_sink=0
have_jobmanager=0
have_taskmanager=0
running_services=$(docker compose ps --status running --services 2>/dev/null || true)
if echo "${running_services}" | grep -Fxq "mysql"; then
  have_mysql=1
fi
if [[ -n "${SINK_SERVICE}" ]] && echo "${running_services}" | grep -Fxq "${SINK_SERVICE}"; then
  have_sink=1
fi
if echo "${running_services}" | grep -Fxq "jobmanager"; then
  have_jobmanager=1
fi
if echo "${running_services}" | grep -Fxq "taskmanager"; then
  have_taskmanager=1
fi

if [[ ${have_mysql} -ne 1 || ${have_jobmanager} -ne 1 || ${have_taskmanager} -ne 1 ]]; then
  if [[ "${AUTO_RECOVER_CONTAINERS}" == "1" ]]; then
    log "WARN: Required services missing, try auto-recover with ./cdcup.sh up"
    set +e
    (cd "${SCRIPT_DIR}" && ./cdcup.sh up >/dev/null 2>&1)
    RECOVER_RC=$?
    set -e
    if [[ ${RECOVER_RC} -ne 0 ]]; then
      log "WARN: auto-recover command returned non-zero: ${RECOVER_RC}"
    fi

    have_mysql=0
    have_sink=0
    have_jobmanager=0
    have_taskmanager=0
    running_services=$(docker compose ps --status running --services 2>/dev/null || true)
    if echo "${running_services}" | grep -Fxq "mysql"; then
      have_mysql=1
    fi
    if [[ -n "${SINK_SERVICE}" ]] && echo "${running_services}" | grep -Fxq "${SINK_SERVICE}"; then
      have_sink=1
    fi
    if echo "${running_services}" | grep -Fxq "jobmanager"; then
      have_jobmanager=1
    fi
    if echo "${running_services}" | grep -Fxq "taskmanager"; then
      have_taskmanager=1
    fi
  fi
fi

if [[ ${have_mysql} -ne 1 || ${have_jobmanager} -ne 1 || ${have_taskmanager} -ne 1 ]]; then
  log "ERROR: Required containers are not fully running (need mysql/jobmanager/taskmanager)."
  log "Start them manually with ./cdcup.sh up, then re-run this script."
  exit 1
fi

if [[ "${SINK_SQL_ENABLED}" == "1" && ${have_sink} -ne 1 ]]; then
  log "ERROR: Required sink container is not running: ${SINK_SERVICE}"
  log "Start them manually with ./cdcup.sh up, then re-run this script."
  exit 1
fi

if ! wait_flink_ready 90; then
  log "ERROR: Flink cluster is not ready (no active taskmanagers)"
  exit 1
fi

MYSQL_PORT="${MYSQL_PORT:-$(resolve_port mysql 3306 || true)}"
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  SINK_PORT="${SINK_PORT:-$(resolve_port "${SINK_SERVICE}" "${SINK_DB_PORT}" || true)}"
else
  SINK_PORT=""
fi

if [[ -z "${MYSQL_PORT}" || ( "${SINK_SQL_ENABLED}" == "1" && -z "${SINK_PORT}" ) ]]; then
  log "ERROR: Failed to resolve MySQL/sink ports"
  exit 1
fi

if ! wait_mysql_ready 127.0.0.1 "${MYSQL_PORT}" 90; then
  log "ERROR: MySQL is not ready on port ${MYSQL_PORT}"
  exit 1
fi

if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  if ! wait_mysql_ready 127.0.0.1 "${SINK_PORT}" 90; then
    log "ERROR: ${SINK_LABEL} MySQL endpoint is not ready on port ${SINK_PORT}"
    exit 1
  fi
fi

PYTHON_BIN=""
if [[ -x "${SCRIPT_DIR}/.venv/bin/python" ]]; then
  PYTHON_BIN="${SCRIPT_DIR}/.venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PYTHON_BIN="python3"
else
  log "ERROR: python runtime not found"
  exit 1
fi

{
  echo "============================================================"
  echo "SQLancer CDC E2E Final Report"
  echo "Started at: $(timestamp)"
  echo "Workdir: ${SCRIPT_DIR}"
  echo "MySQL: 127.0.0.1:${MYSQL_PORT}"
  if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
    echo "Sink(${SINK_LABEL}): 127.0.0.1:${SINK_PORT}"
  else
    echo "Sink(${SINK_LABEL}): SQL endpoint not used"
  fi
  echo "Database/Table: ${DATABASE}.${TABLE}"
  echo "Pipeline YAML: ${PIPELINE_YAML}"
  echo "Pipeline YAML (resolved): ${PIPELINE_YAML_PATH}"
  echo "Aggressive Trigger Mode: ${AGGRESSIVE_BUG_TRIGGER}"
  echo "============================================================"
} > "${REPORT_FILE}"

: > "${STATUS_FILE}"
STATUS_EVENT_COUNTER=0
STATUS_LAST_SOURCE_COUNT="NA"
STATUS_LAST_SINK_COUNT="NA"

append_report ""
append_report "[Step 1] Create source database/table"
step_timer_start "Step 1 Create source database/table"

mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root -e "DROP DATABASE IF EXISTS ${DATABASE};"
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root -e "DROP DATABASE IF EXISTS ${DATABASE};" >/dev/null 2>&1 || true
fi
append_report "Source/sink database reset: ${DATABASE}"

mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root -e "CREATE DATABASE IF NOT EXISTS ${DATABASE};"
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "
DROP TABLE IF EXISTS ${TABLE};
CREATE TABLE ${TABLE} (
  c0 INT PRIMARY KEY,
  c1 VARCHAR(256) NOT NULL,
  c2 INT,
  c3 VARCHAR(256),
  c4 DECIMAL(10,2)
);
"
append_report "Source table created: ${DATABASE}.${TABLE}"
append_status "bootstrap" 0 "init" "ok" "source table created" "CREATE TABLE ${TABLE} (...)"

# Reset sink table to avoid cross-run state pollution during repeated rounds.
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root -e "CREATE DATABASE IF NOT EXISTS ${DATABASE};" >/dev/null 2>&1 || true
  mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -e "DROP TABLE IF EXISTS ${TABLE};" >/dev/null 2>&1 || true
  append_report "Sink table reset in ${SINK_LABEL}: ${DATABASE}.${TABLE}"
else
  append_report "Sink reset skipped for ${SINK_LABEL} (no SQL endpoint)"
fi
append_status "bootstrap" 1 "init" "ok" "sink bootstrap" "reset sink object"
step_timer_end

append_report ""
append_report "[Step 2] Submit pipeline"
step_timer_start "Step 2 Submit pipeline"

PIPELINE_SUBMIT_PATH="${PIPELINE_YAML_PATH}"
if [[ "${FLINK36741_MAIN_TRANSFORM}" == "1" && "${SINK_TYPE}" == "doris" ]]; then
  cat > "${RUNTIME_PIPELINE_YAML}" <<EOF
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
    projection: c0, c1, c2, c3, c4 as deposits
sink:
  type: doris
  fenodes: doris:8030
  benodes: doris:8040
  jdbc-url: jdbc:mysql://doris:9030
  username: root
  password: ''
  table.create.properties.light_schema_change: true
  table.create.properties.replication_num: 1
EOF
  PIPELINE_SUBMIT_PATH="${RUNTIME_PIPELINE_YAML}"
  append_report "Main-flow FLINK-36741 mode: enabled (transform alias c4 -> deposits)."
fi

if [[ "${CANCEL_OLD_JOBS}" = "1" ]]; then
  append_report "Cancel active Flink jobs before submission"
  EXISTING_JOBS=$(list_active_job_ids || true)
  if [[ -n "${EXISTING_JOBS}" ]]; then
    while IFS= read -r jid; do
      [[ -z "${jid}" ]] && continue
      cancel_job_by_id "${jid}"
    done <<< "${EXISTING_JOBS}"
  fi
  if ! wait_no_active_jobs 90; then
    append_report "WARN: timeout waiting active jobs to terminate; continue submission"
  fi
fi

set +e
(cd "${SCRIPT_DIR}" && ./cdcup.sh pipeline "${PIPELINE_SUBMIT_PATH}") >"${PIPELINE_LOG}" 2>&1
PIPELINE_RC=$?
set -e

if [[ ${PIPELINE_RC} -ne 0 ]]; then
  append_report "Pipeline submission failed. See log: ${PIPELINE_LOG}"
  tail -n 80 "${PIPELINE_LOG}" | tee -a "${REPORT_FILE}"
  exit 1
fi

PIPELINE_JOB_ID=$(grep -Eo 'Job ID: [a-fA-F0-9]+' "${PIPELINE_LOG}" | awk '{print $3}' | tail -n1 || true)
if [[ -z "${PIPELINE_JOB_ID}" ]]; then
  PIPELINE_JOB_ID=$(grep -Eo 'job ID is [a-fA-F0-9]+' "${PIPELINE_LOG}" | awk '{print $4}' | tail -n1 || true)
fi
if [[ -z "${PIPELINE_JOB_ID}" ]]; then
  PIPELINE_JOB_ID=$(list_active_job_ids | head -n1 || true)
fi
append_report "Pipeline submitted successfully. Job ID: ${PIPELINE_JOB_ID:-UNKNOWN}"
step_timer_end

append_report ""
append_report "[Step 3] Wait for sink table creation in ${SINK_LABEL}"
step_timer_start "Step 3 Wait sink table creation"

# Trigger at least one CDC event so sink table is materialized even when snapshot is empty.
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "
INSERT INTO ${TABLE} (c0, c1, c2, c3, c4) VALUES (1, 'bootstrap', 1, 'init', 1.0)
ON DUPLICATE KEY UPDATE c1='bootstrap', c2=1, c3='init', c4=1.0;
" >/dev/null 2>&1 || true

table_ready="false"
for i in $(seq 1 "${EFFECTIVE_WAIT_TABLE_TIMEOUT}"); do
  if [[ -n "${PIPELINE_JOB_ID:-}" ]]; then
    state=$(get_job_state "${PIPELINE_JOB_ID}" || true)
    if [[ "${state}" == "FAILED" || "${state}" == "CANCELED" || "${state}" == "SUSPENDED" ]]; then
      append_report "Pipeline job entered terminal state early: ${state}"
      append_job_exception "${PIPELINE_JOB_ID}"
      exit 1
    fi
  fi

  if [[ "${SINK_SQL_ENABLED}" != "1" ]]; then
    table_ready="true"
    break
  fi

  if mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root -e "SHOW DATABASES LIKE '${DATABASE}';" | grep -q "${DATABASE}"; then
    if mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -e "SHOW TABLES LIKE '${TABLE}';" 2>/dev/null | grep -q "${TABLE}"; then
      table_ready="true"
      break
    fi
  fi
  sleep 1
done

if [[ "${table_ready}" != "true" ]]; then
  append_report "Sink table not detected within ${EFFECTIVE_WAIT_TABLE_TIMEOUT}s."
  append_report "Try checking job logs and pipeline regex in ${PIPELINE_YAML_PATH}."
  append_job_exception "${PIPELINE_JOB_ID:-}"
  exit 1
fi

append_report "Sink ready in ${SINK_LABEL}: ${DATABASE}.${TABLE}"
step_timer_end

append_report ""
append_report "[Step 4] DML phase: generate and execute on source, verify on sink"
step_timer_start "Step 4 DML phase"

"${PYTHON_BIN}" "${SCRIPT_DIR}/dml_generator.py" \
  --count "${DML_COUNT}" \
  --seed "${BASE_SEED}" \
  $( [[ "${DML_COMPLEX_WHERE}" = "1" ]] && echo "--allow-complex-where" ) \
  --output-sql "${DML_SQL}" >/dev/null

DML_OK=0
DML_FAIL=0
stmt_idx=0
while IFS= read -r stmt; do
  [[ -z "${stmt}" || "${stmt}" =~ ^-- ]] && continue
  stmt_idx=$((stmt_idx + 1))
  if printf '%s\n' "${stmt}" | mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" >/dev/null 2>&1; then
    DML_OK=$((DML_OK + 1))
    append_status "dml" "${stmt_idx}" "DML" "ok" "phase=dml" "${stmt}"
  else
    DML_FAIL=$((DML_FAIL + 1))
    append_status "dml" "${stmt_idx}" "DML" "fail" "phase=dml" "${stmt}"
  fi
done < "${DML_SQL}"

sleep "${EFFECTIVE_WAIT_SYNC}"

MYSQL_COUNT_1=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};")
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  SINK_COUNT_1=$(mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};")
else
  SINK_COUNT_1="NA"
fi

append_report "DML executed: success=${DML_OK}, failed=${DML_FAIL}"
append_report "Row count after DML: MySQL=${MYSQL_COUNT_1}, ${SINK_LABEL}=${SINK_COUNT_1}"

if [[ "${ENABLE_SELECT_PHASE}" == "1" ]]; then
  "${PYTHON_BIN}" "${SCRIPT_DIR}/select_generator.py" \
    --count 10 \
    --seed "$((BASE_SEED + 7))" \
    --type all \
    --output-sql "${SELECT_SQL}" >/dev/null 2>&1 || true
fi
  step_timer_end

append_report ""
append_report "[Step 5] DDL phase: generate ALTER statements and verify schema sync"
  step_timer_start "Step 5 DDL phase"

"${PYTHON_BIN}" "${SCRIPT_DIR}/ddl_generator.py" \
  --count "${DDL_COUNT}" \
  --seed "$((BASE_SEED + 1))" \
  --type "${DDL_MODE}" \
  --table-name "${TABLE}" \
  --existing-cols "$(get_source_columns_csv)" \
  --protected-cols "c0,c4" \
  --drop-ratio "${DDL_DROP_RATIO}" \
  $( [[ "${DDL_ENABLE_MODIFY}" == "1" ]] && echo "--enable-modify" ) \
  --output-sql "${DDL_SQL}" >/dev/null

DDL_OK=0
DDL_FAIL=0
NEW_COLS_FILE="${REPORT_DIR}/new_columns.txt"
DDL_EXPECTED_COLS_FILE="${REPORT_DIR}/ddl_expected_columns.txt"
rm -f "${NEW_COLS_FILE}" "${DDL_EXPECTED_COLS_FILE}"
stmt_idx=0

while IFS= read -r ddl; do
  [[ -z "${ddl}" || "${ddl}" =~ ^-- ]] && continue
  stmt_idx=$((stmt_idx + 1))
  if printf '%s\n' "${ddl}" | mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" >/dev/null 2>&1; then
    DDL_OK=$((DDL_OK + 1))
    append_status "ddl" "${stmt_idx}" "DDL" "ok" "phase=ddl" "${ddl}"
    col=$(echo "${ddl}" | sed -n 's/.*ADD COLUMN `\([^`]*\)`.*/\1/p')
    if [[ -n "${col}" ]]; then
      echo "${col}" >> "${NEW_COLS_FILE}"
      echo "${col}" >> "${DDL_EXPECTED_COLS_FILE}"
    fi
    dropped_col=$(echo "${ddl}" | sed -n 's/.*DROP COLUMN `\([^`]*\)`.*/\1/p')
    if [[ -n "${dropped_col}" && -f "${DDL_EXPECTED_COLS_FILE}" ]]; then
      grep -vx "${dropped_col}" "${DDL_EXPECTED_COLS_FILE}" > "${DDL_EXPECTED_COLS_FILE}.tmp" || true
      mv "${DDL_EXPECTED_COLS_FILE}.tmp" "${DDL_EXPECTED_COLS_FILE}"
    fi
  else
    DDL_FAIL=$((DDL_FAIL + 1))
    append_status "ddl" "${stmt_idx}" "DDL" "fail" "phase=ddl" "${ddl}"
  fi
done < "${DDL_SQL}"

sleep "${EFFECTIVE_WAIT_SYNC}"

DDL_SYNC_OK=0
DDL_SYNC_FAIL=0
if [[ "${SINK_SQL_ENABLED}" == "1" && -f "${DDL_EXPECTED_COLS_FILE}" ]]; then
  read -r DDL_SYNC_OK DDL_SYNC_FAIL <<< "$(wait_sink_expected_columns "${DDL_EXPECTED_COLS_FILE}" "${EFFECTIVE_DDL_SYNC_TIMEOUT}")"
fi

append_report "DDL executed: success=${DDL_OK}, failed=${DDL_FAIL}"
append_report "DDL schema sync check: synced=${DDL_SYNC_OK}, not_synced=${DDL_SYNC_FAIL}"
step_timer_end

append_report ""
append_report "[Step 6] Mixed phase: interleave DDL(drop/add) and DML with realtime status"
step_timer_start "Step 6 Mixed phase"

SCHEMA_CACHE_COLUMNS_CSV=""
SCHEMA_CACHE_DML_SPEC=""
refresh_schema_cache_from_source || true

MIX_DML_OK=0
MIX_DML_FAIL=0
MIX_DDL_OK=0
MIX_DDL_FAIL=0
MIX_NEW_COLS_FILE="${REPORT_DIR}/mixed_new_columns.txt"
MIX_EXPECTED_COLS_FILE="${REPORT_DIR}/mixed_expected_columns.txt"
rm -f "${MIX_NEW_COLS_FILE}" "${MIX_EXPECTED_COLS_FILE}" "${MIXED_SQL}"
MIX_DML_POOL=()
MIX_DML_POOL_POS=0

for i in $(seq 1 "${MIXED_COUNT}"); do
  choose=$((RANDOM % 100))
  if [[ ${choose} -lt ${MIXED_DDL_RATIO} ]]; then
    existing_cols=$(get_source_columns_csv)
    stmt=$("${PYTHON_BIN}" "${SCRIPT_DIR}/ddl_generator.py" \
      --count 1 \
      --seed "$((BASE_SEED + 200000 + i))" \
      --type alter_mixed \
      --table-name "${TABLE}" \
      --existing-cols "${existing_cols}" \
      --protected-cols "c0,c4" \
      $( [[ "${DDL_ENABLE_MODIFY}" == "1" ]] && echo "--enable-modify" ) \
      --output-format sql 2>/dev/null | awk 'NF && $0 !~ /^--/ {print; exit}')
    if [[ -z "${stmt}" ]]; then
      continue
    fi
    if [[ "${MIXED_RECORD_SQL}" == "1" ]]; then
      echo "${stmt}" >> "${MIXED_SQL}"
    fi

    if printf '%s\n' "${stmt}" | mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" >/dev/null 2>&1; then
      MIX_DDL_OK=$((MIX_DDL_OK + 1))
      update_schema_cache_for_ddl "${stmt}"
      # Schema changed: invalidate pre-generated DML pool and rebuild on next DML turn.
      MIX_DML_POOL=()
      MIX_DML_POOL_POS=0
      append_status "mixed" "${i}" "DDL" "ok" "phase=mixed" "${stmt}"
      added_col=$(echo "${stmt}" | sed -n 's/.*ADD COLUMN `\([^`]*\)`.*/\1/p')
      if [[ -n "${added_col}" ]]; then
        echo "${added_col}" >> "${MIX_NEW_COLS_FILE}"
        echo "${added_col}" >> "${MIX_EXPECTED_COLS_FILE}"
      fi
      dropped_col=$(echo "${stmt}" | sed -n 's/.*DROP COLUMN `\([^`]*\)`.*/\1/p')
      if [[ -n "${dropped_col}" && -f "${MIX_EXPECTED_COLS_FILE}" ]]; then
        grep -vx "${dropped_col}" "${MIX_EXPECTED_COLS_FILE}" > "${MIX_EXPECTED_COLS_FILE}.tmp" || true
        mv "${MIX_EXPECTED_COLS_FILE}.tmp" "${MIX_EXPECTED_COLS_FILE}"
      fi
    else
      MIX_DDL_FAIL=$((MIX_DDL_FAIL + 1))
      append_status "mixed" "${i}" "DDL" "fail" "phase=mixed" "${stmt}"
    fi
  else
    if [[ ${#MIX_DML_POOL[@]} -eq 0 || ${MIX_DML_POOL_POS} -ge ${#MIX_DML_POOL[@]} ]]; then
      refill_mixed_dml_pool "$((BASE_SEED + 300000 + i))"
    fi

    if [[ ${#MIX_DML_POOL[@]} -eq 0 || ${MIX_DML_POOL_POS} -ge ${#MIX_DML_POOL[@]} ]]; then
      continue
    fi

    stmt="${MIX_DML_POOL[${MIX_DML_POOL_POS}]}"
    MIX_DML_POOL_POS=$((MIX_DML_POOL_POS + 1))
    if [[ -z "${stmt}" ]]; then
      continue
    fi
    if [[ "${MIXED_RECORD_SQL}" == "1" ]]; then
      echo "${stmt}" >> "${MIXED_SQL}"
    fi

    if printf '%s\n' "${stmt}" | mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" >/dev/null 2>&1; then
      MIX_DML_OK=$((MIX_DML_OK + 1))
      append_status "mixed" "${i}" "DML" "ok" "phase=mixed" "${stmt}"
    else
      MIX_DML_FAIL=$((MIX_DML_FAIL + 1))
      append_status "mixed" "${i}" "DML" "fail" "phase=mixed" "${stmt}"
    fi
  fi
done

sleep "${EFFECTIVE_WAIT_SYNC}"

MIX_DDL_SYNC_OK=0
MIX_DDL_SYNC_FAIL=0
if [[ "${SINK_SQL_ENABLED}" == "1" && -f "${MIX_EXPECTED_COLS_FILE}" ]]; then
  read -r MIX_DDL_SYNC_OK MIX_DDL_SYNC_FAIL <<< "$(wait_sink_expected_columns "${MIX_EXPECTED_COLS_FILE}" "${EFFECTIVE_DDL_SYNC_TIMEOUT}")"
fi

MYSQL_COUNT_1=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};")
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  SINK_COUNT_1=$(mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};")
else
  SINK_COUNT_1="NA"
fi
MYSQL_COL_COUNT=$(get_source_column_count)
SINK_COL_COUNT=$(get_sink_column_count)
FINAL_JOB_STATE="UNKNOWN"
if [[ -n "${PIPELINE_JOB_ID:-}" ]]; then
  FINAL_JOB_STATE=$(get_job_state "${PIPELINE_JOB_ID}" || echo "UNKNOWN")
fi

append_report "Mixed DML success/fail: ${MIX_DML_OK}/${MIX_DML_FAIL}"
append_report "Mixed DDL success/fail: ${MIX_DDL_OK}/${MIX_DDL_FAIL}"
append_report "Mixed added-column sync (ok/fail): ${MIX_DDL_SYNC_OK}/${MIX_DDL_SYNC_FAIL}"
append_report "Realtime status file: ${STATUS_FILE}"

if ! wait_row_count_converged "${ROW_CONVERGE_RETRIES}"; then
  append_report "WARN: row count not converged within 90s after mixed phase"
fi
step_timer_end

append_report ""
append_report "[Step 7] Final source/sink state dump"
step_timer_start "Step 7 Final source/sink dump"

append_report ""
if [[ "${PRINT_SCHEMA_SNAPSHOT}" == "1" ]]; then
  append_report "--- MySQL DESC TABLE ---"
  mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "DESC ${TABLE};" | tee -a "${REPORT_FILE}"

  append_report ""
  append_report "--- MySQL SHOW CREATE TABLE ---"
  mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "SHOW CREATE TABLE ${TABLE};" | tee -a "${REPORT_FILE}"

  append_report ""
  if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
    append_report "--- ${SINK_LABEL} DESC TABLE ---"
    mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -e "DESC ${TABLE};" | tee -a "${REPORT_FILE}"

    append_report ""
    append_report "--- ${SINK_LABEL} SHOW CREATE TABLE ---"
    mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -e "SHOW CREATE TABLE ${TABLE};" | tee -a "${REPORT_FILE}"
  else
    append_report "--- ${SINK_LABEL} schema snapshot ---"
    append_report "Skipped: sink has no SQL endpoint in current mode"
  fi
fi

append_report ""
append_report "--- MySQL data snapshot (first 50 rows) ---"
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "SELECT * FROM ${TABLE} ORDER BY c0 LIMIT 50;" | tee -a "${REPORT_FILE}"

append_report ""
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  append_report "--- ${SINK_LABEL} data snapshot (first 50 rows) ---"
  mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -e "SELECT * FROM ${TABLE} ORDER BY c0 LIMIT 50;" | tee -a "${REPORT_FILE}"
else
  append_report "--- ${SINK_LABEL} data snapshot (first 50 rows) ---"
  append_report "Skipped: sink has no SQL endpoint in current mode"
fi

# Reconcile summary counters at the end to reduce false mismatches under heavy load.
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  FINAL_RECON_RETRIES=45
  if [[ "${AGGRESSIVE_BUG_TRIGGER}" == "1" ]]; then
    FINAL_RECON_RETRIES=40
  fi
  wait_row_count_converged "${FINAL_RECON_RETRIES}" >/dev/null 2>&1 || true
fi

MYSQL_COUNT_1=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};")
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  SINK_COUNT_1=$(mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};")
else
  SINK_COUNT_1="NA"
fi
MYSQL_COL_COUNT=$(get_source_column_count)
SINK_COL_COUNT=$(get_sink_column_count)
FINAL_JOB_STATE="UNKNOWN"
if [[ -n "${PIPELINE_JOB_ID:-}" ]]; then
  FINAL_JOB_STATE=$(get_job_state "${PIPELINE_JOB_ID}" || true)
fi
if [[ -z "${FINAL_JOB_STATE}" ]]; then
  FINAL_JOB_STATE=$(flink_api_get "/jobs/overview" | "${PYTHON_BIN}" -c 'import sys,json
try:
    d=json.load(sys.stdin)
    jobs=d.get("jobs",[])
    print(jobs[0].get("state","UNKNOWN") if jobs else "UNKNOWN")
except Exception:
    print("UNKNOWN")
')
fi

append_report ""
append_report "--- Summary ---"
append_report "DML success/fail: ${DML_OK}/${DML_FAIL}"
append_report "DDL success/fail: ${DDL_OK}/${DDL_FAIL}"
append_report "Mixed DML success/fail: ${MIX_DML_OK}/${MIX_DML_FAIL}"
append_report "Mixed DDL success/fail: ${MIX_DDL_OK}/${MIX_DDL_FAIL}"
append_report "Row count (MySQL/${SINK_LABEL}): ${MYSQL_COUNT_1}/${SINK_COUNT_1}"
append_report "Column count (MySQL/${SINK_LABEL}): ${MYSQL_COL_COUNT}/${SINK_COL_COUNT}"
append_report "Schema new columns synced (ok/fail): ${DDL_SYNC_OK}/${DDL_SYNC_FAIL}"
append_report "Final Flink job state: ${FINAL_JOB_STATE}"

MAIN_SOURCE_C4_TYPE=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -N -B -e "DESC ${TABLE} c4;" 2>/dev/null | awk -F'\t' '{print $2}' | head -n1 || true)
MAIN_SINK_C4_TYPE=""
MAIN_SINK_DECIMAL_FIELD="c4"
if [[ "${FLINK36741_MAIN_TRANSFORM}" == "1" ]]; then
  MAIN_SINK_DECIMAL_FIELD="deposits"
fi
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  MAIN_SINK_C4_TYPE=$(mysql -h 127.0.0.1 -P "${SINK_PORT}" -u root "${DATABASE}" -N -B -e "DESC ${TABLE};" 2>/dev/null | awk -F'\t' -v col="${MAIN_SINK_DECIMAL_FIELD}" '$1==col{print $2}' | head -n1 || true)
fi
append_report "Main-flow decimal check (source.c4 -> sink.${MAIN_SINK_DECIMAL_FIELD}): source=${MAIN_SOURCE_C4_TYPE:-UNKNOWN}, sink=${MAIN_SINK_C4_TYPE:-UNKNOWN}"
if echo "${MAIN_SOURCE_C4_TYPE}" | grep -Eqi 'decimal\(10,[[:space:]]*2\)' && echo "${MAIN_SINK_C4_TYPE}" | grep -Eqi 'decimal\(19,[[:space:]]*0\)'; then
  append_report "Main-flow decimal precision loss detected: YES"
else
  append_report "Main-flow decimal precision loss detected: NO"
fi

append_report "Finished at: $(timestamp)"
WORKFLOW_END_TS=$(date +%s)
append_report "Step timer sum seconds: ${TOTAL_STEP_SECONDS}"
append_report "Workflow elapsed seconds: $((WORKFLOW_END_TS - WORKFLOW_START_TS))"
append_report "Final report path: ${REPORT_FILE}"
step_timer_end

log "Workflow completed. Report: ${REPORT_FILE}"
