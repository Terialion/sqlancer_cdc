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
SOURCE_TYPE="${SOURCE_TYPE:-auto}"
DATABASE="${DATABASE:-database0}"
TABLE="${TABLE:-t0}"
SINK_TYPE="${SINK_TYPE:-auto}"
WAIT_SYNC="${WAIT_SYNC:-10}"
ENABLE_STATUS_LOG="${ENABLE_STATUS_LOG:-1}"
EXEC_DML_BATCH="${EXEC_DML_BATCH:-1}"
EXEC_DDL_BATCH="${EXEC_DDL_BATCH:-0}"
STATUS_ROW_SAMPLE_EVERY="${STATUS_ROW_SAMPLE_EVERY:-10}"
STATUS_INCLUDE_SQL="${STATUS_INCLUDE_SQL:-0}"
ENABLE_SELECT_PHASE="${ENABLE_SELECT_PHASE:-0}"
MIXED_RECORD_SQL="${MIXED_RECORD_SQL:-0}"
AGGRESSIVE_BUG_TRIGGER="${AGGRESSIVE_BUG_TRIGGER:-0}"
FLINK36741_MAIN_TRANSFORM="${FLINK36741_MAIN_TRANSFORM:-0}"
PIPELINE_PATCH_SCRIPT="${PIPELINE_PATCH_SCRIPT:-}"
TRANSFORM_SOURCE_TABLE="${TRANSFORM_SOURCE_TABLE:-}"
TRANSFORM_PROJECTION="${TRANSFORM_PROJECTION:-}"
TRANSFORM_PROJECTION_MODE="${TRANSFORM_PROJECTION_MODE:-expand-all}"
TRANSFORM_FILTER="${TRANSFORM_FILTER:-}"
ENABLE_RANDOM_TRANSFORM="${ENABLE_RANDOM_TRANSFORM:-0}"
RANDOM_TRANSFORM_SEED="${RANDOM_TRANSFORM_SEED:-}"
TRANSFORM_EXPECTS_ROW_PARITY="${TRANSFORM_EXPECTS_ROW_PARITY:-auto}"
ENABLE_PQS_PRESENCE_PROBE="${ENABLE_PQS_PRESENCE_PROBE:-0}"
FORCE_DISABLE_TRANSFORM="${FORCE_DISABLE_TRANSFORM:-0}"
# Keep prophecy probe always on to broaden default coverage each run.
ENABLE_TRANSFORM_PROPHECY_PROBE="1"
TRANSFORM_PROPHECY_STRICT="${TRANSFORM_PROPHECY_STRICT:-0}"
TEST_FOCUS="${TEST_FOCUS:-default}"
BUG_CAMPAIGN_MODE="${BUG_CAMPAIGN_MODE:-off}"
BUG_CAMPAIGN_RANDOM_RATE="${BUG_CAMPAIGN_RANDOM_RATE:-45}"
BUG_CAMPAIGN_PROFILES="${BUG_CAMPAIGN_PROFILES:-bug36866,transform,timezone,schema}"
BUG_CAMPAIGN_FORCE_FOCUS="${BUG_CAMPAIGN_FORCE_FOCUS:-bug36866}"
FOCUS_TIME_ZONE="${FOCUS_TIME_ZONE:-Asia/Shanghai}"
FOCUS_TRANSFORM_MODE="0"
FOCUS_TIMEZONE_MODE="0"
FOCUS_SCHEMA_MODE="0"
FOCUS_36866_MODE="0"
ENABLE_TIME_COLUMNS="${ENABLE_TIME_COLUMNS:-0}"
PQS_CLEAN_RESTART="${PQS_CLEAN_RESTART:-0}"
PQS_WAIT_SECONDS="${PQS_WAIT_SECONDS:-1}"
PQS_WAIT_RETRIES="${PQS_WAIT_RETRIES:-30}"
PRINT_SCHEMA_SNAPSHOT="${PRINT_SCHEMA_SNAPSHOT:-0}"
PRINT_DATA_SNAPSHOT="${PRINT_DATA_SNAPSHOT:-0}"
# Timeout for waiting sink table creation after pipeline submission
WAIT_TABLE_TIMEOUT="${WAIT_TABLE_TIMEOUT:-60}"
# DML statements to generate and execute
DML_COUNT="${DML_COUNT:-80}"
DML_COMPLEX_WHERE="${DML_COMPLEX_WHERE:-1}"
# DDL statements to generate and execute
DDL_COUNT="${DDL_COUNT:-16}"
# DDL generation mode: alter_add or alter_mixed
DDL_MODE="${DDL_MODE:-alter_mixed}"
DDL_ENABLE_MODIFY="${DDL_ENABLE_MODIFY:-auto}"
# DROP COLUMN ratio in pure DDL phase when DDL_MODE=alter_mixed
DDL_DROP_RATIO="${DDL_DROP_RATIO:-35}"
# Statements for mixed DDL+DML phase after pure DDL phase
MIXED_COUNT="${MIXED_COUNT:-60}"
# Percentage of DDL in mixed phase (0-100)
MIXED_DDL_RATIO="${MIXED_DDL_RATIO:-35}"
MIX_DML_POOL_REFILL_SIZE="${MIX_DML_POOL_REFILL_SIZE:-15}"
MIX_DDL_POOL_REFILL_SIZE="${MIX_DDL_POOL_REFILL_SIZE:-8}"
DDL_SYNC_TIMEOUT="${DDL_SYNC_TIMEOUT:-60}"
MIX_DDL_SYNC_TIMEOUT="${MIX_DDL_SYNC_TIMEOUT:-10}"
COLUMN_SYNC_STABLE_WINDOW="${COLUMN_SYNC_STABLE_WINDOW:-5}"
ROW_CONVERGE_RETRIES="${ROW_CONVERGE_RETRIES:-16}"
BASE_SEED="${BASE_SEED:-42}"
REPORT_DIR_FROM_ENV="${REPORT_DIR:-}"
REPORT_DIR="${REPORT_DIR:-/tmp/cdc_sqlancer_${BASE_SEED}}"
CANCEL_OLD_JOBS="${CANCEL_OLD_JOBS:-1}"
AUTO_RECOVER_CONTAINERS="${AUTO_RECOVER_CONTAINERS:-1}"
AUTO_RESOURCE_RECOVERY_RETRIES="${AUTO_RESOURCE_RECOVERY_RETRIES:-2}"
AUTO_RESOURCE_RECOVERY_DELAY="${AUTO_RESOURCE_RECOVERY_DELAY:-3}"
AUTO_RESOURCE_RECOVERY_POST_WAIT="${AUTO_RESOURCE_RECOVERY_POST_WAIT:-8}"
BATCH_EXCLUDE_NORESOURCE_ROUNDS="${BATCH_EXCLUDE_NORESOURCE_ROUNDS:-1}"
BATCH_HEALTH_MIN_SUCCESS_ROUNDS="${BATCH_HEALTH_MIN_SUCCESS_ROUNDS:-1}"
ROUNDS="${ROUNDS:-1}"
SEED_STEP="${SEED_STEP:-1}"
BATCH_ROUND_INDEX="${BATCH_ROUND_INDEX:-1}"
BATCH_ROUND1_EXTRA_SYNC_WAIT="${BATCH_ROUND1_EXTRA_SYNC_WAIT:-20}"
BATCH_ROUND1_EXTRA_SCHEMA_SYNC_TIMEOUT="${BATCH_ROUND1_EXTRA_SCHEMA_SYNC_TIMEOUT:-20}"
SUMMARY_FILE_FROM_ENV="${SUMMARY_FILE:-}"
SUMMARY_FILE="${SUMMARY_FILE:-/tmp/cdc_sqlancer_batch_${BASE_SEED}.txt}"
IN_BATCH_MODE="${IN_BATCH_MODE:-0}"
SELF_SCRIPT="${SCRIPT_DIR}/$(basename "${BASH_SOURCE[0]}")"
TRANSFORM_ADVISOR_SCRIPT="${SCRIPT_DIR}/tools/transform_support_advisor.py"

print_usage() {
  cat <<EOF
Usage: ./run_sqlancer_cdc_e2e.sh [options]

Options:
  -h, --help                    Show this help and exit
  --pipeline-yaml PATH          Pipeline yaml file (default: ${PIPELINE_YAML})
  --source-type TYPE            Source type override, e.g. mysql/postgres (default: ${SOURCE_TYPE})
  --sink-type TYPE              Sink type, e.g. doris/paimon/kafka (default: ${SINK_TYPE})
  --base-seed N                 Base random seed (default: ${BASE_SEED})
  --rounds N                    Batch rounds (default: ${ROUNDS})
  --seed-step N                 Seed increment per round (default: ${SEED_STEP})
  --wait-sync N                 Wait seconds before checks (default: ${WAIT_SYNC})
  --dml-count N                 DML statement count (default: ${DML_COUNT})
  --ddl-count N                 DDL statement count (default: ${DDL_COUNT})
  --mixed-count N               Mixed statement count (default: ${MIXED_COUNT})
  --report-dir PATH             Report output directory
  --aggressive                  Enable aggressive trigger profile
  --pipeline-patch-script PATH  External script to patch pipeline yaml before submit
  --transform-source-table TBL  Built-in transform source table regex (default: DATABASE\\.TABLE$)
  --transform-projection EXPR   Built-in transform projection expression
  --transform-projection-mode M Transform projection mode: strict|expand-all (default: ${TRANSFORM_PROJECTION_MODE})
  --transform-filter EXPR       Built-in transform filter expression (optional)
  --random-transform            Auto-generate transform projection/filter by seed
  --disable-transform           Force disable transform projection/filter/random/prophecy for baseline sync check
  --random-transform-seed N     Seed for random transform generation (default: base-seed+20000)
  --enable-transform-prophecy-probe
                                Inject one prophecy row and assert sink presence/absence under transform
  --transform-prophecy-strict   Enable strict value parity check for prophecy row
  --enable-pqs-presence-probe   Run PQS absent->present probe at workflow end
  --test-focus LIST             Focus profile list: transform,timezone,schema,bug36866 (comma-separated)
  --bug-campaign-mode MODE      Bug campaign mode: off|random|focus (default: ${BUG_CAMPAIGN_MODE})
  --bug-campaign-random-rate N  Random focus activation rate [0-100] (default: ${BUG_CAMPAIGN_RANDOM_RATE})
  --bug-campaign-profiles LIST  Candidate focus profiles for random mode (default: ${BUG_CAMPAIGN_PROFILES})
  --bug-campaign-force-focus F  Fixed focus profile for focus mode (default: ${BUG_CAMPAIGN_FORCE_FOCUS})
  --focus-time-zone TZ          Time zone for timezone focus (default: ${FOCUS_TIME_ZONE})

Examples:
  ./run_sqlancer_cdc_e2e.sh --pipeline-yaml pipeline-definition-doris.yaml --sink-type doris
  ./run_sqlancer_cdc_e2e.sh --base-seed 111 --dml-count 120 --ddl-count 20 --mixed-count 80
  ./run_sqlancer_cdc_e2e.sh --transform-projection "c0, c1, c4 as deposits"
  ./run_sqlancer_cdc_e2e.sh --test-focus transform,timezone --focus-time-zone Asia/Shanghai
EOF
}

INITIAL_BASE_SEED="${BASE_SEED}"
CLI_SET_REPORT_DIR="0"
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      print_usage
      exit 0
      ;;
    --pipeline-yaml)
      PIPELINE_YAML="$2"
      shift 2
      ;;
    --sink-type)
      SINK_TYPE="$2"
      shift 2
      ;;
    --source-type)
      SOURCE_TYPE="$2"
      shift 2
      ;;
    --base-seed)
      BASE_SEED="$2"
      shift 2
      ;;
    --rounds)
      ROUNDS="$2"
      shift 2
      ;;
    --seed-step)
      SEED_STEP="$2"
      shift 2
      ;;
    --wait-sync)
      WAIT_SYNC="$2"
      shift 2
      ;;
    --dml-count)
      DML_COUNT="$2"
      shift 2
      ;;
    --ddl-count)
      DDL_COUNT="$2"
      shift 2
      ;;
    --mixed-count)
      MIXED_COUNT="$2"
      shift 2
      ;;
    --report-dir)
      REPORT_DIR="$2"
      CLI_SET_REPORT_DIR="1"
      shift 2
      ;;
    --aggressive)
      AGGRESSIVE_BUG_TRIGGER="1"
      shift
      ;;
    --pipeline-patch-script)
      PIPELINE_PATCH_SCRIPT="$2"
      shift 2
      ;;
    --transform-source-table)
      TRANSFORM_SOURCE_TABLE="$2"
      shift 2
      ;;
    --transform-projection)
      TRANSFORM_PROJECTION="$2"
      shift 2
      ;;
    --transform-projection-mode)
      TRANSFORM_PROJECTION_MODE="$2"
      shift 2
      ;;
    --transform-filter)
      TRANSFORM_FILTER="$2"
      shift 2
      ;;
    --random-transform)
      ENABLE_RANDOM_TRANSFORM="1"
      shift
      ;;
    --disable-transform)
      FORCE_DISABLE_TRANSFORM="1"
      shift
      ;;
    --random-transform-seed)
      RANDOM_TRANSFORM_SEED="$2"
      shift 2
      ;;
    --enable-transform-prophecy-probe)
      ENABLE_TRANSFORM_PROPHECY_PROBE="1"
      shift
      ;;
    --transform-prophecy-strict)
      TRANSFORM_PROPHECY_STRICT="1"
      shift
      ;;
    --enable-pqs-presence-probe)
      ENABLE_PQS_PRESENCE_PROBE="1"
      shift
      ;;
    --test-focus)
      TEST_FOCUS="$2"
      shift 2
      ;;
    --bug-campaign-mode)
      BUG_CAMPAIGN_MODE="$2"
      shift 2
      ;;
    --bug-campaign-random-rate)
      BUG_CAMPAIGN_RANDOM_RATE="$2"
      shift 2
      ;;
    --bug-campaign-profiles)
      BUG_CAMPAIGN_PROFILES="$2"
      shift 2
      ;;
    --bug-campaign-force-focus)
      BUG_CAMPAIGN_FORCE_FOCUS="$2"
      shift 2
      ;;
    --focus-time-zone)
      FOCUS_TIME_ZONE="$2"
      shift 2
      ;;
    *)
      echo "ERROR: unknown option: $1" >&2
      print_usage >&2
      exit 2
      ;;
  esac
done

if [[ "${CLI_SET_REPORT_DIR}" != "1" && -z "${REPORT_DIR_FROM_ENV}" && "${BASE_SEED}" != "${INITIAL_BASE_SEED}" ]]; then
  REPORT_DIR="/tmp/cdc_sqlancer_${BASE_SEED}"
fi
if [[ -z "${SUMMARY_FILE_FROM_ENV}" && "${BASE_SEED}" != "${INITIAL_BASE_SEED}" ]]; then
  SUMMARY_FILE="/tmp/cdc_sqlancer_batch_${BASE_SEED}.txt"
fi

configure_test_focus() {
  local normalized
  normalized=$(echo "${TEST_FOCUS}" | tr '[:upper:]' '[:lower:]' | tr -d ' ')

  FOCUS_TRANSFORM_MODE="0"
  FOCUS_TIMEZONE_MODE="0"
  FOCUS_SCHEMA_MODE="0"
  FOCUS_36866_MODE="0"

  if [[ "${normalized}" == "default" || -z "${normalized}" ]]; then
    return 0
  fi

  if echo "${normalized}" | tr ',' '\n' | grep -Fxq "transform"; then
    FOCUS_TRANSFORM_MODE="1"
  fi
  if echo "${normalized}" | tr ',' '\n' | grep -Fxq "timezone"; then
    FOCUS_TIMEZONE_MODE="1"
  fi
  if echo "${normalized}" | tr ',' '\n' | grep -Fxq "schema"; then
    FOCUS_SCHEMA_MODE="1"
  fi
  if echo "${normalized}" | tr ',' '\n' | grep -Fxq "bug36866"; then
    FOCUS_36866_MODE="1"
  fi

  if [[ "${FOCUS_TRANSFORM_MODE}" == "1" ]]; then
    if [[ "${TABLE}" == "t0" ]]; then
      TABLE="t0_focus_${BASE_SEED}"
    fi
    if [[ -z "${TRANSFORM_PROJECTION}" && "${ENABLE_RANDOM_TRANSFORM}" != "1" ]]; then
      # FLINK-36741-like path: alias decimal column under transform.
      FLINK36741_MAIN_TRANSFORM="1"
    fi
    ENABLE_PQS_PRESENCE_PROBE="1"
  fi

  if [[ "${FOCUS_TIMEZONE_MODE}" == "1" ]]; then
    ENABLE_TIME_COLUMNS="1"
  fi

  if [[ "${FOCUS_SCHEMA_MODE}" == "1" ]]; then
    (( DDL_COUNT < 16 )) && DDL_COUNT=16
    (( MIXED_DDL_RATIO < 40 )) && MIXED_DDL_RATIO=40
  fi
  if [[ "${FOCUS_36866_MODE}" == "1" ]]; then
    if [[ "${TABLE}" == "t0" ]]; then
      TABLE="t0_focus_36866_${BASE_SEED}"
    fi
    ENABLE_RANDOM_TRANSFORM="0"
    TRANSFORM_SOURCE_TABLE="${DATABASE}.${TABLE}"
    TRANSFORM_PROJECTION="c0, CAST(ROUND(c4,0) AS INT) as c4_int_probe, CHAR_LENGTH(c3) as c3_len_probe"
    TRANSFORM_FILTER=""
    TRANSFORM_EXPECTS_ROW_PARITY="0"
    AGGRESSIVE_BUG_TRIGGER="1"
    ENABLE_PQS_PRESENCE_PROBE="1"
  fi
}

pick_campaign_focus_by_seed() {
  local seed="$1"
  local profiles_norm
  local rate
  local trigger
  local profiles=()
  local count
  local index
  local mode

  mode=$(echo "${BUG_CAMPAIGN_MODE}" | tr '[:upper:]' '[:lower:]' | tr -d ' ')
  case "${mode}" in
    off|"")
      echo "${TEST_FOCUS}"
      return 0
      ;;
    focus)
      echo "${BUG_CAMPAIGN_FORCE_FOCUS}"
      return 0
      ;;
    random)
      ;;
    *)
      echo "${TEST_FOCUS}"
      return 0
      ;;
  esac

  rate="${BUG_CAMPAIGN_RANDOM_RATE}"
  if ! [[ "${rate}" =~ ^[0-9]+$ ]]; then
    rate=45
  fi
  (( rate < 0 )) && rate=0
  (( rate > 100 )) && rate=100

  trigger=$((seed % 100))
  if (( trigger >= rate )); then
    echo "default"
    return 0
  fi

  profiles_norm=$(echo "${BUG_CAMPAIGN_PROFILES}" | tr '[:upper:]' '[:lower:]' | tr -d ' ')
  IFS=',' read -r -a profiles <<< "${profiles_norm}"
  count=${#profiles[@]}
  if (( count == 0 )); then
    echo "default"
    return 0
  fi

  index=$(((seed / 7 + 3) % count))
  if [[ -z "${profiles[$index]}" ]]; then
    echo "default"
    return 0
  fi
  echo "${profiles[$index]}"
}

resolve_campaign_focus_once() {
  local selected

  selected=$(pick_campaign_focus_by_seed "${BASE_SEED}")
  if [[ -n "${selected}" ]]; then
    TEST_FOCUS="${selected}"
  fi
}

apply_pipeline_local_timezone() {
  local input_yaml="$1"
  local output_yaml="$2"
  local tz="$3"

  awk -v tz="${tz}" '
    BEGIN { in_pipeline=0; inserted=0 }
    {
      if ($0 ~ /^pipeline:[[:space:]]*$/) {
        in_pipeline=1
        print
        next
      }
      if (in_pipeline==1) {
        if ($0 ~ /^[[:space:]]+local-time-zone:[[:space:]]*/) {
          print "  local-time-zone: \"" tz "\""
          inserted=1
          next
        }
        if ($0 ~ /^[^[:space:]].*:[[:space:]]*$/) {
          if (inserted==0) {
            print "  local-time-zone: \"" tz "\""
            inserted=1
          }
          in_pipeline=0
        }
      }
      print
    }
    END {
      if (in_pipeline==1 && inserted==0) {
        print "  local-time-zone: \"" tz "\""
      }
    }
  ' "${input_yaml}" > "${output_yaml}"
}

pin_source_tables_in_pipeline() {
  local input_yaml="$1"
  local output_yaml="$2"
  local table_pattern="$3"

  awk -v table_pattern="${table_pattern}" '
    BEGIN { in_source=0 }
    {
      if ($0 ~ /^source:[[:space:]]*$/) {
        in_source=1
        print
        next
      }
      if (in_source==1 && $0 ~ /^[^[:space:]].*:[[:space:]]*$/) {
        in_source=0
      }
      if (in_source==1 && $0 ~ /^[[:space:]]+tables:[[:space:]]*/) {
        print "  tables: \"" table_pattern "\""
        next
      }
      print
    }
  ' "${input_yaml}" > "${output_yaml}"
}

resolve_campaign_focus_once
configure_test_focus

mkdir -p "${REPORT_DIR}"
REPORT_FILE="${REPORT_DIR}/source_sink_final_state.txt"
PIPELINE_LOG="${REPORT_DIR}/pipeline_submit.log"
RUNTIME_PIPELINE_YAML="${REPORT_DIR}/pipeline_mainflow_flink36741.yaml"
RUNTIME_PIPELINE_TZ_YAML="${REPORT_DIR}/pipeline_mainflow_timezone.yaml"
SUBMITTED_PIPELINE_ARCHIVE="${REPORT_DIR}/submitted_pipeline.yaml"
SUBMITTED_TRANSFORM_SECTION="${REPORT_DIR}/submitted_pipeline.transform.txt"
DML_SQL="${REPORT_DIR}/phase_dml.sql"
DDL_SQL="${REPORT_DIR}/phase_ddl.sql"
SELECT_SQL="${REPORT_DIR}/phase_select.sql"
MIXED_SQL="${REPORT_DIR}/phase_mixed.sql"
STATUS_FILE="${REPORT_DIR}/realtime_status.log"
PQS_PROBE_LOG="${REPORT_DIR}/pqs_presence_probe.log"
SCHEMA_PQS_VALIDATOR_LOG="${REPORT_DIR}/schema_pqs_validator.log"
EXPERIMENT_ARCHIVE_FILE="${REPORT_DIR}/experiment_archive.txt"
SCHEMA_PQS_SLEEP_SECONDS="${SCHEMA_PQS_SLEEP_SECONDS:-5}"
SCHEMA_PQS_TRIALS="${SCHEMA_PQS_TRIALS:-3}"
SCHEMA_PQS_RETRIES="${SCHEMA_PQS_RETRIES:-5}"
SCHEMA_PQS_RETRY_DELAY="${SCHEMA_PQS_RETRY_DELAY:-1}"
TRANSFORM_PROPHECY_WAIT_SECONDS="${TRANSFORM_PROPHECY_WAIT_SECONDS:-10}"
TRANSFORM_PROPHECY_ROWS="${TRANSFORM_PROPHECY_ROWS:-3}"
TRANSFORM_PROPHECY_SAMPLE_MAX="${TRANSFORM_PROPHECY_SAMPLE_MAX:-3}"
TRANSFORM_PROPHECY_EDGE_CASES="${TRANSFORM_PROPHECY_EDGE_CASES:-2}"
TRANSFORM_PROPHECY_EXTRA_TRIES_MAX="${TRANSFORM_PROPHECY_EXTRA_TRIES_MAX:-2}"

PROPHECY_INITIALIZED="0"
declare -a PROPHECY_CASES
PROPHECY_EVIDENCE_LOG=""

EFFECTIVE_WAIT_SYNC="${WAIT_SYNC}"
EFFECTIVE_WAIT_TABLE_TIMEOUT="${WAIT_TABLE_TIMEOUT}"
EFFECTIVE_DDL_SYNC_TIMEOUT="${DDL_SYNC_TIMEOUT}"
EFFECTIVE_MIX_DDL_SYNC_TIMEOUT="${MIX_DDL_SYNC_TIMEOUT}"
SINK_SQL_RETRY_COUNT="${SINK_SQL_RETRY_COUNT:-8}"
SINK_SQL_RETRY_DELAY="${SINK_SQL_RETRY_DELAY:-1}"
RANDOM_TRANSFORM_SEED_EFFECTIVE=""

# Keep behavior but reduce waiting overhead for quick bug reproduction loops.
if [[ "${EFFECTIVE_WAIT_SYNC}" -gt 2 ]]; then
  EFFECTIVE_WAIT_SYNC="2"
fi
if [[ "${EFFECTIVE_WAIT_TABLE_TIMEOUT}" -gt 90 ]]; then
  EFFECTIVE_WAIT_TABLE_TIMEOUT="45"
fi
if [[ "${EFFECTIVE_DDL_SYNC_TIMEOUT}" -gt 45 ]]; then
  EFFECTIVE_DDL_SYNC_TIMEOUT="15"
fi
if [[ "${EFFECTIVE_MIX_DDL_SYNC_TIMEOUT}" -gt 20 ]]; then
  EFFECTIVE_MIX_DDL_SYNC_TIMEOUT="8"
fi
if [[ "${ROW_CONVERGE_RETRIES}" -gt 40 ]]; then
  ROW_CONVERGE_RETRIES="24"
fi
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

detect_source_type_from_yaml() {
  local yaml_path="$1"
  awk '
    /^source:[[:space:]]*$/ { in_source=1; next }
    in_source && /^[^[:space:]]/ { in_source=0 }
    in_source && $1 == "type:" { print $2; exit }
  ' "${yaml_path}" | tr -d "\"'"
}

resolve_yaml_path() {
  local yaml_path="$1"
  if [[ -f "${yaml_path}" ]]; then
    printf '%s\n' "${yaml_path}"
    return 0
  fi
  if [[ -f "${SCRIPT_DIR}/${yaml_path}" ]]; then
    printf '%s\n' "${SCRIPT_DIR}/${yaml_path}"
    return 0
  fi
  return 1
}

extract_transform_section() {
  local yaml_path="$1"
  local out_path="$2"
  awk '
    BEGIN { in_transform=0; found=0 }
    {
      if ($0 ~ /^transform:[[:space:]]*$/) {
        in_transform=1
        found=1
        print
        next
      }
      if (in_transform==1) {
        if ($0 ~ /^[^[:space:]].*:[[:space:]]*$/) {
          in_transform=0
          exit
        }
        print
      }
    }
    END {
      if (found==0) {
        print "<no transform section in submitted pipeline>"
      }
    }
  ' "${yaml_path}" > "${out_path}"
}

archive_and_report_submitted_pipeline() {
  local submit_yaml="$1"
  local resolved

  resolved=$(resolve_yaml_path "${submit_yaml}" || true)
  if [[ -z "${resolved}" ]]; then
    append_report "WARN: cannot resolve submitted pipeline path: ${submit_yaml}"
    return 1
  fi

  cp "${resolved}" "${SUBMITTED_PIPELINE_ARCHIVE}"
  extract_transform_section "${SUBMITTED_PIPELINE_ARCHIVE}" "${SUBMITTED_TRANSFORM_SECTION}"

  append_report "Submitted pipeline archive: ${SUBMITTED_PIPELINE_ARCHIVE}"
  append_report "Submitted transform section: ${SUBMITTED_TRANSFORM_SECTION}"
  append_report "--- Submitted transform section ---"
  cat "${SUBMITTED_TRANSFORM_SECTION}" | tee -a "${REPORT_FILE}"
  append_report "--- End submitted transform section ---"
}

report_transform_support_advice() {
  local source_type="$1"
  local sink_type="$2"
  local projection="$3"
  local filter_expr="$4"

  if [[ ! -f "${TRANSFORM_ADVISOR_SCRIPT}" ]]; then
    return 0
  fi

  set +e
  local advice
  advice=$("${PYTHON_BIN}" "${TRANSFORM_ADVISOR_SCRIPT}" \
    --source-type "${source_type}" \
    --sink-type "${sink_type}" \
    --projection "${projection}" \
    --filter "${filter_expr}" 2>/dev/null)
  local rc=$?
  set -e

  if [[ ${rc} -eq 0 && -n "${advice}" ]]; then
    append_report "--- Transform support advisor ---"
    printf '%s\n' "${advice}" | tee -a "${REPORT_FILE}"
    append_report "--- End transform support advisor ---"
  fi
}

normalize_projection_expand_all() {
  local projection_expr="$1"
  "${PYTHON_BIN}" - <<'PY' "${projection_expr}"
import sys

expr = sys.argv[1] if len(sys.argv) > 1 else ""
parts = [p.strip() for p in expr.split(",") if p.strip()]
base = {"c0", "c1", "c2", "c3", "c4", "*", "\\*"}
kept = []
for p in parts:
  head = p.split()[0].lower()
  if head in base:
    continue
  kept.append(p)

if kept:
  print("\\*, " + ", ".join(kept))
else:
  print("\\*")
PY
}

configure_sink_runtime() {
  local detected
  local source_detected
  detected="$(detect_sink_type_from_yaml "${SCRIPT_DIR}/${PIPELINE_YAML}")"
  if [[ "${SINK_TYPE}" == "auto" || -z "${SINK_TYPE}" ]]; then
    SINK_TYPE="${detected:-doris}"
  fi
  SINK_TYPE="$(echo "${SINK_TYPE}" | tr '[:upper:]' '[:lower:]')"

  source_detected="$(detect_source_type_from_yaml "${SCRIPT_DIR}/${PIPELINE_YAML}")"
  if [[ "${SOURCE_TYPE}" == "auto" || -z "${SOURCE_TYPE}" ]]; then
    SOURCE_TYPE="${source_detected:-mysql}"
  fi
  SOURCE_TYPE="$(echo "${SOURCE_TYPE}" | tr '[:upper:]' '[:lower:]')"

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

generate_random_transform() {
  local seed
  local projection_pick
  local filter_pick

  if [[ -n "${RANDOM_TRANSFORM_SEED}" ]]; then
    seed="${RANDOM_TRANSFORM_SEED}"
  else
    seed=$((BASE_SEED + 20000))
  fi
  RANDOM_TRANSFORM_SEED_EFFECTIVE="${seed}"

  RANDOM="${seed}"
  projection_pick=$((RANDOM % 16))
  filter_pick=$((RANDOM % 5))

  case "${projection_pick}" in
    0) TRANSFORM_PROJECTION="c0, c4" ;;
    1) TRANSFORM_PROJECTION="c0, c4, ABS(c4) as c4_abs" ;;
    2) TRANSFORM_PROJECTION="c0, c4, c0 as pivot_key" ;;
    3) TRANSFORM_PROJECTION="c0, c4, c0 + 1 as c0_plus" ;;
    4) TRANSFORM_PROJECTION="c0, c4, ABS(c0) as c0_abs" ;;
    5) TRANSFORM_PROJECTION="c0, c4, CHAR_LENGTH(c3) as c3_len_probe" ;;
    6) TRANSFORM_PROJECTION="c0, c4, CHAR_LENGTH(c1) as c1_len_probe" ;;
    7) TRANSFORM_PROJECTION="c0, c4, CAST(ROUND(c4,0) AS INT) as c4_int_probe" ;;
    8) TRANSFORM_PROJECTION="c0, c4, ROUND(c4, 2) as c4_round2" ;;
    9) TRANSFORM_PROJECTION="c0, c4, FLOOR(c4) as c4_floor" ;;
    10) TRANSFORM_PROJECTION="c0, c4, UPPER(c3) as c3_upper" ;;
    11) TRANSFORM_PROJECTION="c0, c4, UPPER(c3) as c3_upper, CHAR_LENGTH(c3) as c3_len_probe" ;;
    12) TRANSFORM_PROJECTION="c0, c4, UPPER(COALESCE(c3, '')) as c3_upper_safe" ;;
    13) TRANSFORM_PROJECTION="c0, c4, CONCAT(c1, '_suffix') as c1_tagged" ;;
    14) TRANSFORM_PROJECTION="c0, c4, COALESCE(c3, 'missing') as c3_filled" ;;
    15) TRANSFORM_PROJECTION="c0, c4, CASE WHEN c0 >= 0 THEN c0 ELSE 0 - c0 END as c0_abs_case" ;;
  esac

  case "${filter_pick}" in
    0) TRANSFORM_FILTER="c0 >= 0" ;;
    1) TRANSFORM_FILTER="c0 >= 1" ;;
    2) TRANSFORM_FILTER="c0 <> 0" ;;
    3) TRANSFORM_FILTER="c0 BETWEEN -50000 AND 50000" ;;
    4) TRANSFORM_FILTER="c0 <= 20000" ;;
  esac

  if [[ "${TRANSFORM_EXPECTS_ROW_PARITY}" == "auto" ]]; then
    TRANSFORM_EXPECTS_ROW_PARITY="0"
  fi
}

prepare_transform_runtime_flags() {
  if [[ "${FORCE_DISABLE_TRANSFORM}" == "1" ]]; then
    ENABLE_RANDOM_TRANSFORM="0"
    FLINK36741_MAIN_TRANSFORM="0"
    TRANSFORM_PROJECTION=""
    TRANSFORM_FILTER=""
    ENABLE_TRANSFORM_PROPHECY_PROBE="0"
    ENABLE_PQS_PRESENCE_PROBE="0"
    TRANSFORM_EXPECTS_ROW_PARITY="1"
    append_report "Transform mode: force disabled by --disable-transform"
    return 0
  fi

  if [[ "${ENABLE_RANDOM_TRANSFORM}" == "1" && -z "${TRANSFORM_PROJECTION}" ]]; then
    generate_random_transform
    append_report "Random transform mode: enabled"
    append_report "Random transform projection: ${TRANSFORM_PROJECTION}"
    append_report "Random transform filter: ${TRANSFORM_FILTER}"
  fi

  if [[ "${ENABLE_RANDOM_TRANSFORM}" == "1" && "${ENABLE_PQS_PRESENCE_PROBE}" != "1" ]]; then
    ENABLE_PQS_PRESENCE_PROBE="1"
    append_report "Random transform mode: auto-enabled PQS presence probe"
  fi

  TRANSFORM_PROJECTION_MODE="$(echo "${TRANSFORM_PROJECTION_MODE}" | tr '[:upper:]' '[:lower:]' | tr -d ' ')"
  if [[ "${TRANSFORM_PROJECTION_MODE}" != "strict" && "${TRANSFORM_PROJECTION_MODE}" != "expand-all" ]]; then
    TRANSFORM_PROJECTION_MODE="expand-all"
  fi

  if [[ -n "${TRANSFORM_PROJECTION}" && "${TRANSFORM_PROJECTION_MODE}" == "expand-all" ]]; then
    if ! echo "${TRANSFORM_PROJECTION}" | grep -Eq '[*]'; then
      TRANSFORM_PROJECTION="$(normalize_projection_expand_all "${TRANSFORM_PROJECTION}")"
      append_report "Transform projection mode: expand-all (wildcard + deduplicated extras)"
    fi
  fi

  if [[ "${TRANSFORM_EXPECTS_ROW_PARITY}" == "auto" ]]; then
    if [[ -n "${TRANSFORM_FILTER}" ]]; then
      TRANSFORM_EXPECTS_ROW_PARITY="0"
    else
      TRANSFORM_EXPECTS_ROW_PARITY="1"
    fi
  fi
}

init_transform_prophecy_row() {
  if [[ "${ENABLE_TRANSFORM_PROPHECY_PROBE}" != "1" ]]; then
    return 0
  fi

  local seed
  local i
  local extra_tries
  local expected
  local match_count
  local prophecy_c0
  local prophecy_c1
  local prophecy_c2
  local prophecy_c3
  local prophecy_c4
  local prophecy_c4_raw
  local seen_present
  local seen_absent
  local edge_idx
  local edge_slot
  local sample_max
  local sample_count
  local pool_size
  local pick_idx
  local case_line
  local sampled_cases=()
  local pool_cases=()
  local log_idx
  local edge_c4
  local edge_c3

  seed=$((BASE_SEED + 910000))
  RANDOM="${seed}"
  PROPHECY_CASES=()
  seen_present=0
  seen_absent=0
  PROPHECY_EVIDENCE_LOG="${REPORT_DIR}/transform_prophecy_evidence.log"
  : > "${PROPHECY_EVIDENCE_LOG}"

  for i in $(seq 1 "${TRANSFORM_PROPHECY_ROWS}"); do
    prophecy_c0=$((100000000 + (RANDOM % 1000000) + i))
    prophecy_c1="prophecy_${BASE_SEED}_${prophecy_c0}"
    prophecy_c2=$((RANDOM % 2000 - 1000))
    prophecy_c3="oracle_${RANDOM}"
    prophecy_c4_raw=$((RANDOM % 100000))
    prophecy_c4=$(printf '%d.%02d' "$((prophecy_c4_raw / 100))" "$((prophecy_c4_raw % 100))")

    mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "
INSERT INTO ${TABLE} (c0, c1, c2, c3, c4)
VALUES (${prophecy_c0}, '${prophecy_c1}', ${prophecy_c2}, '${prophecy_c3}', ${prophecy_c4})
ON DUPLICATE KEY UPDATE c1=VALUES(c1), c2=VALUES(c2), c3=VALUES(c3), c4=VALUES(c4);
" >/dev/null 2>&1

    if [[ -n "${TRANSFORM_FILTER}" ]]; then
      match_count=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "
SELECT COUNT(*) FROM ${TABLE} WHERE c0=${prophecy_c0} AND (${TRANSFORM_FILTER});
" 2>/dev/null || echo "0")
      if [[ "${match_count}" =~ ^[0-9]+$ ]] && [[ "${match_count}" -gt 0 ]]; then
        expected="1"
      else
        expected="0"
      fi
    else
      expected="1"
    fi

    if [[ "${expected}" == "1" ]]; then
      seen_present=1
    else
      seen_absent=1
    fi

    PROPHECY_CASES+=("${prophecy_c0}|${expected}|${prophecy_c4}")
  done

  # Inject a small number of deterministic cast edge rows to widen bug-trigger surface.
  for edge_idx in $(seq 1 "${TRANSFORM_PROPHECY_EDGE_CASES}"); do
    edge_slot=$((((edge_idx - 1) % 4) + 1))
    case "${edge_slot}" in
      1)
        edge_c4="21474836.47"
        edge_c3="2147483647"
        ;;
      2)
        edge_c4="-21474836.48"
        edge_c3="-2147483648"
        ;;
      3)
        edge_c4="99999.99"
        edge_c3="2147483648"
        ;;
      *)
        edge_c4="-99999.99"
        edge_c3="-2147483649"
        ;;
    esac

    prophecy_c0=$((120000000 + (RANDOM % 1000000) + edge_idx))
    prophecy_c1="prophecy_edge_${BASE_SEED}_${prophecy_c0}"
    prophecy_c2=$((RANDOM % 2000 - 1000))

    mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "
INSERT INTO ${TABLE} (c0, c1, c2, c3, c4)
VALUES (${prophecy_c0}, '${prophecy_c1}', ${prophecy_c2}, '${edge_c3}', ${edge_c4})
ON DUPLICATE KEY UPDATE c1=VALUES(c1), c2=VALUES(c2), c3=VALUES(c3), c4=VALUES(c4);
" >/dev/null 2>&1

    if [[ -n "${TRANSFORM_FILTER}" ]]; then
      match_count=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "
SELECT COUNT(*) FROM ${TABLE} WHERE c0=${prophecy_c0} AND (${TRANSFORM_FILTER});
" 2>/dev/null || echo "0")
      if [[ "${match_count}" =~ ^[0-9]+$ ]] && [[ "${match_count}" -gt 0 ]]; then
        expected="1"
      else
        expected="0"
      fi
    else
      expected="1"
    fi

    if [[ "${expected}" == "1" ]]; then
      seen_present=1
    else
      seen_absent=1
    fi

    PROPHECY_CASES+=("${prophecy_c0}|${expected}|${edge_c4}")
  done

  extra_tries=0
  while [[ -n "${TRANSFORM_FILTER}" && ${extra_tries} -lt ${TRANSFORM_PROPHECY_EXTRA_TRIES_MAX} && ( ${seen_present} -eq 0 || ${seen_absent} -eq 0 ) ]]; do
    extra_tries=$((extra_tries + 1))
    prophecy_c0=$((110000000 + (RANDOM % 1000000) + extra_tries))
    prophecy_c1="prophecy_ext_${BASE_SEED}_${prophecy_c0}"
    prophecy_c2=$((RANDOM % 2000 - 1000))
    prophecy_c3="oracle_ext_${RANDOM}"
    prophecy_c4_raw=$((RANDOM % 100000))
    prophecy_c4=$(printf '%d.%02d' "$((prophecy_c4_raw / 100))" "$((prophecy_c4_raw % 100))")

    mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "
INSERT INTO ${TABLE} (c0, c1, c2, c3, c4)
VALUES (${prophecy_c0}, '${prophecy_c1}', ${prophecy_c2}, '${prophecy_c3}', ${prophecy_c4})
ON DUPLICATE KEY UPDATE c1=VALUES(c1), c2=VALUES(c2), c3=VALUES(c3), c4=VALUES(c4);
" >/dev/null 2>&1

    match_count=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "
SELECT COUNT(*) FROM ${TABLE} WHERE c0=${prophecy_c0} AND (${TRANSFORM_FILTER});
" 2>/dev/null || echo "0")
    if [[ "${match_count}" =~ ^[0-9]+$ ]] && [[ "${match_count}" -gt 0 ]]; then
      expected="1"
      seen_present=1
    else
      expected="0"
      seen_absent=1
    fi

    PROPHECY_CASES+=("${prophecy_c0}|${expected}|${prophecy_c4}")
  done

  sample_max="${TRANSFORM_PROPHECY_SAMPLE_MAX}"
  if ! [[ "${sample_max}" =~ ^[0-9]+$ ]] || [[ "${sample_max}" -le 0 ]]; then
    sample_max=6
  fi
  sample_count=${#PROPHECY_CASES[@]}
  if [[ ${sample_count} -gt ${sample_max} ]]; then
    RANDOM=$((BASE_SEED + 930000))
    pool_cases=("${PROPHECY_CASES[@]}")
    sampled_cases=()
    while [[ ${#sampled_cases[@]} -lt ${sample_max} && ${#pool_cases[@]} -gt 0 ]]; do
      pool_size=${#pool_cases[@]}
      pick_idx=$((RANDOM % pool_size))
      sampled_cases+=("${pool_cases[${pick_idx}]}")
      unset 'pool_cases[pick_idx]'
      pool_cases=("${pool_cases[@]}")
    done
    PROPHECY_CASES=("${sampled_cases[@]}")
  fi

  log_idx=0
  for case_line in "${PROPHECY_CASES[@]}"; do
    log_idx=$((log_idx + 1))
    IFS='|' read -r prophecy_c0 expected prophecy_c4 <<< "${case_line}"
    append_report "Transform prophecy row[${log_idx}]: c0=${prophecy_c0}, expect_present=${expected}, c4=${prophecy_c4}"
  done

  PROPHECY_INITIALIZED="1"
  append_report "Transform prophecy probe: enabled (cases=${#PROPHECY_CASES[@]}, sample_max=${sample_max})"
}

run_transform_prophecy_probe() {
  if [[ "${ENABLE_TRANSFORM_PROPHECY_PROBE}" != "1" || "${PROPHECY_INITIALIZED}" != "1" ]]; then
    return 0
  fi

  if [[ "${SINK_SQL_ENABLED}" != "1" ]]; then
    append_report "Transform prophecy probe skipped: sink has no SQL endpoint"
    return 0
  fi

  local sink_count
  local retries
  local case_line
  local c0
  local expected
  local c4_expected
  local src_v
  local sink_v
  local passed
  local failed

  retries="${TRANSFORM_PROPHECY_WAIT_SECONDS}"
  passed=0
  failed=0

  for case_line in "${PROPHECY_CASES[@]}"; do
    IFS='|' read -r c0 expected c4_expected <<< "${case_line}"

    if [[ "${expected}" == "1" ]]; then
      sink_count="0"
      for _ in $(seq 1 "${retries}"); do
        sink_count=$(sink_mysql_exec "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE} WHERE c0=${c0};" 2>/dev/null || echo "0")
        if [[ "${sink_count}" =~ ^[0-9]+$ ]] && [[ "${sink_count}" -gt 0 ]]; then
          break
        fi
        sleep 1
      done

      if [[ ! "${sink_count}" =~ ^[0-9]+$ ]] || [[ "${sink_count}" -eq 0 ]]; then
        append_report "Transform prophecy probe FAILED: expected row c0=${c0} to appear in sink, but not found"
        echo "FAIL|c0=${c0}|expect=1|sink_count=${sink_count}" >> "${PROPHECY_EVIDENCE_LOG}"
        failed=$((failed + 1))
        continue
      fi

      if [[ "${TRANSFORM_PROPHECY_STRICT}" == "1" ]]; then
        src_v=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT CAST(c4 AS DECIMAL(38,2)) FROM ${TABLE} WHERE c0=${c0} LIMIT 1;" 2>/dev/null || echo "")
        sink_v=$(sink_mysql_exec "${DATABASE}" -sN -e "SELECT CAST(c4 AS DECIMAL(38,2)) FROM ${TABLE} WHERE c0=${c0} LIMIT 1;" 2>/dev/null || echo "")
        if [[ -n "${src_v}" && -n "${sink_v}" && "${src_v}" != "${sink_v}" ]]; then
          append_report "Transform prophecy probe FAILED(strict): c4 mismatch for c0=${c0}, source=${src_v}, sink=${sink_v}"
          echo "FAIL|c0=${c0}|expect=1|strict_c4_mismatch|source=${src_v}|sink=${sink_v}" >> "${PROPHECY_EVIDENCE_LOG}"
          failed=$((failed + 1))
          continue
        fi
      fi

      echo "PASS|c0=${c0}|expect=1|sink_count=${sink_count}" >> "${PROPHECY_EVIDENCE_LOG}"
      passed=$((passed + 1))
      continue
    fi

    sleep "${retries}"
    sink_count=$(sink_mysql_exec "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE} WHERE c0=${c0};" 2>/dev/null || echo "0")
    if [[ "${sink_count}" =~ ^[0-9]+$ ]] && [[ "${sink_count}" -gt 0 ]]; then
      append_report "Transform prophecy probe FAILED: expected row c0=${c0} to be filtered out, but found in sink"
      echo "FAIL|c0=${c0}|expect=0|sink_count=${sink_count}" >> "${PROPHECY_EVIDENCE_LOG}"
      failed=$((failed + 1))
      continue
    fi

    echo "PASS|c0=${c0}|expect=0|sink_count=${sink_count}" >> "${PROPHECY_EVIDENCE_LOG}"
    passed=$((passed + 1))
  done

  append_report "Transform prophecy probe summary: pass=${passed}, fail=${failed}, evidence=${PROPHECY_EVIDENCE_LOG}"
  if [[ ${failed} -gt 0 ]]; then
    return 1
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

job_has_no_resource_exception() {
  local jid="$1"
  [[ -z "${jid}" ]] && return 1
  local exc
  exc=$(flink_api_get "/jobs/${jid}/exceptions")
  [[ -z "${exc}" ]] && return 1
  printf '%s' "${exc}" | grep -q 'NoResourceAvailableException'
}

recover_flink_resources() {
  append_report "Auto-recover Flink resources: restarting jobmanager/taskmanager"
  set +e
  docker compose up -d jobmanager taskmanager >/dev/null 2>&1
  local rc=$?
  set -e
  if [[ ${rc} -ne 0 ]]; then
    append_report "WARN: docker compose up -d jobmanager taskmanager failed (rc=${rc}), fallback to ./cdcup.sh up"
    set +e
    (cd "${SCRIPT_DIR}" && ./cdcup.sh up >/dev/null 2>&1)
    rc=$?
    set -e
  fi
  if [[ ${rc} -ne 0 ]]; then
    append_report "ERROR: resource auto-recover failed"
    return 1
  fi
  if ! wait_flink_ready 90; then
    append_report "ERROR: Flink cluster not ready after auto-recover"
    return 1
  fi
  sleep "${AUTO_RESOURCE_RECOVERY_DELAY}"
  RESOURCE_RECOVERED_RECENTLY="1"
  return 0
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

is_sink_transient_error() {
  local msg="$1"
  echo "${msg}" | grep -Eqi 'ERROR 1105|cancelled|timeout|timed out|temporarily unavailable|try again|server has gone away|lost connection|connection reset|EOFException'
}

sink_mysql_exec() {
  local db="${1:-}"
  shift || true

  local attempt
  local rc=0
  local out=""
  local err=""
  local err_file
  local args=(-h 127.0.0.1 -P "${SINK_PORT}" -u root)

  if [[ -n "${db}" ]]; then
    args+=("${db}")
  fi
  args+=("$@")

  for attempt in $(seq 1 "${SINK_SQL_RETRY_COUNT}"); do
    err_file=$(mktemp)
    set +e
    out=$(mysql "${args[@]}" 2>"${err_file}")
    rc=$?
    set -e
    err=$(cat "${err_file}")
    rm -f "${err_file}"

    if [[ ${rc} -eq 0 ]]; then
      [[ -n "${out}" ]] && printf '%s\n' "${out}"
      return 0
    fi

    if is_sink_transient_error "${err}" && [[ ${attempt} -lt ${SINK_SQL_RETRY_COUNT} ]]; then
      sleep "${SINK_SQL_RETRY_DELAY}"
      continue
    fi

    echo "${err}" >&2
    return ${rc}
  done

  echo "ERROR: sink mysql command failed after ${SINK_SQL_RETRY_COUNT} retries" >&2
  return 1
}

wait_row_count_converged() {
  local retries="${1:-60}"
  local i

  if [[ "${TRANSFORM_EXPECTS_ROW_PARITY}" != "1" ]]; then
    return 0
  fi

  if [[ "${SINK_SQL_ENABLED}" != "1" ]]; then
    return 0
  fi

  for i in $(seq 1 "${retries}"); do
    local s_count
    local d_count
    s_count=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
    if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
      d_count=$(sink_mysql_exec "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
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
  local sink_cols
  for i in $(seq 1 "${retries}"); do
    sink_cols=$(sink_mysql_exec "${DATABASE}" -sN -e "DESC ${TABLE};" 2>/dev/null | awk '{print $1}' || true)
    if [[ -n "${sink_cols}" ]] && echo "${sink_cols}" | grep -Fxq "${col_name}"; then
      return 0
    fi
    sleep 1
  done
  return 1
}

wait_sink_expected_columns() {
  local expected_file="$1"
  local retries="${2:-60}"
  local stable_window="${3:-${COLUMN_SYNC_STABLE_WINDOW}}"

  if [[ "${SINK_SQL_ENABLED}" != "1" || ! -f "${expected_file}" ]]; then
    echo "0 0"
    return 0
  fi

  local total=0
  local pending=0
  local i
  local sink_cols
  local col
  local last_pending=-1
  local stable_hits=0

  total=$(sort -u "${expected_file}" | sed '/^$/d' | wc -l | tr -d ' ')
  if [[ -z "${total}" || "${total}" == "0" ]]; then
    echo "0 0"
    return 0
  fi

  pending="${total}"
  for i in $(seq 1 "${retries}"); do
    sink_cols=$(sink_mysql_exec "${DATABASE}" -sN -e "DESC ${TABLE};" 2>/dev/null | awk '{print $1}' || true)
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

      if [[ "${pending}" -eq "${last_pending}" ]]; then
        stable_hits=$((stable_hits + 1))
      else
        stable_hits=0
        last_pending="${pending}"
      fi

      if [[ "${stable_window}" -gt 0 && ${stable_hits} -ge ${stable_window} ]]; then
        break
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
      STATUS_LAST_SINK_COUNT=$(sink_mysql_exec "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
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

count_effective_sql_statements() {
  local sql_file="$1"
  awk 'NF && $0 !~ /^--/ {c++} END {print c+0}' "${sql_file}"
}

execute_sql_file_batch_mysql() {
  local db="$1"
  local sql_file="$2"
  local total
  local fail_count
  local ok_count
  local rc=0
  local err_file

  total=$(count_effective_sql_statements "${sql_file}")
  err_file=$(mktemp)

  set +e
  mysql --force -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${db}" < "${sql_file}" >/dev/null 2>"${err_file}"
  rc=$?
  set -e

  fail_count=$(grep -c '^ERROR ' "${err_file}" || true)
  rm -f "${err_file}"

  if [[ "${fail_count}" -gt "${total}" ]]; then
    fail_count="${total}"
  fi
  ok_count=$((total - fail_count))

  # If mysql returns non-zero without parseable per-statement errors, treat as a hard failure.
  if [[ ${rc} -ne 0 && "${fail_count}" -eq 0 && "${total}" -gt 0 ]]; then
    ok_count=0
    fail_count="${total}"
  fi

  echo "${ok_count} ${fail_count}"
}

write_experiment_archive() {
  {
    echo "timestamp=$(timestamp)"
    echo "base_seed=${BASE_SEED}"
    echo "pipeline_yaml=${PIPELINE_YAML}"
    echo "source_type=${SOURCE_TYPE}"
    echo "sink_type=${SINK_TYPE}"
    echo "report_dir=${REPORT_DIR}"
    echo "submitted_pipeline_archive=${SUBMITTED_PIPELINE_ARCHIVE}"
    echo "submitted_transform_section=${SUBMITTED_TRANSFORM_SECTION}"
    echo "random_transform_enabled=${ENABLE_RANDOM_TRANSFORM}"
    echo "random_transform_seed=${RANDOM_TRANSFORM_SEED_EFFECTIVE:-NA}"
    echo "transform_projection=${TRANSFORM_PROJECTION:-NA}"
    echo "transform_filter=${TRANSFORM_FILTER:-NA}"
    echo "transform_expects_row_parity=${TRANSFORM_EXPECTS_ROW_PARITY}"
    echo "test_focus=${TEST_FOCUS}"
    echo "focus_time_zone=${FOCUS_TIME_ZONE}"
    echo "dml_count=${DML_COUNT}"
    echo "ddl_count=${DDL_COUNT}"
    echo "mixed_count=${MIXED_COUNT}"
    echo "pqs_probe_enabled=${ENABLE_PQS_PRESENCE_PROBE}"
    echo "pqs_probe_log=${PQS_PROBE_LOG}"
    echo "schema_pqs_validator_log=${SCHEMA_PQS_VALIDATOR_LOG}"
  } > "${EXPERIMENT_ARCHIVE_FILE}"
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
  sink_mysql_exec "" -sN -e "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema='${DATABASE}' AND table_name='${TABLE}';" 2>/dev/null || echo "0"
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
    "${PYTHON_BIN}" "${SCRIPT_DIR}/generators/dml_generator.py" \
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

refill_mixed_ddl_pool() {
  local seed="$1"
  local existing_cols
  existing_cols=$(get_source_columns_csv)

  mapfile -t MIX_DDL_POOL < <(
    "${PYTHON_BIN}" "${SCRIPT_DIR}/generators/ddl_generator.py" \
      --count "${MIX_DDL_POOL_REFILL_SIZE}" \
      --seed "${seed}" \
      --type alter_mixed \
      --table-name "${TABLE}" \
      --existing-cols "${existing_cols}" \
      --protected-cols "c0,c4" \
      $( [[ "${DDL_ENABLE_MODIFY}" == "1" ]] && echo "--enable-modify" ) \
      --output-format sql 2>/dev/null | awk 'NF && $0 !~ /^--/'
  )
  MIX_DDL_POOL_POS=0
}

run_batch_mode_if_needed() {
  local total_rounds
  total_rounds=$((ROUNDS))

  if [[ "${IN_BATCH_MODE}" = "1" || ${total_rounds} -le 1 ]]; then
    return 0
  fi

  local r
  local round_seed
  local round_focus
  local round_report_dir
  local round_rc
  local round_noresource=0
  local excluded=0
  local ok=0
  local fail=0

  : > "${SUMMARY_FILE}"
  : > "${STATUS_FILE}"
  echo "Batch run started at: $(timestamp)" | tee -a "${SUMMARY_FILE}"
  echo "Rounds=${ROUNDS}, seed_start=${BASE_SEED}, seed_step=${SEED_STEP}" | tee -a "${SUMMARY_FILE}"
  echo | tee -a "${SUMMARY_FILE}"

  for ((r = 1; r <= total_rounds; r++)); do
    round_seed=$((BASE_SEED + (r - 1) * SEED_STEP))
    round_focus=$(pick_campaign_focus_by_seed "${round_seed}")
    round_report_dir="${REPORT_DIR}_round${r}"

    echo "=== ROUND ${r}/${total_rounds} seed=${round_seed} ===" | tee -a "${SUMMARY_FILE}"
    echo "round_focus=${round_focus}" | tee -a "${SUMMARY_FILE}"
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
    MIXED_COUNT="${MIXED_COUNT}" \
    MIXED_DDL_RATIO="${MIXED_DDL_RATIO}" \
    MIX_DML_POOL_REFILL_SIZE="${MIX_DML_POOL_REFILL_SIZE}" \
    MIX_DDL_POOL_REFILL_SIZE="${MIX_DDL_POOL_REFILL_SIZE}" \
    DDL_SYNC_TIMEOUT="${DDL_SYNC_TIMEOUT}" \
    MIX_DDL_SYNC_TIMEOUT="${MIX_DDL_SYNC_TIMEOUT}" \
    ROW_CONVERGE_RETRIES="${ROW_CONVERGE_RETRIES}" \
    ENABLE_TRANSFORM_PROPHECY_PROBE="${ENABLE_TRANSFORM_PROPHECY_PROBE}" \
    TRANSFORM_PROPHECY_ROWS="${TRANSFORM_PROPHECY_ROWS}" \
    TRANSFORM_PROPHECY_SAMPLE_MAX="${TRANSFORM_PROPHECY_SAMPLE_MAX}" \
    TRANSFORM_PROPHECY_EDGE_CASES="${TRANSFORM_PROPHECY_EDGE_CASES}" \
    TRANSFORM_PROPHECY_EXTRA_TRIES_MAX="${TRANSFORM_PROPHECY_EXTRA_TRIES_MAX}" \
    TRANSFORM_PROPHECY_WAIT_SECONDS="${TRANSFORM_PROPHECY_WAIT_SECONDS}" \
    TRANSFORM_PROPHECY_STRICT="${TRANSFORM_PROPHECY_STRICT}" \
    AGGRESSIVE_BUG_TRIGGER="${AGGRESSIVE_BUG_TRIGGER}" \
    TEST_FOCUS="${round_focus}" \
    BUG_CAMPAIGN_MODE="off" \
    BATCH_ROUND_INDEX="${r}" \
    FOCUS_TIME_ZONE="${FOCUS_TIME_ZONE}" \
    ENABLE_TIME_COLUMNS="${ENABLE_TIME_COLUMNS}" \
    PRINT_SCHEMA_SNAPSHOT="${PRINT_SCHEMA_SNAPSHOT}" \
    CANCEL_OLD_JOBS="${CANCEL_OLD_JOBS}" \
    "${SELF_SCRIPT}"
    round_rc=$?
    set -e

    round_noresource=0
    if [[ -f "${round_report_dir}/source_sink_final_state.txt" ]]; then
      if grep -q 'NoResourceAvailableException' "${round_report_dir}/source_sink_final_state.txt"; then
        round_noresource=1
      fi
    fi
    if [[ ${round_noresource} -eq 0 && -f "${round_report_dir}/pipeline_submit.log" ]]; then
      if grep -q 'NoResourceAvailableException' "${round_report_dir}/pipeline_submit.log"; then
        round_noresource=1
      fi
    fi

    if [[ ${round_rc} -eq 0 ]]; then
      ok=$((ok + 1))
    else
      if [[ "${BATCH_EXCLUDE_NORESOURCE_ROUNDS}" == "1" && ${round_noresource} -eq 1 ]]; then
        excluded=$((excluded + 1))
        echo "round_excluded_due_to_noresource=1" | tee -a "${SUMMARY_FILE}"
      else
        fail=$((fail + 1))
      fi
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
  echo "round_ok=${ok}, round_fail=${fail}, round_excluded=${excluded}" | tee -a "${SUMMARY_FILE}"
  if [[ ${ok} -lt ${BATCH_HEALTH_MIN_SUCCESS_ROUNDS} ]]; then
    echo "health_gate_failed=min_success_required=${BATCH_HEALTH_MIN_SUCCESS_ROUNDS}, actual_success=${ok}" | tee -a "${SUMMARY_FILE}"
    exit 1
  fi
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
  echo "Aggressive Trigger Mode: ${AGGRESSIVE_BUG_TRIGGER}"
  echo "Test Focus: ${TEST_FOCUS}"
  if [[ "${FOCUS_TIMEZONE_MODE}" == "1" ]]; then
    echo "Focus Time Zone: ${FOCUS_TIME_ZONE}"
  fi
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
SOURCE_TABLE_EXTRA_COLS=""
if [[ "${ENABLE_TIME_COLUMNS}" == "1" ]]; then
  SOURCE_TABLE_EXTRA_COLS=",\n  c5 TIMESTAMP NULL,\n  c6 DATETIME NULL"
fi
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "
DROP TABLE IF EXISTS ${TABLE};
CREATE TABLE ${TABLE} (
  c0 INT PRIMARY KEY,
  c1 VARCHAR(256) NOT NULL,
  c2 INT,
  c3 VARCHAR(256),
  c4 DECIMAL(10,2)${SOURCE_TABLE_EXTRA_COLS}
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

running_services=$(docker compose ps --status running --services 2>/dev/null || true)
if ! echo "${running_services}" | grep -Fxq "jobmanager"; then
  append_report "WARN: jobmanager not running before submission; try auto-recover"
  set +e
  (cd "${SCRIPT_DIR}" && ./cdcup.sh up >/dev/null 2>&1)
  RECOVER_RC=$?
  set -e
  if [[ ${RECOVER_RC} -ne 0 ]]; then
    append_report "ERROR: failed to recover services before submission"
    exit 1
  fi
  if ! wait_flink_ready 90; then
    append_report "ERROR: Flink cluster is not ready after recovery"
    exit 1
  fi
fi

prepare_transform_runtime_flags

if [[ "${ENABLE_TRANSFORM_PROPHECY_PROBE}" == "1" && -z "${TRANSFORM_PROJECTION}" ]]; then
  ENABLE_RANDOM_TRANSFORM="1"
  prepare_transform_runtime_flags
fi

init_transform_prophecy_row

PIPELINE_SUBMIT_PATH="${PIPELINE_YAML}"
if [[ "${FLINK36741_MAIN_TRANSFORM}" == "1" && -z "${TRANSFORM_PROJECTION}" ]]; then
  TRANSFORM_PROJECTION="c0, c1, c2, c3, c4 as deposits"
  TRANSFORM_SOURCE_TABLE="${TRANSFORM_SOURCE_TABLE:-${DATABASE}.${TABLE}}"
  append_report "Main-flow FLINK-36741 mode: enabled (transform alias c4 -> deposits)."
fi

if [[ -n "${TRANSFORM_PROJECTION}" ]]; then
  local_pipeline_input="${SCRIPT_DIR}/${PIPELINE_YAML}"
  if [[ ! -f "${local_pipeline_input}" ]]; then
    append_report "ERROR: pipeline yaml not found: ${local_pipeline_input}"
    exit 1
  fi
  transform_table="${TRANSFORM_SOURCE_TABLE:-${DATABASE}.${TABLE}}"
  awk '
    BEGIN { skip = 0 }
    {
      if (skip == 0 && $0 ~ /^transform:[[:space:]]*$/) {
        skip = 1
        next
      }
      if (skip == 1) {
        if ($0 ~ /^[^[:space:]].*:[[:space:]]*$/) {
          skip = 0
        } else {
          next
        }
      }
      print
    }
  ' "${local_pipeline_input}" > "${RUNTIME_PIPELINE_YAML}"

  cat >> "${RUNTIME_PIPELINE_YAML}" <<EOF
transform:
  - source-table: ${transform_table}
    projection: ${TRANSFORM_PROJECTION}
EOF
  if [[ -n "${TRANSFORM_FILTER}" ]]; then
    cat >> "${RUNTIME_PIPELINE_YAML}" <<EOF
    filter: ${TRANSFORM_FILTER}
EOF
  fi
  if [[ "${FOCUS_TRANSFORM_MODE}" == "1" || "${FLINK36741_MAIN_TRANSFORM}" == "1" ]]; then
    pin_source_tables_in_pipeline "${RUNTIME_PIPELINE_YAML}" "${RUNTIME_PIPELINE_YAML}.tmp" "${DATABASE}.${TABLE}"
    mv "${RUNTIME_PIPELINE_YAML}.tmp" "${RUNTIME_PIPELINE_YAML}"
  fi
  PIPELINE_SUBMIT_PATH="${RUNTIME_PIPELINE_YAML}"
  append_report "Built-in transform mode: enabled (projection from --transform-projection, sink=${SINK_TYPE})."
fi

if [[ -n "${PIPELINE_PATCH_SCRIPT}" ]]; then
  if [[ ! -x "${PIPELINE_PATCH_SCRIPT}" ]]; then
    append_report "ERROR: pipeline patch script is not executable: ${PIPELINE_PATCH_SCRIPT}"
    exit 1
  fi

  base_pipeline_input="${PIPELINE_SUBMIT_PATH}"
  set +e
  "${PIPELINE_PATCH_SCRIPT}" "${base_pipeline_input}" "${RUNTIME_PIPELINE_YAML}"
  PATCH_RC=$?
  set -e
  if [[ ${PATCH_RC} -ne 0 ]]; then
    append_report "ERROR: pipeline patch script failed with code ${PATCH_RC}"
    exit 1
  fi
  PIPELINE_SUBMIT_PATH="${RUNTIME_PIPELINE_YAML}"
  append_report "Pipeline patch mode: enabled via ${PIPELINE_PATCH_SCRIPT}"
fi

if [[ "${FOCUS_TIMEZONE_MODE}" == "1" ]]; then
  timezone_input_yaml="${PIPELINE_SUBMIT_PATH}"
  if [[ ! -f "${timezone_input_yaml}" ]]; then
    timezone_input_yaml="${SCRIPT_DIR}/${PIPELINE_SUBMIT_PATH}"
  fi
  apply_pipeline_local_timezone "${timezone_input_yaml}" "${RUNTIME_PIPELINE_TZ_YAML}" "${FOCUS_TIME_ZONE}"
  PIPELINE_SUBMIT_PATH="${RUNTIME_PIPELINE_TZ_YAML}"
  append_report "Timezone focus mode: enabled (pipeline.local-time-zone=${FOCUS_TIME_ZONE})"
  if [[ "${SINK_TYPE}" != "paimon" && "${SINK_TYPE}" != "starrocks" ]]; then
    append_report "WARN: timezone focus is strongest on paimon/starrocks sinks (current sink=${SINK_TYPE})."
  fi
fi

archive_and_report_submitted_pipeline "${PIPELINE_SUBMIT_PATH}" || true
report_transform_support_advice "${SOURCE_TYPE}" "${SINK_TYPE}" "${TRANSFORM_PROJECTION}" "${TRANSFORM_FILTER}"

submit_attempt=1
submit_max_attempts=$((AUTO_RESOURCE_RECOVERY_RETRIES + 1))
table_ready="false"
RESOURCE_RECOVERED_RECENTLY="0"

while [[ ${submit_attempt} -le ${submit_max_attempts} ]]; do
  append_report "Pipeline submit/wait attempt ${submit_attempt}/${submit_max_attempts}"

  if [[ "${RESOURCE_RECOVERED_RECENTLY}" == "1" ]]; then
    append_report "Post-recover stabilization wait: ${AUTO_RESOURCE_RECOVERY_POST_WAIT}s"
    sleep "${AUTO_RESOURCE_RECOVERY_POST_WAIT}"
    RESOURCE_RECOVERED_RECENTLY="0"
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
    if grep -q 'NoResourceAvailableException' "${PIPELINE_LOG}" && [[ ${submit_attempt} -lt ${submit_max_attempts} ]]; then
      append_report "Detected NoResourceAvailableException during submit, auto-recover and retry"
      recover_flink_resources || exit 1
      submit_attempt=$((submit_attempt + 1))
      continue
    fi
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

  append_report ""
  append_report "[Step 3] Wait for sink table creation in ${SINK_LABEL}"

  # Trigger at least one CDC event so sink table is materialized even when snapshot is empty.
  mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "
INSERT INTO ${TABLE} (c0, c1, c2, c3, c4) VALUES (1, 'bootstrap', 1, 'init', 1.0)
ON DUPLICATE KEY UPDATE c1='bootstrap', c2=1, c3='init', c4=1.0;
" >/dev/null 2>&1 || true

  table_ready="false"
  retry_due_to_no_resource="0"
  for i in $(seq 1 "${EFFECTIVE_WAIT_TABLE_TIMEOUT}"); do
    if [[ -n "${PIPELINE_JOB_ID:-}" ]]; then
      state=$(get_job_state "${PIPELINE_JOB_ID}" || true)
      if [[ "${state}" == "FAILED" || "${state}" == "CANCELED" || "${state}" == "SUSPENDED" ]]; then
        append_report "Pipeline job entered terminal state early: ${state}"
        append_job_exception "${PIPELINE_JOB_ID}"
        if job_has_no_resource_exception "${PIPELINE_JOB_ID}" && [[ ${submit_attempt} -lt ${submit_max_attempts} ]]; then
          append_report "Detected NoResourceAvailableException in job exceptions, auto-recover and retry"
          retry_due_to_no_resource="1"
        else
          exit 1
        fi
        break
      fi
    fi

    if [[ "${SINK_SQL_ENABLED}" != "1" ]]; then
      table_ready="true"
      break
    fi

    if sink_mysql_exec "" -sN -e "SHOW DATABASES LIKE '${DATABASE}';" 2>/dev/null | grep -q "${DATABASE}"; then
      if sink_mysql_exec "${DATABASE}" -sN -e "SHOW TABLES LIKE '${TABLE}';" 2>/dev/null | grep -q "${TABLE}"; then
        table_ready="true"
        break
      fi
    fi
    sleep 1
  done

  if [[ "${table_ready}" == "true" ]]; then
    break
  fi

  if [[ "${retry_due_to_no_resource}" != "1" && -n "${PIPELINE_JOB_ID:-}" ]] && job_has_no_resource_exception "${PIPELINE_JOB_ID}" && [[ ${submit_attempt} -lt ${submit_max_attempts} ]]; then
    append_report "Sink table wait timeout with NoResourceAvailableException, auto-recover and retry"
    retry_due_to_no_resource="1"
  fi

  if [[ "${retry_due_to_no_resource}" == "1" && ${submit_attempt} -lt ${submit_max_attempts} ]]; then
    recover_flink_resources || exit 1
    submit_attempt=$((submit_attempt + 1))
    continue
  fi

  append_report "Sink table not detected within ${EFFECTIVE_WAIT_TABLE_TIMEOUT}s."
  append_report "Try checking job logs and pipeline regex in ${PIPELINE_YAML}."
  append_job_exception "${PIPELINE_JOB_ID:-}"
  exit 1
done

if [[ "${table_ready}" != "true" ]]; then
  append_report "ERROR: failed to get sink table ready after ${submit_max_attempts} attempts"
  exit 1
fi

step_timer_end
append_report "Sink ready in ${SINK_LABEL}: ${DATABASE}.${TABLE}"

if ! run_transform_prophecy_probe; then
  exit 1
fi

append_report ""
append_report "[Step 4] DML phase: generate and execute on source, verify on sink"
step_timer_start "Step 4 DML phase"

"${PYTHON_BIN}" "${SCRIPT_DIR}/generators/dml_generator.py" \
  --count "${DML_COUNT}" \
  --seed "${BASE_SEED}" \
  $( [[ "${DML_COMPLEX_WHERE}" = "1" ]] && echo "--allow-complex-where" ) \
  --output-sql "${DML_SQL}" >/dev/null

DML_OK=0
DML_FAIL=0
if [[ "${EXEC_DML_BATCH}" == "1" ]]; then
  read -r DML_OK DML_FAIL <<< "$(execute_sql_file_batch_mysql "${DATABASE}" "${DML_SQL}")"
else
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
fi

sleep "${EFFECTIVE_WAIT_SYNC}"

MYSQL_COUNT_1=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};")
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  SINK_COUNT_1=$(sink_mysql_exec "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
else
  SINK_COUNT_1="NA"
fi

append_report "DML executed: success=${DML_OK}, failed=${DML_FAIL}"
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  append_report "Row count after DML: MySQL=${MYSQL_COUNT_1}, ${SINK_LABEL}=${SINK_COUNT_1}"
else
  append_report "Row count after DML: MySQL=${MYSQL_COUNT_1}, ${SINK_LABEL}=NA (non-SQL sink, parity skipped)"
fi

if [[ "${ENABLE_SELECT_PHASE}" == "1" ]]; then
  "${PYTHON_BIN}" "${SCRIPT_DIR}/generators/select_generator.py" \
    --count 10 \
    --seed "$((BASE_SEED + 7))" \
    --type all \
    --output-sql "${SELECT_SQL}" >/dev/null 2>&1 || true
fi
  step_timer_end

append_report ""
append_report "[Step 5] DDL phase: generate ALTER statements and verify schema sync"
  step_timer_start "Step 5 DDL phase"

"${PYTHON_BIN}" "${SCRIPT_DIR}/generators/ddl_generator.py" \
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

if [[ "${EXEC_DDL_BATCH}" == "1" ]]; then
  read -r DDL_OK DDL_FAIL <<< "$(execute_sql_file_batch_mysql "${DATABASE}" "${DDL_SQL}")"

  # Build expected sink column set from generated DDL intent for sync validation.
  while IFS= read -r ddl; do
    [[ -z "${ddl}" || "${ddl}" =~ ^-- ]] && continue
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
  done < "${DDL_SQL}"
else
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
fi

sleep "${EFFECTIVE_WAIT_SYNC}"

DDL_SYNC_OK=0
DDL_SYNC_FAIL=0
if [[ "${SINK_SQL_ENABLED}" == "1" && -f "${DDL_EXPECTED_COLS_FILE}" ]]; then
  read -r DDL_SYNC_OK DDL_SYNC_FAIL <<< "$(wait_sink_expected_columns "${DDL_EXPECTED_COLS_FILE}" "${EFFECTIVE_DDL_SYNC_TIMEOUT}")"
  if [[ "${IN_BATCH_MODE}" == "1" && "${BATCH_ROUND_INDEX}" == "1" && ${DDL_SYNC_FAIL} -gt 0 ]]; then
    append_report "DDL schema sync warmup retry: round1 extra wait ${BATCH_ROUND1_EXTRA_SCHEMA_SYNC_TIMEOUT}s"
    read -r DDL_SYNC_OK DDL_SYNC_FAIL <<< "$(wait_sink_expected_columns "${DDL_EXPECTED_COLS_FILE}" "$((EFFECTIVE_DDL_SYNC_TIMEOUT + BATCH_ROUND1_EXTRA_SCHEMA_SYNC_TIMEOUT))")"
  fi
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
MIX_DDL_POOL=()
MIX_DDL_POOL_POS=0

for i in $(seq 1 "${MIXED_COUNT}"); do
  choose=$((RANDOM % 100))
  if [[ ${choose} -lt ${MIXED_DDL_RATIO} ]]; then
    if [[ ${#MIX_DDL_POOL[@]} -eq 0 || ${MIX_DDL_POOL_POS} -ge ${#MIX_DDL_POOL[@]} ]]; then
      refill_mixed_ddl_pool "$((BASE_SEED + 200000 + i))"
    fi

    if [[ ${#MIX_DDL_POOL[@]} -eq 0 || ${MIX_DDL_POOL_POS} -ge ${#MIX_DDL_POOL[@]} ]]; then
      continue
    fi

    stmt="${MIX_DDL_POOL[${MIX_DDL_POOL_POS}]}"
    MIX_DDL_POOL_POS=$((MIX_DDL_POOL_POS + 1))
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
      # Fallback once with fresh schema to avoid stale-pool false failures.
      existing_cols=$(get_source_columns_csv)
      fallback_stmt=$("${PYTHON_BIN}" "${SCRIPT_DIR}/generators/ddl_generator.py" \
        --count 1 \
        --seed "$((BASE_SEED + 500000 + i))" \
        --type alter_mixed \
        --table-name "${TABLE}" \
        --existing-cols "${existing_cols}" \
        --protected-cols "c0,c4" \
        $( [[ "${DDL_ENABLE_MODIFY}" == "1" ]] && echo "--enable-modify" ) \
        --output-format sql 2>/dev/null | awk 'NF && $0 !~ /^--/ {print; exit}')

      if [[ -n "${fallback_stmt}" ]] && printf '%s\n' "${fallback_stmt}" | mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" >/dev/null 2>&1; then
        MIX_DDL_OK=$((MIX_DDL_OK + 1))
        update_schema_cache_for_ddl "${fallback_stmt}"
        MIX_DML_POOL=()
        MIX_DML_POOL_POS=0
        append_status "mixed" "${i}" "DDL" "ok" "phase=mixed:fallback" "${fallback_stmt}"
        added_col=$(echo "${fallback_stmt}" | sed -n 's/.*ADD COLUMN `\([^`]*\)`.*/\1/p')
        if [[ -n "${added_col}" ]]; then
          echo "${added_col}" >> "${MIX_NEW_COLS_FILE}"
          echo "${added_col}" >> "${MIX_EXPECTED_COLS_FILE}"
        fi
        dropped_col=$(echo "${fallback_stmt}" | sed -n 's/.*DROP COLUMN `\([^`]*\)`.*/\1/p')
        if [[ -n "${dropped_col}" && -f "${MIX_EXPECTED_COLS_FILE}" ]]; then
          grep -vx "${dropped_col}" "${MIX_EXPECTED_COLS_FILE}" > "${MIX_EXPECTED_COLS_FILE}.tmp" || true
          mv "${MIX_EXPECTED_COLS_FILE}.tmp" "${MIX_EXPECTED_COLS_FILE}"
        fi
      else
        MIX_DDL_FAIL=$((MIX_DDL_FAIL + 1))
        append_status "mixed" "${i}" "DDL" "fail" "phase=mixed" "${stmt}"
      fi
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
  read -r MIX_DDL_SYNC_OK MIX_DDL_SYNC_FAIL <<< "$(wait_sink_expected_columns "${MIX_EXPECTED_COLS_FILE}" "${EFFECTIVE_MIX_DDL_SYNC_TIMEOUT}")"
fi

MYSQL_COUNT_1=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};")
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  SINK_COUNT_1=$(sink_mysql_exec "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
else
  SINK_COUNT_1="NA"
fi

if [[ "${IN_BATCH_MODE}" == "1" && "${BATCH_ROUND_INDEX}" == "1" && "${SINK_SQL_ENABLED}" == "1" ]]; then
  if [[ "${MYSQL_COUNT_1}" =~ ^[0-9]+$ && "${SINK_COUNT_1}" =~ ^[0-9]+$ && ${MYSQL_COUNT_1} -gt 0 && ${SINK_COUNT_1} -eq 0 ]]; then
    append_report "Round1 warmup stabilization: sink row count is zero, extra wait ${BATCH_ROUND1_EXTRA_SYNC_WAIT}s before final summary"
    sleep "${BATCH_ROUND1_EXTRA_SYNC_WAIT}"
    wait_row_count_converged "$((ROW_CONVERGE_RETRIES + BATCH_ROUND1_EXTRA_SYNC_WAIT))" >/dev/null 2>&1 || true
    MYSQL_COUNT_1=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};")
    SINK_COUNT_1=$(sink_mysql_exec "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
  fi
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

if [[ "${TRANSFORM_EXPECTS_ROW_PARITY}" != "1" ]]; then
  append_report "Row count converge check skipped: transform filter may intentionally change sink cardinality"
elif ! wait_row_count_converged "${ROW_CONVERGE_RETRIES}"; then
  append_report "WARN: row count not converged within ${ROW_CONVERGE_RETRIES}s after mixed phase"
fi
step_timer_end

append_report ""
append_report "[Step 7] Unified schema type validation + PQS"
step_timer_start "Step 7 Unified schema type validation + PQS"

if [[ -n "${TRANSFORM_PROJECTION}" ]]; then
  append_report "Unified schema/PQS validator skipped: transform projection is enabled and may intentionally change sink column set"
  step_timer_end
else

  if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
    set +e
    "${PYTHON_BIN}" "${SCRIPT_DIR}/validators/cdc_schema_pqs_validator.py" \
      --mysql-port "${MYSQL_PORT}" \
      --sink-port "${SINK_PORT}" \
      --database "${DATABASE}" \
      --tables "${TABLE}" \
      --sleep-seconds "${SCHEMA_PQS_SLEEP_SECONDS}" \
      --pqs-trials-per-table "${SCHEMA_PQS_TRIALS}" \
      --sink-consistency-retries "${SCHEMA_PQS_RETRIES}" \
      --sink-consistency-delay "${SCHEMA_PQS_RETRY_DELAY}" \
      --seed "$((BASE_SEED + 700000))" \
      >"${SCHEMA_PQS_VALIDATOR_LOG}" 2>&1
    VALIDATOR_RC=$?
    set -e

    if [[ ${VALIDATOR_RC} -ne 0 ]]; then
      append_report "Unified schema/PQS validator failed (rc=${VALIDATOR_RC}). Log: ${SCHEMA_PQS_VALIDATOR_LOG}"
      tail -n 120 "${SCHEMA_PQS_VALIDATOR_LOG}" | tee -a "${REPORT_FILE}" || true
      exit 1
    fi

    append_report "Unified schema/PQS validator passed. Log: ${SCHEMA_PQS_VALIDATOR_LOG}"
    tail -n 80 "${SCHEMA_PQS_VALIDATOR_LOG}" | tee -a "${REPORT_FILE}" || true
  else
    append_report "Unified schema/PQS validator skipped: sink has no SQL endpoint"
  fi

  step_timer_end
fi

append_report ""
append_report "[Step 8] Final source/sink state dump"
step_timer_start "Step 8 Final source/sink dump"

append_report ""
append_report "--- MySQL DESC TABLE ---"
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "DESC ${TABLE};" | tee -a "${REPORT_FILE}"

# append_report ""
# append_report "--- MySQL SHOW CREATE TABLE ---"
# mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "SHOW CREATE TABLE ${TABLE};" | tee -a "${REPORT_FILE}"

append_report ""
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  append_report "--- ${SINK_LABEL} DESC TABLE ---"
  sink_mysql_exec "${DATABASE}" -e "DESC ${TABLE};" 2>/dev/null | tee -a "${REPORT_FILE}" || append_report "WARN: failed to dump ${SINK_LABEL} DESC ${TABLE}"

#  append_report ""
#  append_report "--- ${SINK_LABEL} SHOW CREATE TABLE ---"
#  sink_mysql_exec "${DATABASE}" -e "SHOW CREATE TABLE ${TABLE};" 2>/dev/null | tee -a "${REPORT_FILE}" || append_report "WARN: failed to dump ${SINK_LABEL} SHOW CREATE ${TABLE}"
else
  append_report "--- ${SINK_LABEL} schema snapshot ---"
  append_report "Skipped: sink has no SQL endpoint in current mode"
fi

append_report ""
append_report "--- MySQL data snapshot (first 10 rows) ---"
mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -e "SELECT * FROM ${TABLE} ORDER BY c0 LIMIT 10;" | tee -a "${REPORT_FILE}"

append_report ""
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  append_report "--- ${SINK_LABEL} data snapshot (first 10 rows) ---"
  sink_mysql_exec "${DATABASE}" -e "SELECT * FROM ${TABLE} ORDER BY c0 LIMIT 10;" 2>/dev/null | tee -a "${REPORT_FILE}" || append_report "WARN: failed to dump ${SINK_LABEL} data snapshot"
else
  append_report "--- ${SINK_LABEL} data snapshot (first 10 rows) ---"
  append_report "Skipped: sink has no SQL endpoint in current mode"
fi

# Reconcile summary counters at the end to reduce false mismatches under heavy load.
if [[ "${SINK_SQL_ENABLED}" == "1" && "${TRANSFORM_EXPECTS_ROW_PARITY}" == "1" ]]; then
  FINAL_RECON_RETRIES=45
  if [[ "${AGGRESSIVE_BUG_TRIGGER}" == "1" ]]; then
    FINAL_RECON_RETRIES=40
  fi
  wait_row_count_converged "${FINAL_RECON_RETRIES}" >/dev/null 2>&1 || true
fi

MYSQL_COUNT_1=$(mysql -h 127.0.0.1 -P "${MYSQL_PORT}" -u root "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};")
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  SINK_COUNT_1=$(sink_mysql_exec "${DATABASE}" -sN -e "SELECT COUNT(*) FROM ${TABLE};" 2>/dev/null || echo "NA")
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
if [[ "${SINK_SQL_ENABLED}" == "1" ]]; then
  append_report "Row count (MySQL/${SINK_LABEL}): ${MYSQL_COUNT_1}/${SINK_COUNT_1}"
else
  append_report "Row count (MySQL/${SINK_LABEL}): ${MYSQL_COUNT_1}/NA (non-SQL sink, parity skipped)"
fi
append_report "Column count (MySQL/${SINK_LABEL}): ${MYSQL_COL_COUNT}/${SINK_COL_COUNT}"
append_report "Schema new columns synced (ok/fail): ${DDL_SYNC_OK}/${DDL_SYNC_FAIL}"
append_report "Final Flink job state: ${FINAL_JOB_STATE}"

write_experiment_archive
append_report "Experiment archive path: ${EXPERIMENT_ARCHIVE_FILE}"

append_report "Finished at: $(timestamp)"
WORKFLOW_END_TS=$(date +%s)
append_report "Step timer sum seconds: ${TOTAL_STEP_SECONDS}"
append_report "Workflow elapsed seconds: $((WORKFLOW_END_TS - WORKFLOW_START_TS))"
append_report "Final report path: ${REPORT_FILE}"
step_timer_end

log "Workflow completed. Report: ${REPORT_FILE}"
