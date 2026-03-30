#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

BASE_SEED="${BASE_SEED:-700}"
DORIS_PIPELINE="${DORIS_PIPELINE:-pipeline-definition-doris.yaml}"
PAIMON_PIPELINE="${PAIMON_PIPELINE:-pipeline-definition-paimon.yaml}"
RUN_PAIMON="${RUN_PAIMON:-1}"

PULL_LOG="/tmp/ci_pull_images_${BASE_SEED}.log"
DORIS_LOG="/tmp/ci_e2e_doris_${BASE_SEED}.log"
PAIMON_LOG="/tmp/ci_e2e_paimon_${BASE_SEED}.log"

echo "[CI] Step 1/3 pull_images batch dry-run"
python3 "${SCRIPT_DIR}/pull_images.py" \
  --batch \
  --mode quick \
  --source-type mysql \
  --sink-type doris \
  --cdc-version 3.2.1 \
  --project-name cdcup \
  --output-dir "/tmp/ci_pull_images_run_${BASE_SEED}" \
  --target-dir "/tmp/ci_pull_images_cdc_${BASE_SEED}" \
  --dry-run \
  --skip-image-pull \
  >"${PULL_LOG}" 2>&1

echo "[CI] Step 2/3 e2e doris short run"
"${SCRIPT_DIR}/run_sqlancer_cdc_e2e.sh" \
  --pipeline-yaml "${DORIS_PIPELINE}" \
  --sink-type doris \
  --base-seed "${BASE_SEED}" \
  --dml-count 2 \
  --ddl-count 1 \
  --mixed-count 2 \
  --wait-sync 1 \
  >"${DORIS_LOG}" 2>&1

PAIMON_RESULT="SKIPPED"
if [[ "${RUN_PAIMON}" == "1" ]]; then
  echo "[CI] Step 3/3 e2e paimon short run"
  if "${SCRIPT_DIR}/run_sqlancer_cdc_e2e.sh" \
    --pipeline-yaml "${PAIMON_PIPELINE}" \
    --sink-type paimon \
    --base-seed "$((BASE_SEED + 1))" \
    --dml-count 2 \
    --ddl-count 1 \
    --mixed-count 2 \
    --wait-sync 1 \
    >"${PAIMON_LOG}" 2>&1; then
    PAIMON_RESULT="PASS"
  else
    PAIMON_RESULT="FAIL"
  fi
fi

echo ""
echo "+----------------------+--------+-----------------------------------------------+"
echo "| Check                | Result | Log                                           |"
echo "+----------------------+--------+-----------------------------------------------+"
echo "| pull_images batch    | PASS   | ${PULL_LOG} |"
echo "| e2e doris short      | PASS   | ${DORIS_LOG} |"
echo "| e2e paimon short     | ${PAIMON_RESULT} | ${PAIMON_LOG} |"
echo "+----------------------+--------+-----------------------------------------------+"

if [[ "${PAIMON_RESULT}" == "FAIL" ]]; then
  exit 1
fi
