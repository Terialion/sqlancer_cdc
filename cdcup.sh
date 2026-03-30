#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../.." && pwd)"

if [[ ! -x "${ROOT_DIR}/cdcup.sh" ]]; then
  echo "ERROR: root cdcup.sh not found: ${ROOT_DIR}/cdcup.sh" >&2
  exit 1
fi

ARGS=("$@")
if [[ ${#ARGS[@]} -ge 2 && "${ARGS[0]}" == "pipeline" ]]; then
  pipeline_arg="${ARGS[1]}"
  if [[ "${pipeline_arg}" != /* ]]; then
    ARGS[1]="$(cd "${SCRIPT_DIR}" && cd "$(dirname "${pipeline_arg}")" && pwd -P)/$(basename "${pipeline_arg}")"
  fi
fi

# Always run from repository root so compose context stays consistent.
cd "${ROOT_DIR}"
exec ./cdcup.sh "${ARGS[@]}"
