#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-/home/hliu/hft_runtime}"
CONFIG="${1:-${ROOT_DIR}/runtime/pm5m-recorder.json}"
LOG_DIR="${LOG_DIR:-${ROOT_DIR}/runtime/logs}"
RESTART_DELAY_SECONDS="${RESTART_DELAY_SECONDS:-5}"

mkdir -p "${LOG_DIR}"
cd "${ROOT_DIR}"

while true; do
  echo "$(date -Is) starting pm5m-recorder config=${CONFIG}" | tee -a "${LOG_DIR}/pm5m-recorder-supervisor.log"
  cargo run --release -p pm5m_recorder --bin pm5m-recorder -- run --config "${CONFIG}" \
    >> "${LOG_DIR}/pm5m-recorder.stdout.log" \
    2>> "${LOG_DIR}/pm5m-recorder.stderr.log" || true
  echo "$(date -Is) pm5m-recorder exited; restarting in ${RESTART_DELAY_SECONDS}s" | tee -a "${LOG_DIR}/pm5m-recorder-supervisor.log"
  sleep "${RESTART_DELAY_SECONDS}"
done
