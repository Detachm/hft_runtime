#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-/home/hliu/hft_runtime}"
CONFIG="${1:-${CONFIG:-${ROOT_DIR}/configs/pm5m-recorder-hftrec4.example.json}}"
LOG_DIR="${LOG_DIR:-${ROOT_DIR}/runtime/logs}"
RESTART_DELAY_SECONDS="${RESTART_DELAY_SECONDS:-5}"
RAW_ROOT="${RAW_ROOT:-/mnt/data/hft/hft_runtime/live_polymarket_all_current_hftrec4_ws_raw/raw}"
STATE_ROOT="${STATE_ROOT:-/mnt/data/hft/hft_runtime/live_polymarket_all_current_hftrec4_ws_raw/state}"
BOOK_STATE_CACHE_ROOT="${BOOK_STATE_CACHE_ROOT:-/mnt/data/hft/hft_runtime/live_polymarket_all_current_hftrec4_ws_raw/book_hftbook2}"

mkdir -p "${LOG_DIR}"
cd "${ROOT_DIR}"

while true; do
  echo "$(date -Is) starting pm5m-recorder dual config=${CONFIG} raw=${RAW_ROOT} state=${STATE_ROOT} book_cache=${BOOK_STATE_CACHE_ROOT}" | tee -a "${LOG_DIR}/pm5m-recorder-supervisor.log"
  cargo run --release -p pm5m_recorder --bin pm5m-recorder -- run-dual \
    --config "${CONFIG}" \
    --raw-root "${RAW_ROOT}" \
    --state-root "${STATE_ROOT}" \
    --book-state-cache-root "${BOOK_STATE_CACHE_ROOT}" \
    >> "${LOG_DIR}/pm5m-recorder.stdout.log" \
    2>> "${LOG_DIR}/pm5m-recorder.stderr.log" || true
  echo "$(date -Is) pm5m-recorder exited; restarting in ${RESTART_DELAY_SECONDS}s" | tee -a "${LOG_DIR}/pm5m-recorder-supervisor.log"
  sleep "${RESTART_DELAY_SECONDS}"
done
