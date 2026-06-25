#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ROOT_DIR:-/home/hliu/hft_runtime}"
ROLE="${ROLE:-${1:-poly_btc}}"
DATA_ROOT="${DATA_ROOT:-/mnt/data/hft/hft_runtime/pm5m_recorder_prod}"
RESTART_DELAY_SECONDS="${RESTART_DELAY_SECONDS:-5}"
CHANNEL_CAPACITY="${CHANNEL_CAPACITY:-65536}"
FLUSH_INTERVAL_MS="${FLUSH_INTERVAL_MS:-5000}"
FLUSH_BYTES="${FLUSH_BYTES:-67108864}"
FLUSH_ROWS="${FLUSH_ROWS:-50000}"
CONNECT_TIMEOUT_MS="${CONNECT_TIMEOUT_MS:-15000}"
PING_INTERVAL_MS="${PING_INTERVAL_MS:-10000}"
REDISCOVERY_INTERVAL_MS="${REDISCOVERY_INTERVAL_MS:-60000}"

cd "${ROOT_DIR}"

case "${ROLE}" in
  poly_btc)
    CONFIG="${CONFIG:-${ROOT_DIR}/configs/pm5m-recorder-poly-btc-5m-15m.json}"
    RAW_ROOT="${RAW_ROOT:-${DATA_ROOT}/poly_btc_5m_15m/raw}"
    STATE_ROOT="${STATE_ROOT:-${DATA_ROOT}/poly_btc_5m_15m/state}"
    TYPED_ROOT="${TYPED_ROOT:-${DATA_ROOT}/poly_btc_5m_15m/typed}"
    LOG_DIR="${LOG_DIR:-${DATA_ROOT}/poly_btc_5m_15m/logs}"
    SYMBOL_ARGS=(--symbol BTC --interval 5m --interval 15m)
    MODE="poly"
    ;;
  poly_eth)
    CONFIG="${CONFIG:-${ROOT_DIR}/configs/pm5m-recorder-poly-eth-5m-15m.json}"
    RAW_ROOT="${RAW_ROOT:-${DATA_ROOT}/poly_eth_5m_15m/raw}"
    STATE_ROOT="${STATE_ROOT:-${DATA_ROOT}/poly_eth_5m_15m/state}"
    TYPED_ROOT="${TYPED_ROOT:-${DATA_ROOT}/poly_eth_5m_15m/typed}"
    LOG_DIR="${LOG_DIR:-${DATA_ROOT}/poly_eth_5m_15m/logs}"
    SYMBOL_ARGS=(--symbol ETH --interval 5m --interval 15m)
    MODE="poly"
    ;;
  poly_sol)
    CONFIG="${CONFIG:-${ROOT_DIR}/configs/pm5m-recorder-poly-sol-5m-15m.json}"
    RAW_ROOT="${RAW_ROOT:-${DATA_ROOT}/poly_sol_5m_15m/raw}"
    STATE_ROOT="${STATE_ROOT:-${DATA_ROOT}/poly_sol_5m_15m/state}"
    TYPED_ROOT="${TYPED_ROOT:-${DATA_ROOT}/poly_sol_5m_15m/typed}"
    LOG_DIR="${LOG_DIR:-${DATA_ROOT}/poly_sol_5m_15m/logs}"
    SYMBOL_ARGS=(--symbol SOL --interval 5m --interval 15m)
    MODE="poly"
    ;;
  reference_binance)
    RAW_ROOT="${RAW_ROOT:-${DATA_ROOT}/reference_binance_btc_eth_sol_1s/raw}"
    STATE_ROOT="${STATE_ROOT:-${DATA_ROOT}/reference_binance_btc_eth_sol_1s/state}"
    LOG_DIR="${LOG_DIR:-${DATA_ROOT}/reference_binance_btc_eth_sol_1s/logs}"
    MODE="reference"
    ;;
  *)
    echo "unknown ROLE=${ROLE}; expected poly_btc, poly_eth, poly_sol, reference_binance" >&2
    exit 2
    ;;
esac

if [[ "${MODE}" == "poly" ]]; then
  mkdir -p "${RAW_ROOT}" "${STATE_ROOT}" "${TYPED_ROOT}" "${LOG_DIR}"
else
  mkdir -p "${RAW_ROOT}" "${STATE_ROOT}" "${LOG_DIR}"
fi
echo "$$" > "${LOG_DIR}/supervisor.pid"

if [[ -n "${RECORDER_BIN:-}" ]]; then
  RECORDER_CMD=("${RECORDER_BIN}")
else
  RECORDER_CMD=(cargo run --release -p pm5m_recorder --bin pm5m-recorder --)
fi

while true; do
  echo "$(date -Is) starting pm5m-recorder role=${ROLE} raw=${RAW_ROOT} state=${STATE_ROOT} typed=${TYPED_ROOT:-}" \
    | tee -a "${LOG_DIR}/supervisor.log"

  if [[ "${MODE}" == "poly" ]]; then
    "${RECORDER_CMD[@]}" run-ws \
      --config "${CONFIG}" \
      --raw-root "${RAW_ROOT}" \
      --state-root "${STATE_ROOT}" \
      --typed-root "${TYPED_ROOT}" \
      --channel-capacity "${CHANNEL_CAPACITY}" \
      --flush-interval-ms "${FLUSH_INTERVAL_MS}" \
      --flush-bytes "${FLUSH_BYTES}" \
      --flush-rows "${FLUSH_ROWS}" \
      --connect-timeout-ms "${CONNECT_TIMEOUT_MS}" \
      --ping-interval-ms "${PING_INTERVAL_MS}" \
      --rediscovery-interval-ms "${REDISCOVERY_INTERVAL_MS}" \
      "${SYMBOL_ARGS[@]}" \
      >> "${LOG_DIR}/stdout.log" \
      2>> "${LOG_DIR}/stderr.log" || true
  else
    "${RECORDER_CMD[@]}" run-reference-ws \
      --raw-root "${RAW_ROOT}" \
      --state-root "${STATE_ROOT}" \
      --reference-venue binance \
      --reference-symbol BTC \
      --reference-symbol ETH \
      --reference-symbol SOL \
      --channel-capacity "${CHANNEL_CAPACITY}" \
      --flush-interval-ms "${FLUSH_INTERVAL_MS}" \
      --flush-bytes "${FLUSH_BYTES}" \
      --flush-rows "${FLUSH_ROWS}" \
      --shadow-latency-ms 200 \
      --shadow-tick-offset 0 \
      >> "${LOG_DIR}/stdout.log" \
      2>> "${LOG_DIR}/stderr.log" || true
  fi

  echo "$(date -Is) pm5m-recorder role=${ROLE} exited; restarting in ${RESTART_DELAY_SECONDS}s" \
    | tee -a "${LOG_DIR}/supervisor.log"
  sleep "${RESTART_DELAY_SECONDS}"
done
