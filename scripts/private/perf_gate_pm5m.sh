#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

STATIC_ONLY=0
for arg in "$@"; do
  case "${arg}" in
    --static-only) STATIC_ONLY=1 ;;
    --required) STATIC_ONLY=0 ;;
    *) echo "unknown argument: ${arg}" >&2; exit 2 ;;
  esac
done

echo "PM5M perf gate:"
echo "  raw audit format: HFTREC4"
echo "  book cache format: HFTBOOK2"
echo "  book index format: HFTIDX1"
echo "  reference cache format: HFTREF1"
echo "  settlement cache format: HFTSETTLE1"

echo "checking static hot-path guards"
for retired_path in \
  hft_private/src/bin/ws_raw_segment_backtest.rs \
  hft_private/src/bin/filter_ws_raw_window.rs \
  hft_private/src/bin/filter_pm5m_raw.rs \
  hft_private/src/bin/backfill_pm5m_raw_metadata.rs \
  hft_private/src/bin/summarize_raw_symbols.rs \
  hft_private/src/ws_raw_filter.rs \
  scripts/private/run_pm5m_backtest_window.sh \
  crates/pm5m_research_engine \
  crates/market_data_etl_core/src/hftrec.rs \
  crates/market_data_etl_core/tests/hftrec.rs \
  crates/pm5m_market_cache/src/hftbook.rs \
  crates/pm5m_data_etl/src/coverage.rs \
  crates/pm5m_recorder/src/book.rs \
  crates/pm5m_recorder/src/import.rs \
  crates/pm5m_recorder/src/recorder.rs; do
  if [[ -e "${retired_path}" ]]; then
    echo "retired PM5M legacy path must not exist: ${retired_path}" >&2
    exit 2
  fi
done

if rg -n 'pm5m_data_etl' crates/pm5m_recorder/Cargo.toml crates/pm5m_recorder/src; then
  echo "pm5m_recorder must not depend on pm5m_data_etl" >&2
  exit 2
fi

mapfile -t script_files < <(find scripts -type f -name '*.sh' ! -path 'scripts/private/perf_gate_pm5m.sh' | sort)
if [[ "${#script_files[@]}" -gt 0 ]] && rg -n 'pm5m-recorder -- run([[:space:]]|$)| run --config' "${script_files[@]}"; then
  echo "recorder scripts must use pm5m-recorder run-dual" >&2
  exit 2
fi
if [[ "${#script_files[@]}" -gt 0 ]] && rg -n 'pm5m-recorder.*run-ws|-- run-ws([[:space:]]|$)' "${script_files[@]}"; then
  echo "recorder scripts must use pm5m-recorder run-dual" >&2
  exit 2
fi

private_paths=()
[[ -d hft_private/src ]] && private_paths+=(hft_private/src)
[[ -d hft_private/tests ]] && private_paths+=(hft_private/tests)

if [[ "${#private_paths[@]}" -gt 0 ]]; then
  if rg -n 'build_facts|build_event_index|__row_json|for_each_parquet_table_row' "${private_paths[@]}"; then
    echo "private backtest hot path must not call ETL/event-index/row-json readers" >&2
    exit 2
  fi
fi

public_runtime_paths=(crates/pm5m_market_cache/src crates/pm5m_data_etl/src)
book_scan_paths=("${public_runtime_paths[@]}")
[[ -d hft_private/src ]] && book_scan_paths+=(hft_private/src)
if rg -n 'read_book_cache_rows|validate_book_cache\(|HFTBOOK1' "${book_scan_paths[@]}"; then
  echo "private run-fast hot path must not use HFTBOOK1" >&2
  exit 2
fi

hftrec_scan_paths=(crates/market_data_etl_core/src crates/pm5m_market_cache/src crates/pm5m_data_etl/src crates/pm5m_recorder/src)
[[ -d hft_private/src ]] && hftrec_scan_paths+=(hft_private/src)
if rg -n 'discover_hftrec3_manifests|read_hftrec3_records|verify_hftrec3_manifest|write_hftrec3_segment|HFTREC3|hftrec3' "${hftrec_scan_paths[@]}"; then
  echo "PM5M new chain must not expose HFTREC3 code paths" >&2
  exit 2
fi

private_cli_paths=()
[[ -f hft_private/src/cli.rs ]] && private_cli_paths+=(hft_private/src/cli.rs)
[[ -f hft_private/src/main.rs ]] && private_cli_paths+=(hft_private/src/main.rs)
if [[ "${#private_cli_paths[@]}" -gt 0 ]]; then
  if rg -n 'build_fast_book_index_from_book_state_index|read_book_state_index\(' "${private_cli_paths[@]}"; then
    echo "private run-fast hot path must use file-backed HFTIDX1, not full index materialization" >&2
    exit 2
  fi
fi

row_json_paths=(crates)
[[ -d hft_private ]] && row_json_paths+=(hft_private)
if rg -n '__row_json' "${row_json_paths[@]}" -g '*.rs'; then
  echo "__row_json is not allowed in production code" >&2
  exit 2
fi

if [[ "${STATIC_ONLY}" == "1" ]]; then
  echo "static-only perf gate passed"
  exit 0
fi

RAW_ROOT="${PM5M_RAW_ROOT:-}"
BOOK_CACHE_ROOT="${PM5M_BOOK_CACHE_ROOT:-}"
BOOK_INDEX_ROOT="${PM5M_BOOK_INDEX_ROOT:-}"
REFERENCE_CACHE_ROOT="${PM5M_REFERENCE_CACHE_ROOT:-}"
SETTLEMENT_CACHE_ROOT="${PM5M_SETTLEMENT_CACHE_ROOT:-}"
CONFIG="${PM5M_CONFIG:-}"
OUTPUT_ROOT="${PM5M_OUTPUT_ROOT:-/tmp/pm5m_perf_gate}"
START_TS_NS="${PM5M_START_TS_NS:-}"
END_TS_NS="${PM5M_END_TS_NS:-}"
MAX_BUILD_MS="${PM5M_MAX_BUILD_MS:-3600000}"
MIN_SCAN_ROWS_PER_SEC="${PM5M_MIN_SCAN_ROWS_PER_SEC:-0}"
MIN_INDEX_ROWS_PER_SEC="${PM5M_MIN_INDEX_ROWS_PER_SEC:-1000000}"
MAX_BACKTEST_MS="${PM5M_MAX_BACKTEST_MS:-300000}"

missing=0
for name in RAW_ROOT BOOK_CACHE_ROOT BOOK_INDEX_ROOT REFERENCE_CACHE_ROOT SETTLEMENT_CACHE_ROOT CONFIG; do
  if [[ -z "${!name}" ]]; then
    echo "missing required env: PM5M_${name#PM5M_}" >&2
    missing=1
  fi
done
if [[ "${missing}" != "0" ]]; then
  echo "real perf gate requires PM5M_RAW_ROOT, PM5M_BOOK_CACHE_ROOT, PM5M_BOOK_INDEX_ROOT, PM5M_REFERENCE_CACHE_ROOT, PM5M_SETTLEMENT_CACHE_ROOT, and PM5M_CONFIG" >&2
  echo "use --static-only only for CI smoke/static checks" >&2
  exit 2
fi
if [[ ! -f hft_private/Cargo.toml ]]; then
  echo "real perf gate requires local private hft_private checkout" >&2
  exit 2
fi

rm -rf "${OUTPUT_ROOT}"
mkdir -p "${OUTPUT_ROOT}"

json_number() {
  local key="$1"
  local file="$2"
  sed -n "s/.*\"${key}\"[[:space:]]*:[[:space:]]*\\([0-9.][0-9.]*\\).*/\\1/p" "${file}" | head -1
}

require_le() {
  local label="$1"
  local actual="$2"
  local max="$3"
  if [[ -z "${actual}" ]]; then
    echo "missing metric ${label}" >&2
    exit 2
  fi
  if awk "BEGIN { exit !(${actual} <= ${max}) }"; then
    echo "${label}=${actual} <= ${max}"
  else
    echo "${label}=${actual} exceeds ${max}" >&2
    exit 3
  fi
}

require_ge() {
  local label="$1"
  local actual="$2"
  local min="$3"
  if [[ -z "${actual}" ]]; then
    echo "missing metric ${label}" >&2
    exit 2
  fi
  if awk "BEGIN { exit !(${actual} >= ${min}) }"; then
    echo "${label}=${actual} >= ${min}"
  else
    echo "${label}=${actual} below ${min}" >&2
    exit 3
  fi
}

echo "building HFTBOOK2 cache"
build_report="${OUTPUT_ROOT}/build-book-cache.json"
build_cmd=(cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- build-book-cache
  --raw-root "${RAW_ROOT}"
  --cache-root "${BOOK_CACHE_ROOT}"
  --overwrite
  --output "${build_report}")
if [[ -n "${START_TS_NS}" ]]; then
  build_cmd+=(--raw-start-ts-ns "${START_TS_NS}")
fi
if [[ -n "${END_TS_NS}" ]]; then
  build_cmd+=(--raw-end-ts-ns "${END_TS_NS}")
fi
"${build_cmd[@]}"
require_le "raw_to_hftbook2_elapsed_ms" "$(json_number elapsed_ms "${build_report}")" "${MAX_BUILD_MS}"

echo "validating compact caches"
cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- validate-book-cache \
  --cache-root "${BOOK_CACHE_ROOT}" \
  --output "${OUTPUT_ROOT}/validate-book-cache.json"

echo "building HFTIDX1 book index"
index_report="${OUTPUT_ROOT}/build-book-index.json"
index_cmd=(cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- build-book-index
  --book-cache-root "${BOOK_CACHE_ROOT}"
  --index-root "${BOOK_INDEX_ROOT}"
  --overwrite
  --output "${index_report}")
if [[ -n "${START_TS_NS}" ]]; then
  index_cmd+=(--start-ts-ns "${START_TS_NS}")
fi
if [[ -n "${END_TS_NS}" ]]; then
  index_cmd+=(--end-ts-ns "${END_TS_NS}")
fi
"${index_cmd[@]}"

cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- validate-book-index \
  --index-root "${BOOK_INDEX_ROOT}" \
  --output "${OUTPUT_ROOT}/validate-book-index.json"
cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- validate-reference-cache \
  --cache-root "${REFERENCE_CACHE_ROOT}" \
  --output "${OUTPUT_ROOT}/validate-reference-cache.json"
cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- validate-settlement-cache \
  --cache-root "${SETTLEMENT_CACHE_ROOT}" \
  --output "${OUTPUT_ROOT}/validate-settlement-cache.json"

echo "benchmarking HFTBOOK2 cache scan"
scan_report="${OUTPUT_ROOT}/bench-cache.json"
bench_cmd=(cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- bench-cache
  --book-cache-root "${BOOK_CACHE_ROOT}"
  --output "${scan_report}")
if [[ -n "${START_TS_NS}" ]]; then
  bench_cmd+=(--start-ts-ns "${START_TS_NS}")
fi
if [[ -n "${END_TS_NS}" ]]; then
  bench_cmd+=(--end-ts-ns "${END_TS_NS}")
fi
"${bench_cmd[@]}"
require_ge "hftbook2_scan_rows_per_sec" "$(json_number rows_per_sec "${scan_report}")" "${MIN_SCAN_ROWS_PER_SEC}"

echo "benchmarking HFTIDX1 book index"
index_bench_report="${OUTPUT_ROOT}/bench-book-index.json"
index_bench_cmd=(cargo run --release -p pm5m_market_cache --bin pm5m-market-cache -- bench-book-index
  --index-root "${BOOK_INDEX_ROOT}"
  --output "${index_bench_report}")
if [[ -n "${START_TS_NS}" ]]; then
  index_bench_cmd+=(--start-ts-ns "${START_TS_NS}")
fi
if [[ -n "${END_TS_NS}" ]]; then
  index_bench_cmd+=(--end-ts-ns "${END_TS_NS}")
fi
"${index_bench_cmd[@]}"
require_ge "hftidx1_rows_per_sec" "$(json_number rows_per_sec "${index_bench_report}")" "${MIN_INDEX_ROWS_PER_SEC}"

echo "running indexed fast backtest"
backtest_report="${OUTPUT_ROOT}/run-fast.json"
cargo run --release --manifest-path hft_private/Cargo.toml --bin pm5m_backtest -- run-fast \
  --book-cache-root "${BOOK_CACHE_ROOT}" \
  --book-index-root "${BOOK_INDEX_ROOT}" \
  --reference-cache-root "${REFERENCE_CACHE_ROOT}" \
  --settlement-cache-root "${SETTLEMENT_CACHE_ROOT}" \
  --config "${CONFIG}" \
  --output-dir "${OUTPUT_ROOT}/run" \
  > "${backtest_report}"
require_le "run_fast_total_elapsed_ms" "$(json_number total_elapsed_ms "${backtest_report}")" "${MAX_BACKTEST_MS}"

echo "real perf gate passed; reports in ${OUTPUT_ROOT}"
