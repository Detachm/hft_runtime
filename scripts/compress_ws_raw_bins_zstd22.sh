#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:?usage: $0 RAW_ROOT [TMP_DIR] [LOG_FILE]}"
TMP_DIR="${2:-/home/hliu/hft_runtime/.tmp_ws_raw_zstd22}"
LOG_FILE="${3:-/home/hliu/hft_runtime/runtime/logs/compress_ws_raw_zstd22.log}"
ZSTD_BIN="${ZSTD_BIN:-zstd}"
ZSTD_LEVEL="${ZSTD_LEVEL:---ultra -22}"
ZSTD_THREADS="${ZSTD_THREADS:-0}"

mkdir -p "${TMP_DIR}" "$(dirname "${LOG_FILE}")"
LOCK_FILE="${TMP_DIR}/compress.lock"
exec 9>"${LOCK_FILE}"
if ! flock -n 9; then
  echo "another compressor is already running: ${LOCK_FILE}" >&2
  exit 1
fi

CUTOFF_FILE="${TMP_DIR}/cutoff"
touch "${CUTOFF_FILE}"

log() {
  printf '%s %s\n' "$(date -Is)" "$*" | tee -a "${LOG_FILE}"
}

log "start root=${ROOT} tmp=${TMP_DIR} level=${ZSTD_LEVEL} threads=${ZSTD_THREADS}"

count=0
skipped=0
freed=0
compressed=0

while IFS= read -r -d '' src; do
  dst="${src}.zst"
  if [[ -e "${dst}" ]]; then
    skipped=$((skipped + 1))
    continue
  fi

  rel_sha="$(printf '%s' "${src}" | sha256sum | awk '{print $1}')"
  tmp="${TMP_DIR}/${rel_sha}.zst.tmp"
  rm -f "${tmp}"

  orig_size="$(stat -c '%s' "${src}")"
  log "compress src=${src} bytes=${orig_size}"
  # shellcheck disable=SC2086
  "${ZSTD_BIN}" ${ZSTD_LEVEL} -T"${ZSTD_THREADS}" -q -c -- "${src}" > "${tmp}"
  "${ZSTD_BIN}" -tq -- "${tmp}"
  comp_size="$(stat -c '%s' "${tmp}")"

  rm -f -- "${src}"
  mv -- "${tmp}" "${dst}"

  count=$((count + 1))
  freed=$((freed + orig_size - comp_size))
  compressed=$((compressed + comp_size))
  log "done src=${src} zst=${dst} compressed_bytes=${comp_size} freed_bytes=$((orig_size - comp_size)) total_files=${count} total_freed_bytes=${freed}"
done < <(find "${ROOT}" -type f -name '*.bin' ! -newer "${CUTOFF_FILE}" -print0 | sort -z)

log "finish files=${count} skipped=${skipped} compressed_bytes=${compressed} freed_bytes=${freed}"
