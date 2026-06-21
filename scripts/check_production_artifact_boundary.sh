#!/usr/bin/env bash
set -euo pipefail

root="${1:-target/package}"

if [[ ! -e "$root" ]]; then
  echo "artifact root does not exist: $root" >&2
  exit 2
fi

deny_re='(^|/)(edge|trigger|pnl|policy_replay|tradeable_interval)(\.rs|/|$)'

if find "$root" -type f -o -type d | grep -E "$deny_re"; then
  echo "production artifact contains strategy module paths" >&2
  exit 1
fi

if rg -n --hidden 'pm5m_research_engine' "$root"; then
  echo "production artifact contains pm5m_research_engine" >&2
  exit 1
fi

private_re='(^|/)(hft_private|pm5m_backtest)(/|$)'

if find "$root" -type f -o -type d | grep -E "$private_re"; then
  echo "production artifact contains private backtest paths" >&2
  exit 1
fi

if rg -n --hidden 'hft_private|pm5m_backtest' "$root"; then
  echo "production artifact contains private backtest identifiers" >&2
  exit 1
fi
