#!/usr/bin/env bash
# tasks/refresh.sh
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PY="$ROOT/.venv/Scripts/python.exe"

export EOS_NO_OUTLOOK="${EOS_NO_OUTLOOK:-1}"
export PYTHONUTF8=1
export PYTHONIOENCODING=utf-8

S2_LIMIT="${S2_LIMIT:-100}"
S5_DAYS_WAIT="${S5_DAYS_WAIT:-5}"
PROGRAM="${PROGRAM:-YourProgramName}"
S2_SOURCE="${S2_SOURCE:-csv}"
S4_LIMIT="${S4_LIMIT:-500}"
S5_MAX_ROWS="${S5_MAX_ROWS:-500}"
VARIANT="${VARIANT:-default}"

exec "$PY" -X utf8 "$ROOT/scripts/auto_refresh.py" \
  --with-s2 --with-s3 --with-s4 --with-s5 \
  --s2-limit "$S2_LIMIT" \
  --s5-days-wait "$S5_DAYS_WAIT" \
  --program "$PROGRAM" \
  --s2-source "$S2_SOURCE" \
  --s4-limit "$S4_LIMIT" \
  --s5-max-rows "$S5_MAX_ROWS" \
  --variant "$VARIANT"
