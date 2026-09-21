#!/usr/bin/env bash
# Fails if the release binary exceeds the size budget.
# The install budget is ~100MB total; the binary gets a much smaller slice.
set -euo pipefail

BUDGET_MB="${NOIDA_BINARY_BUDGET_MB:-40}"

cargo build --release --quiet
BIN="target/release/noida"
BYTES=$(wc -c < "$BIN")
MB=$(awk "BEGIN { printf \"%.2f\", $BYTES / 1048576 }")

echo "noida binary: ${MB}MB (budget ${BUDGET_MB}MB)"
if awk "BEGIN { exit !($BYTES > $BUDGET_MB * 1048576) }"; then
  echo "error: binary exceeds size budget" >&2
  exit 1
fi
