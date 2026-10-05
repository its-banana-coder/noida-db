#!/usr/bin/env bash
# Runs Redis 7.2's own TCL test suite against a server in external mode
# and prints the suite's ok/err/exception counts.
#
#   tests/official/redis/run.sh PORT [extra runtest args...]
#
# Needs tclsh 8.5+ and a Redis 7.2 source tree (fetched and built here;
# the suite needs redis-cli/redis-benchmark next to it).
set -uo pipefail
cd "$(dirname "$0")/../../.."
port=$1; shift
src=target/official/redis/redis-7.2.12
if [ ! -x "$src/src/redis-cli" ]; then
  mkdir -p target/official/redis
  curl -sSfL https://download.redis.io/releases/redis-7.2.12.tar.gz | tar xz -C target/official/redis
  make -C "$src" -j2 >/dev/null
fi
out=$(mktemp)
(cd "$src" && ./runtest --host 127.0.0.1 --port "$port" --clients 1 --timeout 900 "$@") >"$out" 2>&1
strip() { sed 's/\x1b\[[0-9;]*m//g' "$out"; }
ok=$(strip | grep -c '^\[ok\]')
err=$(strip | grep -c '^\[err\]')
exc=$(strip | grep -c '^\[exception\]')
skip=$(strip | grep -c '^\[skip\]')
echo "redis suite: ok $ok, err $err, exception $exc, skipped $skip (log: $out)"
