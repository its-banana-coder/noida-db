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
# One file at a time: a test that can't run against an external server
# (it needs the server's pid, say) raises an exception that would end a
# whole-suite run, so it only costs its own file here.
files=$(cd "$src/tests" && find unit integration -name '*.tcl' | sed 's/\.tcl$//' | sort)
for f in $files; do
  echo "=== $f" >>"$out"
  (cd "$src" && timeout 900 ./runtest --host 127.0.0.1 --port "$port" --clients 1 --timeout 300 --single "$f" "$@") >>"$out" 2>&1
  redis-cli -p "$port" flushall >/dev/null 2>&1 || true
done
strip() { sed 's/\x1b\[[0-9;]*m//g' "$out"; }
ok=$(strip | grep -c '^\[ok\]')
err=$(strip | grep -c '^\[err\]')
exc=$(strip | grep -c '^\[exception\]')
skip=$(strip | grep -c '^\[skip\]')
failed_files=$(strip | awk '/^=== /{f=$2} /^\[(err|exception)\]/{print f}' | sort -u | tr '\n' ' ')
echo "redis suite: ok $ok, err $err, exception $exc, skipped $skip (log: $out)"
echo "files with errors: $failed_files"
