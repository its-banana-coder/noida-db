#!/usr/bin/env bash
# Runs the committed Memcached client apps against a server.
#
#   tests/clients/memcached/run.sh               # starts noida-db on a free port
#   NOIDA_MEMCACHED_PORT=11211 tests/clients/memcached/run.sh  # an already-running server
#
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
cache="${NOIDA_CLIENT_CACHE:-$root/target/client-deps}"
mkdir -p "$cache"

started_noida=""
cleanup() {
  [ -n "$started_noida" ] && kill "$started_noida" 2>/dev/null
  return 0
}
trap cleanup EXIT

if [ -z "${NOIDA_MEMCACHED_PORT:-}" ]; then
  echo "== starting noida-db"
  cargo build --quiet --features memcached --bin noida-db || exit 1
  NOIDA_MEMCACHED_PORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
  "$root/target/debug/noida-db" start --only memcached --memcached-port "$NOIDA_MEMCACHED_PORT" > "$here/noida.log" 2>&1 &
  started_noida=$!
  sleep 1
  if ! kill -0 "$started_noida" 2>/dev/null; then
    echo "noida-db failed to start"
    cat "$here/noida.log"
    exit 1
  fi
fi

run_client() {
  local name="$1"
  shift
  echo "-- $name"
  if ! "$@" "127.0.0.1" "$NOIDA_MEMCACHED_PORT"; then
    echo "$name FAILED"
    [ -f "$here/noida.log" ] && tail -n 50 "$here/noida.log"
    exit 1
  fi
}

pylibs="$cache/pylibs"
if [ ! -d "$pylibs/pymemcache" ] || [ ! -d "$pylibs/django" ]; then
  echo "== installing pymemcache, django"
  pip install --quiet --disable-pip-version-check --target "$pylibs" pymemcache django >/dev/null 2>&1
fi

export PYTHONPATH="$pylibs"
run_client "pymemcache" python3 "$here/test.py" || exit 1
run_client "django" python3 "$here/django_test.py" || exit 1

echo "== all memcached client tests passed"
