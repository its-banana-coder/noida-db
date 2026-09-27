#!/usr/bin/env bash
# Runs the official Python and Node Elasticsearch clients against noida-db.
set -uo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
cache="${NOIDA_CLIENT_CACHE:-$root/target/client-deps/elasticsearch}"
mkdir -p "$cache"

started=""
cleanup() {
  [ -n "$started" ] && kill "$started" 2>/dev/null
  return 0
}
trap cleanup EXIT

port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
cargo build --quiet --no-default-features --features elasticsearch --bin noida-db || exit 1
"$root/target/debug/noida-db" start --only elasticsearch --elasticsearch-port "$port" \
  --data-dir "$cache/data" >"$cache/server.log" 2>&1 &
started=$!
for _ in $(seq 1 50); do
  (echo > "/dev/tcp/127.0.0.1/$port") 2>/dev/null && break
  sleep 0.1
done
export ELASTICSEARCH_URL="http://127.0.0.1:$port"

failed=0
run() {
  local name=$1
  shift
  if "$@"; then
    return
  fi
  echo "** $name FAILED"
  failed=1
}

python_libs="$cache/python"
if [ ! -d "$python_libs/elasticsearch" ]; then
  python3 -m pip install --quiet --disable-pip-version-check --target "$python_libs" 'elasticsearch>=8,<9' >/dev/null 2>&1 || true
fi
if PYTHONPATH="$python_libs" python3 -c 'import elasticsearch' 2>/dev/null; then
  run elasticsearch-py env PYTHONPATH="$python_libs" python3 "$here/python_client.py"
else
  echo "-- elasticsearch-py SKIPPED (could not install)"
fi

if command -v node >/dev/null && command -v npm >/dev/null; then
  if [ ! -d "$cache/node_modules/@elastic/elasticsearch" ]; then
    (cd "$cache" && npm install --silent --no-package-lock @elastic/elasticsearch@8 >/dev/null 2>&1) || true
  fi
  if [ -d "$cache/node_modules/@elastic/elasticsearch" ]; then
    run @elastic/elasticsearch env NODE_PATH="$cache/node_modules" node "$here/node_client.js"
  else
    echo "-- @elastic/elasticsearch SKIPPED (could not install)"
  fi
else
  echo "-- @elastic/elasticsearch SKIPPED (no node or npm)"
fi

[ "$failed" -eq 0 ] || exit 1
echo "== all available Elasticsearch clients passed"
