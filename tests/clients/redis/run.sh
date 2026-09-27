#!/usr/bin/env bash
# Runs real Redis client libraries against noida-db's Redis service.
#
#   tests/clients/redis/run.sh            # every client whose toolchain is installed
#
# Each client runs its scenario over RESP2 and RESP3 and exits non-zero on the
# first failed check. Missing toolchains are skipped, not failed. Dependencies
# are installed into target/clients/ (nothing global).
set -uo pipefail

cd "$(dirname "$0")/../../.."
here="tests/clients/redis"
work="target/clients/redis"
mkdir -p "$work"

# Build and find the binary without depending on its name.
bin=$(cargo build -q --release --message-format=json 2>/dev/null \
  | python3 -c 'import sys,json
for l in sys.stdin:
    m=json.loads(l)
    if m.get("executable"): print(m["executable"])' | tail -1)
[ -x "$bin" ] || { echo "could not build the server binary"; exit 2; }

port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
"$bin" start --only redis --redis-port "$port" --data-dir "$work/data" >"$work/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null' EXIT
for _ in $(seq 50); do
  (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null && break
  sleep 0.1
done
export NOIDA_REDIS_PORT="$port"

status=0
summary=()

run() {  # name, command...
  local name=$1; shift
  if "$@"; then summary+=("PASS  $name"); else summary+=("FAIL  $name"); status=1; fi
}
skip() { summary+=("SKIP  $1 ($2)"); }

# redis-py
if command -v python3 >/dev/null; then
  if [ ! -x "$work/venv/bin/python" ]; then
    python3 -m venv "$work/venv" >/dev/null 2>&1 && "$work/venv/bin/pip" install -q redis >/dev/null 2>&1
  fi
  if [ -x "$work/venv/bin/python" ] && "$work/venv/bin/python" -c 'import redis' 2>/dev/null; then
    run "redis-py" "$work/venv/bin/python" "$here/redis_py_test.py"
  else
    skip "redis-py" "could not install"
  fi
else
  skip "redis-py" "no python3"
fi

# ioredis
if command -v node >/dev/null && command -v npm >/dev/null; then
  if [ ! -d "$work/node/node_modules/ioredis" ]; then
    mkdir -p "$work/node" && (cd "$work/node" && npm init -y >/dev/null 2>&1 && npm install --silent ioredis >/dev/null 2>&1)
  fi
  if [ -d "$work/node/node_modules/ioredis" ]; then
    run "ioredis" env NODE_PATH="$PWD/$work/node/node_modules" node "$here/ioredis_test.js"
  else
    skip "ioredis" "could not install"
  fi
else
  skip "ioredis" "no node"
fi

echo
printf '%s\n' "${summary[@]}"
exit $status
