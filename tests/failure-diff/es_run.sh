#!/usr/bin/env bash
# Elasticsearch edge-case differential (es_edges.py): every scenario
# against noida-db and a real Elasticsearch 8 node.
#
#   ES_REF=http://127.0.0.1:9200 tests/failure-diff/es_run.sh [scenario ...]
set -uo pipefail
cd "$(dirname "$0")/../.."
work="target/failure-diff-es"
mkdir -p "$work"
[ -n "${ES_REF:-}" ] || { echo "set ES_REF"; exit 2; }

bin=$(cargo build -q --release --features elasticsearch --bin noida-db --message-format=json 2>/dev/null \
  | python3 -c 'import sys,json
for l in sys.stdin:
    m=json.loads(l)
    if m.get("executable"): print(m["executable"])' | tail -1)
[ -x "$bin" ] || { echo "could not build the server binary"; exit 2; }

port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
rm -rf "$work/data"
"$bin" start --only elasticsearch --elasticsearch-port "$port" --data-dir "$work/data" >"$work/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null' EXIT
for _ in $(seq 50); do
  (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null && break
  sleep 0.1
done
python3 tests/failure-diff/es_edges.py "$ES_REF" "http://127.0.0.1:$port" "$@"
