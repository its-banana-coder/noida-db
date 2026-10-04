#!/usr/bin/env bash
# Failure-path differential: the same failing transactions (duplicate keys,
# strict-mode errors, rollbacks, savepoints, ...) against noida-db and a
# real server, comparing rows, error codes and transaction state per step.
#
#   MYSQL_REF_PORT=3307 POSTGRES_REF_PORT=5432 tests/failure-diff/run.sh
#
# A real server is needed for each service compared: MySQL 8 (root, empty
# password) and Postgres (postgres/postgres). A service whose *_REF_PORT
# isn't set is skipped.
set -uo pipefail

cd "$(dirname "$0")/../.."
here="tests/failure-diff"
work="target/failure-diff"
mkdir -p "$work"

bin=$(cargo build -q --release --features mysql,postgres --bin noida-db --message-format=json 2>/dev/null \
  | python3 -c 'import sys,json
for l in sys.stdin:
    m=json.loads(l)
    if m.get("executable"): print(m["executable"])' | tail -1)
[ -x "$bin" ] || { echo "could not build the server binary"; exit 2; }

free_port() { python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()'; }
my_port=$(free_port)
pg_port=$(free_port)
rm -rf "$work/data"
"$bin" start --only mysql,postgres --mysql-port "$my_port" --postgres-port "$pg_port" \
  --data-dir "$work/data" >"$work/server.log" 2>&1 &
server=$!
trap 'kill $server 2>/dev/null' EXIT
for p in "$my_port" "$pg_port"; do
  for _ in $(seq 50); do
    (exec 3<>/dev/tcp/127.0.0.1/"$p") 2>/dev/null && break
    sleep 0.1
  done
done

if [ ! -x "$work/venv/bin/python" ]; then
  python3 -m venv "$work/venv" && "$work/venv/bin/pip" install -q pymysql==1.1.1 'psycopg[binary]==3.2.3'
fi

status=0
ran=0
if [ -n "${MYSQL_REF_PORT:-}" ]; then
  "$work/venv/bin/python" "$here/failure_diff.py" mysql "$my_port" "$MYSQL_REF_PORT" || status=1
  ran=1
fi
if [ -n "${POSTGRES_REF_PORT:-}" ]; then
  "$work/venv/bin/python" "$here/failure_diff.py" postgres "$pg_port" "$POSTGRES_REF_PORT" || status=1
  ran=1
fi
[ $ran = 1 ] || { echo "set MYSQL_REF_PORT and/or POSTGRES_REF_PORT"; exit 2; }
exit $status
