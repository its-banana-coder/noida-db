#!/usr/bin/env bash
# Runs the committed Postgres client apps (psycopg, SQLAlchemy, node-postgres,
# JDBC) against a server.
#
#   tests/clients/postgres/run.sh            # starts noida on a free port
#   PGPORT=5432 tests/clients/postgres/run.sh  # an already-running server
#
# Dependencies are fetched into target/client-deps on first use. A client
# whose toolchain or download is unavailable prints SKIPPED and is not a
# failure; a client that runs and fails is.
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

if [ -z "${PGPORT:-}" ]; then
  echo "== starting noida"
  cargo build --quiet --features postgres --bin noida || exit 1
  PGPORT=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
  "$root/target/debug/noida" start --only postgres --postgres-port "$PGPORT" \
    --data-dir "$cache/noida-data" >"$cache/noida.log" 2>&1 &
  started_noida=$!
  for _ in $(seq 1 50); do
    (echo > "/dev/tcp/127.0.0.1/$PGPORT") 2>/dev/null && break
    sleep 0.2
  done
fi
export PGPORT
echo "== testing against 127.0.0.1:$PGPORT"

failed=0
run_client() {
  local name="$1"
  shift
  echo "-- $name"
  if "$@"; then
    return 0
  fi
  echo "** $name FAILED"
  failed=1
}

# --- Python: psycopg 3 and SQLAlchemy -------------------------------------
pylibs="$cache/pylibs"
if [ ! -d "$pylibs/psycopg" ]; then
  echo "== installing psycopg and SQLAlchemy"
  pip install --quiet --disable-pip-version-check --target "$pylibs" \
    "psycopg[binary]" sqlalchemy >/dev/null 2>&1
fi
if python3 -c "import sys; sys.path.insert(0, '$pylibs'); import psycopg" 2>/dev/null; then
  export PYTHONPATH="$pylibs"
  run_client "psycopg" python3 "$here/python/psycopg_test.py"
  if python3 -c "import sys; sys.path.insert(0, '$pylibs'); import sqlalchemy" 2>/dev/null; then
    run_client "sqlalchemy" python3 "$here/python/sqlalchemy_test.py"
  else
    echo "-- sqlalchemy SKIPPED (not installed)"
  fi
else
  echo "-- psycopg SKIPPED (not installed)"
fi

# --- Node: node-postgres ---------------------------------------------------
if command -v node >/dev/null && command -v npm >/dev/null; then
  if [ ! -d "$cache/node_modules/pg" ]; then
    echo "== installing node-postgres"
    (cd "$cache" && npm install --silent --no-package-lock pg >/dev/null 2>&1)
  fi
  if [ -d "$cache/node_modules/pg" ]; then
    run_client "node-postgres" env NODE_PATH="$cache/node_modules" node "$here/node/pg_test.js"
  else
    echo "-- node-postgres SKIPPED (npm install failed)"
  fi
else
  echo "-- node-postgres SKIPPED (no node)"
fi

# --- Java: the PostgreSQL JDBC driver --------------------------------------
jar="$cache/postgresql.jar"
if command -v javac >/dev/null && command -v java >/dev/null; then
  if [ ! -s "$jar" ]; then
    echo "== downloading the JDBC driver"
    curl -sS -o "$jar" --max-time 120 \
      https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar || rm -f "$jar"
  fi
  if [ -s "$jar" ]; then
    mkdir -p "$cache/classes"
    if javac -d "$cache/classes" "$here/java/JdbcTest.java" 2>&1; then
      run_client "jdbc" java -cp "$cache/classes:$jar" JdbcTest
    else
      echo "** jdbc FAILED (compile)"
      failed=1
    fi
  else
    echo "-- jdbc SKIPPED (driver download failed)"
  fi
else
  echo "-- jdbc SKIPPED (no JDK)"
fi

if [ "$failed" -ne 0 ]; then
  echo "== some clients failed"
  exit 1
fi
echo "== all available clients passed"
