#!/usr/bin/env bash
# Runs real MySQL drivers and ORMs against noida-db's MySQL service:
# pymysql, SQLAlchemy, Django (Python), mysql2 (Node), database/sql + GORM
# (Go) and Connector/J (Java).
#
#   tests/clients/mysql/run.sh                          # starts noida-db on a free port
#   NOIDA_MYSQL_PORT=3306 tests/clients/mysql/run.sh    # an already-running, empty server
#
# Each client exits non-zero on any failed check. Missing toolchains are
# skipped, not failed. Dependencies are cached under target/clients/mysql/.
set -uo pipefail

cd "$(dirname "$0")/../../.."
here="tests/clients/mysql"
work="target/clients/mysql"
mkdir -p "$work"

started_noida=""
cleanup() {
  [ -n "$started_noida" ] && kill "$started_noida" 2>/dev/null
  return 0
}
trap cleanup EXIT

if [ -z "${NOIDA_MYSQL_PORT:-}" ]; then
  bin=$(cargo build -q --release --features mysql --bin noida-db --message-format=json 2>/dev/null \
    | python3 -c 'import sys,json
for l in sys.stdin:
    m=json.loads(l)
    if m.get("executable"): print(m["executable"])' | tail -1)
  [ -x "$bin" ] || { echo "could not build the server binary"; exit 2; }

  port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
  rm -rf "$work/data"
  "$bin" start --only mysql --mysql-port "$port" --data-dir "$work/data" >"$work/server.log" 2>&1 &
  started_noida=$!
  for _ in $(seq 50); do
    (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null && break
    sleep 0.1
  done
  export NOIDA_MYSQL_PORT="$port"
fi

status=0
summary=()
run() {  # name, command...
  local name=$1; shift
  echo "-- $name"
  if "$@"; then summary+=("PASS  $name"); else summary+=("FAIL  $name"); status=1; fi
}
skip() { summary+=("SKIP  $1 ($2)"); }

# 1-3. Python: pymysql, SQLAlchemy, Django
if command -v python3 >/dev/null; then
  if [ ! -x "$work/venv/bin/python" ]; then
    python3 -m venv "$work/venv" >/dev/null 2>&1 &&
      "$work/venv/bin/pip" install -q pymysql==1.1.1 sqlalchemy==2.0.36 'django>=5.1,<5.3' >/dev/null 2>&1
  fi
  if "$work/venv/bin/python" -c 'import pymysql, sqlalchemy, django' 2>/dev/null; then
    run "pymysql" "$work/venv/bin/python" "$here/python/pymysql_test.py"
    run "sqlalchemy" "$work/venv/bin/python" "$here/python/sqlalchemy_test.py"
    run "django" "$work/venv/bin/python" "$here/python/django_test.py"
  else
    skip "python clients" "could not install"
  fi
else
  skip "python clients" "no python3"
fi

# 4. Node.js: mysql2 (server-side prepared statements)
if command -v node >/dev/null && command -v npm >/dev/null; then
  if [ ! -d "$work/node/node_modules/mysql2" ]; then
    mkdir -p "$work/node" && (cd "$work/node" && npm init -y >/dev/null 2>&1 && npm install --silent mysql2@3 >/dev/null 2>&1)
  fi
  if [ -d "$work/node/node_modules/mysql2" ]; then
    run "mysql2" env NODE_PATH="$PWD/$work/node/node_modules" node "$here/node/mysql2_test.js"
  else
    skip "mysql2" "could not install"
  fi
else
  skip "mysql2" "no node"
fi

# 5. Go: database/sql + go-sql-driver/mysql, and GORM
if command -v go >/dev/null; then
  if (cd "$here/go" && GOFLAGS=-modcacherw go build -o "$OLDPWD/$work/go-mysql-test" . >/dev/null 2>&1); then
    run "go database/sql + gorm" "$work/go-mysql-test"
  else
    skip "go database/sql + gorm" "could not build"
  fi
else
  skip "go database/sql + gorm" "no go"
fi

# 6. Java: MySQL Connector/J (client- and server-side prepares)
if command -v java >/dev/null && command -v javac >/dev/null; then
  jar="$work/java/mysql-connector-j-8.4.0.jar"
  if [ ! -f "$jar" ]; then
    mkdir -p "$work/java"
    curl -sfL -o "$jar" https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.4.0/mysql-connector-j-8.4.0.jar || rm -f "$jar"
  fi
  if [ -f "$jar" ] && javac -cp "$jar" -d "$work/java" "$here/java/T.java" 2>/dev/null; then
    run "jdbc" java -cp "$jar:$work/java" T
  else
    skip "jdbc" "could not fetch or compile"
  fi
else
  skip "jdbc" "no java"
fi

echo
printf '%s\n' "${summary[@]}"
exit $status
