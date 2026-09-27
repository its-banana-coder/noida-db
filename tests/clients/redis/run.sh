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
  # RQ: a job queue, in the same venv as redis-py.
  if [ -x "$work/venv/bin/python" ] && ! "$work/venv/bin/python" -c 'import rq' 2>/dev/null; then
    "$work/venv/bin/pip" install -q rq >/dev/null 2>&1
  fi
  if [ -x "$work/venv/bin/python" ] && "$work/venv/bin/python" -c 'import rq' 2>/dev/null; then
    run "rq" "$work/venv/bin/python" "$here/rq_test.py"
  else
    skip "rq" "could not install"
  fi
  # Celery: broker and result backend both on noida-db, run against a
  # real worker process.
  if [ -x "$work/venv/bin/python" ] && ! "$work/venv/bin/python" -c 'import celery' 2>/dev/null; then
    "$work/venv/bin/pip" install -q celery >/dev/null 2>&1
  fi
  if [ -x "$work/venv/bin/python" ] && "$work/venv/bin/python" -c 'import celery' 2>/dev/null; then
    (cd "$here" && "$OLDPWD/$work/venv/bin/celery" -A celery_tasks worker --loglevel=warning --pool=solo -c 1        >"$OLDPWD/$work/celery-worker.log" 2>&1 &
     echo $! >"$OLDPWD/$work/celery-worker.pid")
    sleep 3
    run "celery" "$work/venv/bin/python" "$here/celery_test.py"
    kill "$(cat "$work/celery-worker.pid")" 2>/dev/null
  else
    skip "celery" "could not install"
  fi
else
  skip "redis-py" "no python3"
  skip "rq" "no python3"
  skip "celery" "no python3"
fi

# Sidekiq (Ruby): gems install into target/, not the system gem home.
if command -v ruby >/dev/null && command -v gem >/dev/null; then
  export GEM_HOME="$PWD/$work/gems"
  export GEM_PATH="$GEM_HOME"
  export PATH="$GEM_HOME/bin:$PATH"
  if [ ! -x "$GEM_HOME/bin/sidekiq" ]; then
    gem install -N sidekiq redis >/dev/null 2>&1
  fi
  if [ -x "$GEM_HOME/bin/sidekiq" ]; then
    (cd "$here" && "$GEM_HOME/bin/sidekiq" -r ./sidekiq_jobs.rb -c 2 -q default \
       >"$OLDPWD/$work/sidekiq-worker.log" 2>&1 &
     echo $! >"$OLDPWD/$work/sidekiq-worker.pid")
    sleep 2
    run "sidekiq" ruby "$here/sidekiq_test.rb"
    kill "$(cat "$work/sidekiq-worker.pid")" 2>/dev/null
  else
    skip "sidekiq" "could not install"
  fi
else
  skip "sidekiq" "no ruby"
fi

# Node clients: node-redis (the official client), ioredis, and BullMQ (the
# job queue, which is Lua-script heavy)
if command -v node >/dev/null && command -v npm >/dev/null; then
  if [ ! -d "$work/node/node_modules/bullmq" ] || [ ! -d "$work/node/node_modules/redis" ]; then
    mkdir -p "$work/node" && (cd "$work/node" && npm init -y >/dev/null 2>&1 && npm install --silent redis ioredis bullmq >/dev/null 2>&1)
  fi
  if [ -d "$work/node/node_modules/redis" ]; then
    run "node-redis" env NODE_PATH="$PWD/$work/node/node_modules" node "$here/node_redis_test.js"
  else
    skip "node-redis" "could not install"
  fi
  if [ -d "$work/node/node_modules/ioredis" ]; then
    run "ioredis" env NODE_PATH="$PWD/$work/node/node_modules" node "$here/ioredis_test.js"
  else
    skip "ioredis" "could not install"
  fi
  if [ -d "$work/node/node_modules/bullmq" ]; then
    run "bullmq" env NODE_PATH="$PWD/$work/node/node_modules" node "$here/bullmq_test.js"
  else
    skip "bullmq" "could not install"
  fi
else
  skip "node-redis" "no node"
  skip "ioredis" "no node"
  skip "bullmq" "no node"
fi

# Java clients: Jedis, Lettuce, Spring Data Redis and Redisson (Gradle,
# self-fetched wrapper; nothing global, cache under target/)
if command -v java >/dev/null; then
  export GRADLE_USER_HOME="$PWD/$work/gradle-home"
  if (cd "$here/java" && ./gradlew --console=plain -q compileJava >/dev/null 2>&1); then
    run "jedis" bash -c "cd '$here/java' && NOIDA_REDIS_PORT=$NOIDA_REDIS_PORT ./gradlew --console=plain -q run -DmainClass=JedisTest"
    run "lettuce" bash -c "cd '$here/java' && NOIDA_REDIS_PORT=$NOIDA_REDIS_PORT ./gradlew --console=plain -q run -DmainClass=LettuceTest"
    run "spring-data-redis" bash -c "cd '$here/java' && NOIDA_REDIS_PORT=$NOIDA_REDIS_PORT ./gradlew --console=plain -q run -DmainClass=SpringDataRedisTest"
    run "redisson" bash -c "cd '$here/java' && NOIDA_REDIS_PORT=$NOIDA_REDIS_PORT ./gradlew --console=plain -q run -DmainClass=RedissonTest"
  else
    skip "jedis" "could not build"
    skip "lettuce" "could not build"
    skip "spring-data-redis" "could not build"
    skip "redisson" "could not build"
  fi
else
  skip "jedis" "no java"
  skip "lettuce" "no java"
  skip "spring-data-redis" "no java"
  skip "redisson" "no java"
fi

# go-redis (speaks RESP3 by default)
if command -v go >/dev/null; then
  if (cd "$here/go" && GOMODCACHE="$PWD/../../../../$work/gomod" GOFLAGS=-modcacherw GOTOOLCHAIN=local \
        go build -o "$OLDPWD/$work/go-redis-test" . >/dev/null 2>&1); then
    run "go-redis" "$work/go-redis-test"
  else
    skip "go-redis" "could not build"
  fi
else
  skip "go-redis" "no go"
fi

echo
printf '%s\n' "${summary[@]}"
exit $status
