#!/usr/bin/env bash
# Runs real Kafka client libraries against noida-db's Kafka service.
#
#   tests/clients/kafka/run.sh            # starts noida-db on a free port
#   NOIDA_KAFKA_PORT=9092 tests/clients/kafka/run.sh  # an already-running server
#
# Each client runs its scenario and exits non-zero on any failed check.
# Missing toolchains are skipped, not failed. Dependencies are cached
# under target/clients/kafka/ (nothing global).
set -uo pipefail

cd "$(dirname "$0")/../../.."
here="tests/clients/kafka"
work="target/clients/kafka"
mkdir -p "$work"

started_noida=""
cleanup() {
  [ -n "$started_noida" ] && kill "$started_noida" 2>/dev/null
  return 0
}
trap cleanup EXIT

if [ -z "${NOIDA_KAFKA_PORT:-}" ]; then
  bin=$(cargo build -q --release --features kafka --bin noida-db --message-format=json 2>/dev/null \
    | python3 -c 'import sys,json
for l in sys.stdin:
    m=json.loads(l)
    if m.get("executable"): print(m["executable"])' | tail -1)
  [ -x "$bin" ] || { echo "could not build the server binary"; exit 2; }

  port=$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()')
  "$bin" start --only kafka --kafka-port "$port" --data-dir "$work/data" >"$work/server.log" 2>&1 &
  started_noida=$!
  for _ in $(seq 50); do
    (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null && break
    sleep 0.1
  done
  export NOIDA_KAFKA_PORT="$port"
fi

status=0
summary=()

run() {  # name, command...
  local name=$1; shift
  if "$@"; then summary+=("PASS  $name"); else summary+=("FAIL  $name"); status=1; fi
}
skip() { summary+=("SKIP  $1 ($2)"); }

# 1. Node.js: kafkajs
if command -v node >/dev/null && command -v npm >/dev/null; then
  if [ ! -d "$work/node/node_modules/kafkajs" ]; then
    mkdir -p "$work/node" && (cd "$work/node" && npm init -y >/dev/null 2>&1 && npm install --silent kafkajs >/dev/null 2>&1)
  fi
  if [ -d "$work/node/node_modules/kafkajs" ]; then
    run "kafkajs" env NODE_PATH="$PWD/$work/node/node_modules" node "$here/kafkajs_test.js"
  else
    skip "kafkajs" "could not install"
  fi
else
  skip "kafkajs" "no node"
fi

# 2. Python: confluent-kafka (librdkafka)
if command -v python3 >/dev/null; then
  if [ ! -x "$work/venv/bin/python" ]; then
    python3 -m venv "$work/venv" >/dev/null 2>&1 && "$work/venv/bin/pip" install -q confluent-kafka >/dev/null 2>&1
  fi
  if [ -x "$work/venv/bin/python" ] && "$work/venv/bin/python" -c 'import confluent_kafka' 2>/dev/null; then
    run "confluent-kafka" "$work/venv/bin/python" "$here/confluent_kafka_test.py"
  else
    skip "confluent-kafka" "could not install"
  fi
else
  skip "confluent-kafka" "no python3"
fi

# 3. Go: segmentio/kafka-go
if command -v go >/dev/null; then
  if (cd "$here/go" && GOMODCACHE="$PWD/../../../../$work/gomod" GOFLAGS=-modcacherw GOTOOLCHAIN=local \
        go build -o "$OLDPWD/$work/go-kafka-test" . >/dev/null 2>&1); then
    run "kafka-go" "$work/go-kafka-test"
  else
    skip "kafka-go" "could not build"
  fi
else
  skip "kafka-go" "no go"
fi

# 4. Java: kafka-clients 3.8
if command -v java >/dev/null && command -v javac >/dev/null; then
  jars_dir="$work/java/jars"
  mkdir -p "$jars_dir"
  python3 -c '
import urllib.request, os, sys
jars_dir = sys.argv[1]
deps = [
    ("kafka-clients-3.8.0.jar", "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.8.0/kafka-clients-3.8.0.jar"),
    ("slf4j-api-2.0.13.jar", "https://repo1.maven.org/maven2/org/slf4j/slf4j-api/2.0.13/slf4j-api-2.0.13.jar"),
    ("slf4j-simple-2.0.13.jar", "https://repo1.maven.org/maven2/org/slf4j/slf4j-simple/2.0.13/slf4j-simple-2.0.13.jar"),
    ("zstd-jni-1.5.6-3.jar", "https://repo1.maven.org/maven2/com/github/luben/zstd-jni/1.5.6-3/zstd-jni-1.5.6-3.jar"),
    ("lz4-java-1.8.0.jar", "https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar"),
    ("snappy-java-1.1.10.5.jar", "https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar"),
]
for name, url in deps:
    dest = os.path.join(jars_dir, name)
    if not os.path.exists(dest):
        urllib.request.urlretrieve(url, dest)
' "$jars_dir" >/dev/null 2>&1

  mkdir -p "$work/java/bin"
  if javac -cp "$jars_dir/*" -d "$work/java/bin" "$here/java/KafkaClientsTest.java" >/dev/null 2>&1; then
    run "kafka-clients" java -cp "$work/java/bin:$jars_dir/*" KafkaClientsTest
  else
    skip "kafka-clients" "could not build"
  fi
else
  skip "kafka-clients" "no java"
fi

# 5. Java (Gradle): Spring Kafka, a transactional producer, and every
# compression codec. Separate from the plain kafka-clients test above
# because Spring Kafka's dependency tree is painful to hand-fetch as raw
# jars; this uses a self-fetched Gradle wrapper instead (nothing global).
if command -v java >/dev/null; then
  export GRADLE_USER_HOME="$PWD/$work/gradle-home"
  if (cd "$here/java-gradle" && ./gradlew --console=plain -q compileJava >/dev/null 2>&1); then
    run "spring-kafka" bash -c "cd '$here/java-gradle' && NOIDA_KAFKA_PORT=$NOIDA_KAFKA_PORT ./gradlew --console=plain -q run -DmainClass=SpringKafkaTest"
    run "compression-codecs" bash -c "cd '$here/java-gradle' && NOIDA_KAFKA_PORT=$NOIDA_KAFKA_PORT ./gradlew --console=plain -q run -DmainClass=CompressionTest"
    run "transactional-producer" bash -c "cd '$here/java-gradle' && NOIDA_KAFKA_PORT=$NOIDA_KAFKA_PORT ./gradlew --console=plain -q run -DmainClass=TransactionalProducerTest"
  else
    skip "spring-kafka" "could not build"
    skip "compression-codecs" "could not build"
    skip "transactional-producer" "could not build"
  fi
else
  skip "spring-kafka" "no java"
  skip "compression-codecs" "no java"
  skip "transactional-producer" "no java"
fi

# 6. Python: faust_app (real application test)
if command -v python3 >/dev/null; then
  if [ ! -d "$work/faust_env" ]; then
    python3 -m venv "$work/faust_env" >/dev/null 2>&1 && "$work/faust_env/bin/pip" install -q faust-streaming >/dev/null 2>&1
  fi
  if [ -x "$work/faust_env/bin/python" ]; then
    run "faust-app" bash -c "cd '$here/faust_app' && NOIDA_KAFKA_PORT=$NOIDA_KAFKA_PORT PATH=\"$PWD/$work/faust_env/bin:\$PATH\" ./run.sh >/dev/null 2>&1"
  else
    skip "faust-app" "could not install"
  fi
else
  skip "faust-app" "no python3"
fi

echo
printf '%s\n' "${summary[@]}"
exit $status
