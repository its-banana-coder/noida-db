#!/usr/bin/env bash
set -uo pipefail

if [ -z "${NOIDA_KAFKA_PORT:-}" ]; then
  echo "NOIDA_KAFKA_PORT is not set"
  exit 1
fi

export FAUST_BROKER_URL="kafka://127.0.0.1:${NOIDA_KAFKA_PORT}"

echo "Starting Faust workers..."
timeout 20 python3 tests/clients/kafka/faust_app/app.py worker -l info -p 6066 -A tests.clients.kafka.faust_app.app > worker1.log 2>&1 &
w1=$!
sleep 2

timeout 20 python3 tests/clients/kafka/faust_app/app.py worker -l info -p 6067 -A tests.clients.kafka.faust_app.app > worker2.log 2>&1 &
w2=$!
sleep 5

echo "Sending messages..."
python3 tests/clients/kafka/faust_app/app.py -A tests.clients.kafka.faust_app.app send hello-topic '{"message": "m1"}' --key=k1
python3 tests/clients/kafka/faust_app/app.py -A tests.clients.kafka.faust_app.app send hello-topic '{"message": "m2"}' --key=k2
python3 tests/clients/kafka/faust_app/app.py -A tests.clients.kafka.faust_app.app send hello-topic '{"message": "m3"}' --key=k3
python3 tests/clients/kafka/faust_app/app.py -A tests.clients.kafka.faust_app.app send hello-topic '{"message": "m4"}' --key=k4

sleep 10

kill $w1 2>/dev/null || true
kill $w2 2>/dev/null || true

wait $w1 || true
wait $w2 || true

echo "Checking logs for received messages..."
cat worker1.log worker2.log | grep "Received greeting:" > received_messages.log || true

count=$(wc -l < received_messages.log)
if [ "$count" -ge 4 ]; then
  echo "Successfully received messages"
  exit 0
else
  echo "Failed: only $count messages received"
  cat received_messages.log
  exit 1
fi
