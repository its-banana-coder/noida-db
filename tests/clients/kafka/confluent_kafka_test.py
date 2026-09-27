#!/usr/bin/env python3
"""confluent-kafka (librdkafka) test against noida-db."""

import os
import sys
import time
from confluent_kafka import Consumer, KafkaError, Producer
from confluent_kafka.admin import AdminClient, NewTopic

PORT = os.environ.get("NOIDA_KAFKA_PORT")
if not PORT:
    print("NOIDA_KAFKA_PORT not set", file=sys.stderr)
    sys.exit(1)

conf = {"bootstrap.servers": f"127.0.0.1:{PORT}"}
failures = []
checks = 0


def check(name, got, want):
    global checks
    checks += 1
    if got != want:
        failures.append(f"{name}: got {got!r}, want {want!r}")


def main():
    admin = AdminClient(conf)

    # 1. Topic management
    topic = "py-topic"
    fs = admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)])
    for t, f in fs.items():
        f.result()  # will raise if creation failed

    meta = admin.list_topics(timeout=10)
    check("topic exists in metadata", topic in meta.topics, True)
    check("topic has 3 partitions", len(meta.topics[topic].partitions), 3)

    # 2. Produce keyed messages
    p = Producer(conf)
    delivered = []

    def on_delivery(err, msg):
        if err:
            failures.append(f"produce delivery error: {err}")
        else:
            delivered.append((msg.key().decode(), msg.value().decode()))

    p.produce(topic, key="k1", value="v1", callback=on_delivery)
    p.produce(topic, key="k1", value="v2", callback=on_delivery)
    p.produce(topic, key="k2", value="v3", callback=on_delivery)
    p.flush(timeout=10)
    check("3 messages delivered", len(delivered), 3)

    # 3. Consumer group consume and commit
    c1 = Consumer({
        "bootstrap.servers": f"127.0.0.1:{PORT}",
        "group.id": "py-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c1.subscribe([topic])

    consumed = []
    start = time.time()
    while time.time() - start < 10 and len(consumed) < 3:
        msg = c1.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            failures.append(f"consumer error: {msg.error()}")
            break
        consumed.append(msg.value().decode())

    check("all 3 messages consumed", sorted(consumed), ["v1", "v2", "v3"])
    c1.commit(asynchronous=False)
    c1.close()

    # 4. Resume from committed offset with new consumer in same group
    p.produce(topic, key="k3", value="v4", callback=on_delivery)
    p.flush(timeout=10)

    c2 = Consumer({
        "bootstrap.servers": f"127.0.0.1:{PORT}",
        "group.id": "py-consumer-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    c2.subscribe([topic])

    resumed_msgs = []
    start = time.time()
    while time.time() - start < 10 and len(resumed_msgs) < 1:
        msg = c2.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            failures.append(f"consumer 2 error: {msg.error()}")
            break
        resumed_msgs.append(msg.value().decode())

    check("resumes past committed offset", resumed_msgs, ["v4"])
    c2.close()

    print(f"confluent-kafka: {checks} checks, {len(failures)} failed")
    for f in failures:
        print("  FAIL", f)
    sys.exit(1 if failures else 0)


if __name__ == "__main__":
    main()
