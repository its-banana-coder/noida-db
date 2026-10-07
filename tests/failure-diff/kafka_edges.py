"""Kafka edge-case differential: run against a real broker and noida-db,
then diff the outputs.

    python kafka_edges.py 127.0.0.1:19092 > real.out
    python kafka_edges.py 127.0.0.1:9092  > noida.out
    diff real.out noida.out

Needs `confluent-kafka`. Every scenario prints normalized lines (no topic
names, no timestamps) so the two outputs are directly comparable.
"""

import sys
import time
import uuid

from confluent_kafka import Consumer, KafkaException, Producer, TopicPartition
from confluent_kafka.admin import (
    AdminClient,
    AlterConfigOpType,
    ConfigEntry,
    ConfigResource,
    NewTopic,
    ResourceType,
)

BS = sys.argv[1]
ONLY = sys.argv[2:] or None
admin = AdminClient({"bootstrap.servers": BS})


def name(tag):
    return f"edge-{tag}-{uuid.uuid4().hex[:8]}"


def err(e):
    """A KafkaException/KafkaError as a stable string."""
    if isinstance(e, KafkaException):
        e = e.args[0]
    if hasattr(e, "name"):
        return f"ERR {e.name()}"
    return f"ERR {type(e).__name__}"


def create(topic, partitions=1, config=None):
    f = admin.create_topics([NewTopic(topic, partitions, 1, config=config or {})])
    try:
        f[topic].result()
        return "ok"
    except Exception as e:
        return err(e)


def produce(topic, msgs, partition=0):
    p = Producer({"bootstrap.servers": BS, "linger.ms": 0, "enable.idempotence": False})
    results = []

    def cb(e, m):
        results.append(err(e) if e else m.offset())

    for k, v in msgs:
        p.produce(topic, key=k, value=v, partition=partition, on_delivery=cb)
        p.flush(10)
    return results


def watermarks(topic, partition=0):
    c = Consumer({"bootstrap.servers": BS, "group.id": "wm"})
    lo, hi = c.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
    c.close()
    return lo, hi


def consume_all(topic, partition=0, start=-2, reset="error"):
    c = Consumer(
        {
            "bootstrap.servers": BS,
            "group.id": name("g"),
            "auto.offset.reset": reset,
            "enable.auto.commit": False,
        }
    )
    lo, hi = c.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
    c.assign([TopicPartition(topic, partition, start)])
    out = []
    deadline = time.time() + 10
    while time.time() < deadline:
        m = c.poll(0.5)
        if m is None:
            if not out and hi == lo:
                break
            if out and out[-1][0] >= hi - 1:
                break
            continue
        if m.error():
            out.append(("error", m.error().name()))
            break
        out.append((m.offset(), m.key(), m.value()))
        if m.offset() >= hi - 1:
            break
    c.close()
    return out


def delete_records(topic, offset, partition=0):
    tp = TopicPartition(topic, partition, offset)
    try:
        r = admin.delete_records([tp])[tp].result()
        return f"low={r.low_watermark}"
    except Exception as e:
        return err(e)


def describe(topic, keys):
    r = ConfigResource(ResourceType.TOPIC, topic)
    for attempt in range(10):
        try:
            cfg = admin.describe_configs([r])[r].result()
            break
        except Exception as e:
            if attempt == 9:
                return err(e)
            time.sleep(0.3)
    return {k: (cfg[k].value, cfg[k].source) for k in keys if k in cfg}


def alter_incremental(topic, entries):
    r = ConfigResource(
        ResourceType.TOPIC,
        topic,
        incremental_configs=[
            ConfigEntry(k, v, incremental_operation=op) for k, v, op in entries
        ],
    )
    try:
        admin.incremental_alter_configs([r])[r].result()
        return "ok"
    except Exception as e:
        return err(e)


def alter_full(topic, configs):
    r = ConfigResource(ResourceType.TOPIC, topic, set_config=configs)
    try:
        admin.alter_configs([r])[r].result()
        return "ok"
    except Exception as e:
        return err(e)


SCENARIOS = {}


def scenario(fn):
    SCENARIOS[fn.__name__] = fn
    return fn


@scenario
def topic_create_validation():
    for cfg in [
        {"no.such.config": "1"},
        {"retention.ms": "abc"},
        {"retention.ms": "-5"},
        {"retention.ms": "-1"},
        {"cleanup.policy": "bogus"},
        {"cleanup.policy": "compact,delete"},
        {"cleanup.policy": "delete,compact"},
        {"cleanup.policy": ""},
        {"min.cleanable.dirty.ratio": "2"},
        {"min.cleanable.dirty.ratio": "0.0"},
        {"segment.bytes": "10"},
        {"segment.ms": "0"},
        {"message.timestamp.type": "LogAppendTime"},
        {"message.timestamp.type": "Nope"},
        {"compression.type": "zstd"},
        {"compression.type": "brotli"},
        {"max.message.bytes": "-1"},
        {"retention.bytes": "-1"},
        {"retention.bytes": "-2"},
        {"preallocate": "yes"},
        {"min.insync.replicas": "0"},
    ]:
        print("create", cfg, create(name("v"), 1, cfg))
    for t in ["bad/name", "..", ".", "a" * 250, "has space", "ok.name_-1"]:
        r = create(t)
        print("create name", t[:20], len(t), r)
        if r == "ok":
            admin.delete_topics([t])[t].result()


@scenario
def describe_defaults():
    t = name("d")
    create(t, 1, {"retention.ms": "12345"})
    d = describe(
        t,
        [
            "cleanup.policy",
            "retention.ms",
            "retention.bytes",
            "segment.bytes",
            "segment.ms",
            "delete.retention.ms",
            "min.compaction.lag.ms",
            "min.cleanable.dirty.ratio",
            "message.timestamp.type",
            "max.message.bytes",
            "compression.type",
        ],
    )
    for k in sorted(d):
        print(k, d[k])


@scenario
def incremental_alter():
    t = name("ia")
    create(t)
    keys = ["cleanup.policy", "retention.ms"]
    S, D, A, R = (
        AlterConfigOpType.SET,
        AlterConfigOpType.DELETE,
        AlterConfigOpType.APPEND,
        AlterConfigOpType.SUBTRACT,
    )
    steps = [
        [("retention.ms", "1000", S)],
        [("retention.ms", "abc", S)],
        [("no.such", "1", S)],
        [("cleanup.policy", "compact", A)],
        [("cleanup.policy", "compact", A)],
        [("cleanup.policy", "delete", R)],
        [("cleanup.policy", "nope", A)],
        [("retention.ms", "5", A)],
        [("retention.ms", None, D)],
        [("cleanup.policy", "compact", R)],
    ]
    for s in steps:
        print("alter", s, alter_incremental(t, s), describe(t, keys))
    print("alter missing topic", alter_incremental(name("missing"), [("retention.ms", "1", S)]))


@scenario
def full_alter_replaces():
    t = name("fa")
    create(t, 1, {"retention.ms": "1000", "segment.ms": "2000"})
    print(alter_full(t, {"retention.bytes": "100000"}))
    print(describe(t, ["retention.ms", "segment.ms", "retention.bytes"]))
    print(alter_full(t, {"retention.bytes": "x"}))
    print(describe(t, ["retention.ms", "segment.ms", "retention.bytes"]))


@scenario
def delete_records_basic():
    t = name("dr")
    create(t)
    print(produce(t, [(b"k", str(i).encode()) for i in range(10)]))
    print("wm", watermarks(t))
    print("del 3", delete_records(t, 3))
    print("wm", watermarks(t))
    print("del 1 (below start)", delete_records(t, 1))
    print("del 11 (past end)", delete_records(t, 11))
    print("del 10 (=end)", delete_records(t, 10))
    print("wm", watermarks(t))
    print(produce(t, [(b"k", b"after")]))
    print("wm", watermarks(t))
    print("consume", consume_all(t))
    print("del -1 (hw)", delete_records(t, -1))
    print("wm", watermarks(t))
    print("del bad partition", delete_records(t, 0, partition=5))
    print("del missing topic", delete_records(name("missing"), 0))


@scenario
def delete_records_mid_batch():
    t = name("drb")
    create(t)
    p = Producer({"bootstrap.servers": BS, "linger.ms": 100})
    for i in range(6):
        p.produce(t, value=str(i).encode(), partition=0)
    p.flush(10)
    print("wm", watermarks(t))
    print("del 4", delete_records(t, 4))
    print("consume", consume_all(t))
    print("consume from 1 (reset earliest)", consume_all(t, start=1, reset="earliest"))
    print("consume from 1 (reset error)", consume_all(t, start=1, reset="error"))


@scenario
def delete_records_compacted():
    t = name("drc")
    create(t, 1, {"cleanup.policy": "compact"})
    print(produce(t, [(b"k", b"v")]))
    print("del compact", delete_records(t, 1))
    t2 = name("drcd")
    create(t2, 1, {"cleanup.policy": "compact,delete"})
    print(produce(t2, [(b"k", b"v")]))
    print("del compact,delete", delete_records(t2, 1))


@scenario
def compacted_null_key():
    t = name("cnk")
    create(t, 1, {"cleanup.policy": "compact"})
    print(produce(t, [(None, b"v"), (b"k", b"v"), (b"k", None)]))


@scenario
def retention_ms():
    t = name("rms")
    create(t, 1, {"retention.ms": "1000"})
    print(produce(t, [(None, str(i).encode()) for i in range(5)]))
    print("wm", watermarks(t))
    time.sleep(4)
    print("wm after", watermarks(t))
    print(produce(t, [(None, b"new")]))
    print("wm", watermarks(t))
    print("consume", consume_all(t))


@scenario
def retention_old_timestamps():
    # Records whose CreateTime is far in the past expire on the next check.
    t = name("rold")
    create(t, 1, {"retention.ms": "60000"})
    p = Producer({"bootstrap.servers": BS, "linger.ms": 0})
    old = int(time.time() * 1000) - 3600_000
    for i in range(3):
        p.produce(t, value=str(i).encode(), partition=0, timestamp=old)
        p.flush(10)
    print("wm", watermarks(t))
    time.sleep(4)
    print("wm after", watermarks(t))


@scenario
def retention_bytes():
    t = name("rb")
    create(t, 1, {"retention.bytes": "300", "segment.bytes": "200"})
    print(produce(t, [(None, b"x" * 60) for _ in range(12)]))
    print("wm", watermarks(t))
    time.sleep(4)
    lo, hi = watermarks(t)
    print("hw", hi, "kept some", 0 < hi - lo < 12)


@scenario
def compaction():
    t = name("cmp")
    create(
        t,
        1,
        {
            "cleanup.policy": "compact",
            "segment.ms": "100",
            "min.cleanable.dirty.ratio": "0.01",
            "delete.retention.ms": "100",
        },
    )
    msgs = [
        (b"a", b"1"),
        (b"b", b"1"),
        (b"a", b"2"),
        (b"c", b"1"),
        (b"b", None),
        (b"a", b"3"),
        (b"d", b"1"),
        (b"c", b"2"),
    ]
    print(produce(t, msgs))
    time.sleep(0.3)
    print(produce(t, [(b"e", b"1")]))  # rolls the segment
    time.sleep(5)
    print("after first clean", consume_all(t))
    time.sleep(0.3)
    print(produce(t, [(b"f", b"1")]))
    time.sleep(5)
    print("after second clean", consume_all(t))
    print("wm", watermarks(t))


@scenario
def compaction_lag():
    t = name("cml")
    create(
        t,
        1,
        {
            "cleanup.policy": "compact",
            "segment.ms": "100",
            "min.cleanable.dirty.ratio": "0.01",
            "min.compaction.lag.ms": "3600000",
        },
    )
    print(produce(t, [(b"a", b"1"), (b"a", b"2")]))
    time.sleep(0.3)
    print(produce(t, [(b"b", b"1")]))
    time.sleep(4)
    print("lagged", consume_all(t))


@scenario
def list_offsets_after_delete():
    t = name("lo")
    create(t)
    p = Producer({"bootstrap.servers": BS, "linger.ms": 0})
    base = int(time.time() * 1000) - 10000
    for i in range(5):
        p.produce(t, value=str(i).encode(), partition=0, timestamp=base + i * 1000)
        p.flush(10)
    c = Consumer({"bootstrap.servers": BS, "group.id": "lo"})

    def at(ts):
        r = c.offsets_for_times([TopicPartition(t, 0, ts)], timeout=10)
        return r[0].offset

    print("ts", [at(base + i * 1000) for i in range(5)], at(base - 1), at(base + 99999))
    print(delete_records(t, 3))
    print("ts", [at(base + i * 1000) for i in range(5)], at(base - 1))
    c.close()


@scenario
def log_append_time():
    t = name("lat")
    create(t, 1, {"message.timestamp.type": "LogAppendTime"})
    p = Producer({"bootstrap.servers": BS, "linger.ms": 0})
    old = int(time.time() * 1000) - 3600_000
    p.produce(t, value=b"x", partition=0, timestamp=old)
    p.flush(10)
    c = Consumer({"bootstrap.servers": BS, "group.id": name("g"), "auto.offset.reset": "earliest"})
    c.assign([TopicPartition(t, 0, 0)])
    m = None
    for _ in range(20):
        m = c.poll(0.5)
        if m is not None:
            break
    ts_type, ts = m.timestamp()
    print("type", ts_type, "recent", abs(ts - time.time() * 1000) < 60000)
    c.close()
    t2 = name("ct")
    create(t2)
    p.produce(t2, value=b"x", partition=0, timestamp=old)
    p.flush(10)
    c = Consumer({"bootstrap.servers": BS, "group.id": name("g"), "auto.offset.reset": "earliest"})
    c.assign([TopicPartition(t2, 0, 0)])
    for _ in range(20):
        m = c.poll(0.5)
        if m is not None:
            break
    print("create time kept", m.timestamp() == (1, old))
    c.close()


@scenario
def max_message_bytes():
    t = name("mmb")
    create(t, 1, {"max.message.bytes": "200"})
    print(produce(t, [(None, b"x" * 50), (None, b"y" * 500)]))


@scenario
def compaction_compressed_headers():
    t = name("cch")
    create(
        t,
        1,
        {
            "cleanup.policy": "compact",
            "segment.ms": "100",
            "min.cleanable.dirty.ratio": "0.01",
        },
    )
    p = Producer({"bootstrap.servers": BS, "linger.ms": 50, "compression.type": "gzip"})
    for k, v in [(b"a", b"1"), (b"b", b"1"), (b"a", b"2"), (b"b", b"2"), (b"c", b"1")]:
        p.produce(t, key=k, value=v, partition=0, headers=[("h", v)])
    p.flush(10)
    time.sleep(0.3)
    p.produce(t, key=b"z", value=b"roll", partition=0)
    p.flush(10)
    time.sleep(5)
    c = Consumer({"bootstrap.servers": BS, "group.id": name("g"), "auto.offset.reset": "error"})
    c.assign([TopicPartition(t, 0, 1)])
    got = []
    for _ in range(20):
        m = c.poll(0.5)
        if m is None:
            if got:
                break
            continue
        if m.error():
            got.append(m.error().name())
            break
        got.append((m.offset(), m.key(), m.value(), m.headers()))
        if m.offset() >= 5:
            break
    print("from 1", got)
    c.close()


@scenario
def committed_below_start():
    t = name("cbs")
    create(t)
    produce(t, [(None, str(i).encode()) for i in range(8)])
    g = name("grp")
    c = Consumer({"bootstrap.servers": BS, "group.id": g, "enable.auto.commit": False})
    c.commit(offsets=[TopicPartition(t, 0, 2)], asynchronous=False)
    c.close()
    print(delete_records(t, 5))
    for reset in ["earliest", "latest"]:
        c = Consumer(
            {
                "bootstrap.servers": BS,
                "group.id": g,
                "auto.offset.reset": reset,
                "enable.auto.commit": False,
            }
        )
        c.subscribe([t])
        got = []
        deadline = time.time() + 10
        while time.time() < deadline:
            m = c.poll(0.5)
            if m is None:
                if got or (reset == "latest" and time.time() > deadline - 5):
                    break
                continue
            got.append(m.offset())
            if m.offset() >= 7:
                break
        print(reset, got)
        c.close()


@scenario
def retention_disabled_and_infinite():
    t = name("rinf")
    create(t, 1, {"retention.ms": "-1", "retention.bytes": "-1"})
    print(produce(t, [(None, b"x")]))
    time.sleep(2)
    print("wm", watermarks(t))



@scenario
def admin_defaults_and_errors():
    """Topic defaults (-1 partitions/replication), CreatePartitions
    validate_only, ACLs on a broker without an authorizer."""
    from confluent_kafka.admin import (
        AclBindingFilter,
        AclOperation,
        AclPermissionType,
        NewPartitions,
        ResourcePatternType,
    )

    t = name("dflt")
    f = admin.create_topics([NewTopic(t)])  # no partition count / replication factor
    try:
        f[t].result()
        print("create default ok")
    except KafkaException as e:
        print("create default", err(e))
    time.sleep(0.5)
    print("partitions", len(admin.list_topics(t, timeout=10).topics[t].partitions))
    for count, validate_only in [(3, True), (1, False), (1, False), (2, False)]:
        try:
            admin.create_partitions([NewPartitions(t, count)], validate_only=validate_only)[
                t
            ].result()
            print("create_partitions", count, validate_only, "ok")
        except KafkaException as e:
            print("create_partitions", count, validate_only, err(e))
    time.sleep(0.5)
    print("partitions", len(admin.list_topics(t, timeout=10).topics[t].partitions))
    acl_filter = AclBindingFilter(
        ResourceType.ANY,
        None,
        ResourcePatternType.ANY,
        None,
        None,
        AclOperation.ANY,
        AclPermissionType.ANY,
    )
    try:
        print("acls", admin.describe_acls(acl_filter).result())
    except KafkaException as e:
        print("acls", err(e))

for n, fn in SCENARIOS.items():
    if ONLY and n not in ONLY:
        continue
    print(f"== {n}")
    sys.stdout.flush()
    try:
        fn()
    except Exception as e:
        print("EXC", type(e).__name__, str(e)[:200])
    sys.stdout.flush()
