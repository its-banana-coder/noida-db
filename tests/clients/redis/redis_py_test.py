"""redis-py against noida: the things applications do, over RESP2 and RESP3.

Run through tests/clients/redis/run.sh, which starts noida and sets
NOIDA_REDIS_PORT.
"""

import os
import sys
import threading
import time

import redis

PORT = int(os.environ["NOIDA_REDIS_PORT"])
failures = []
checks = 0


def plain(x):
    """redis-py returns tuples for some RESP2 replies and lists for RESP3;
    both mean the same, so compare them as lists."""
    if isinstance(x, (list, tuple)):
        return [plain(i) for i in x]
    if isinstance(x, dict):
        return {k: plain(v) for k, v in x.items()}
    return x


def check(name, got, want):
    global checks
    checks += 1
    if plain(got) != plain(want):
        failures.append(f"{name}: got {got!r}, want {want!r}")


def scenario(protocol):
    tag = f"resp{protocol}"
    r = redis.Redis(port=PORT, protocol=protocol, decode_responses=True)
    r.flushall()

    check(f"{tag} ping", r.ping(), True)
    info = r.info()
    check(f"{tag} info has a version", "redis_version" in info, True)
    check(f"{tag} client name", r.client_setname("py") and r.client_getname(), "py")

    # strings and expiry
    check(f"{tag} set", r.set("k", "v", ex=100), True)
    check(f"{tag} get", r.get("k"), "v")
    check(f"{tag} ttl", 0 < r.ttl("k") <= 100, True)
    check(f"{tag} set nx", r.set("k", "w", nx=True), None)
    check(f"{tag} incr", r.incr("n", 5), 5)
    check(f"{tag} incrbyfloat", r.incrbyfloat("f", 1.5), 1.5)
    check(f"{tag} mset/mget", (r.mset({"a": 1, "b": 2}), r.mget("a", "b", "zz")), (True, ["1", "2", None]))
    check(f"{tag} getdel", (r.getdel("a"), r.exists("a")), ("1", 0))

    # hashes, lists, sets, sorted sets
    r.hset("h", mapping={"x": "1", "y": "2"})
    check(f"{tag} hgetall", r.hgetall("h"), {"x": "1", "y": "2"})
    check(f"{tag} hincrby", r.hincrby("h", "x", 4), 5)
    r.rpush("l", "a", "b", "c")
    check(f"{tag} lrange", r.lrange("l", 0, -1), ["a", "b", "c"])
    check(f"{tag} lpop count", r.lpop("l", 2), ["a", "b"])
    r.sadd("s", "x", "y", "z")
    check(f"{tag} smembers", sorted(r.smembers("s")), ["x", "y", "z"])
    r.zadd("z", {"a": 1.5, "b": 2, "c": 3})
    check(f"{tag} zrange withscores", r.zrange("z", 0, -1, withscores=True), [("a", 1.5), ("b", 2.0), ("c", 3.0)])
    check(f"{tag} zscore", r.zscore("z", "b"), 2.0)
    check(f"{tag} zrangebyscore", r.zrangebyscore("z", 2, 3), ["b", "c"])
    check(f"{tag} zincrby", r.zincrby("z", 0.5, "a"), 2.0)

    # keys
    for i in range(30):
        r.set(f"scan:{i}", i)
    check(f"{tag} scan_iter", len(list(r.scan_iter("scan:*", count=7))), 30)
    check(f"{tag} type", r.type("h"), "hash")
    check(f"{tag} rename", (r.rename("scan:0", "moved"), r.get("moved")), (True, "0"))
    check(f"{tag} dbsize is positive", r.dbsize() > 0, True)

    # pipelines and transactions
    pipe = r.pipeline(transaction=False)
    pipe.set("p1", 1).incr("p1").get("p1")
    check(f"{tag} pipeline", pipe.execute(), [True, 2, "2"])
    tx = r.pipeline(transaction=True)
    tx.set("t1", "x").get("t1")
    check(f"{tag} multi/exec", tx.execute(), [True, "x"])
    r.set("watched", 1)
    with r.pipeline() as wp:
        wp.watch("watched")
        wp.multi()
        wp.incr("watched")
        check(f"{tag} watch", wp.execute(), [2])

    # Lua
    script = r.register_script("return redis.call('incrby', KEYS[1], ARGV[1])")
    check(f"{tag} register_script", (script(keys=["lua"], args=[3]), script(keys=["lua"], args=[4])), (3, 7))
    check(f"{tag} eval returns a table", r.eval("return {1, 'two', {3}}", 0), [1, "two", [3]])

    # pub/sub
    got = []
    sub = r.pubsub()
    sub.subscribe("chan")
    sub.get_message(timeout=2)  # the subscribe confirmation

    def listen():
        for _ in range(20):
            m = sub.get_message(timeout=0.2)
            if m and m["type"] == "message":
                got.append(m["data"])
                return

    t = threading.Thread(target=listen)
    t.start()
    time.sleep(0.2)
    check(f"{tag} publish reaches a subscriber", r.publish("chan", "hello"), 1)
    t.join()
    check(f"{tag} pubsub message", got, ["hello"])
    sub.close()

    # streams
    sid = r.xadd("stream", {"f": "v"})
    check(f"{tag} xadd/xrange", [e[1] for e in r.xrange("stream")], [{"f": "v"}])
    r.xgroup_create("stream", "g", id="0")
    read = r.xreadgroup("g", "c1", {"stream": ">"}, count=1)
    entry = read[0][1][0][0] if protocol == 2 else read["stream"][0][0][0]
    check(f"{tag} xreadgroup", entry, sid)
    check(f"{tag} xack", r.xack("stream", "g", sid), 1)

    # HyperLogLog, SORT, geo, bitmaps
    check(f"{tag} pfadd/pfcount", (r.pfadd("hll", "a", "b", "c"), r.pfcount("hll")), (1, 3))
    r.rpush("nums", 3, 1, 2)
    check(f"{tag} sort", r.sort("nums"), ["1", "2", "3"])
    r.geoadd("geo", (13.361389, 38.115556, "Palermo", 15.087269, 37.502669, "Catania"))
    check(f"{tag} geodist", round(r.geodist("geo", "Palermo", "Catania", "km")), 166)
    r.setbit("bits", 7, 1)
    check(f"{tag} bitcount", r.bitcount("bits"), 1)

    # errors reach the client as exceptions
    r.set("str", "text")
    try:
        r.incr("str")
        check(f"{tag} error raised", "no error", "ResponseError")
    except redis.ResponseError as e:
        check(f"{tag} error text", "not an integer" in str(e), True)
    try:
        r.lpush("str", "x")
        check(f"{tag} wrongtype raised", "no error", "ResponseError")
    except redis.ResponseError as e:
        check(f"{tag} wrongtype text", str(e).startswith("WRONGTYPE"), True)

    # binary safety
    blob = bytes(range(256))
    rb = redis.Redis(port=PORT, protocol=protocol)
    rb.set("blob", blob)
    check(f"{tag} binary round trip", rb.get("blob"), blob)

    # a blocking pop is served by another client
    other = redis.Redis(port=PORT, protocol=protocol, decode_responses=True)
    result = []
    t = threading.Thread(target=lambda: result.append(r.blpop("queue", timeout=3)))
    t.start()
    time.sleep(0.3)
    other.rpush("queue", "job")
    t.join()
    check(f"{tag} blpop", result, [("queue", "job")])

    r.close()
    rb.close()
    other.close()


for proto in (2, 3):
    scenario(proto)

print(f"redis-py: {checks} checks, {len(failures)} failed")
for f in failures:
    print("  FAIL", f)
sys.exit(1 if failures else 0)
