// ioredis against noida-db: what Node applications do, over RESP2 and RESP3.
// Run through tests/clients/redis/run.sh, which starts the server and sets
// NOIDA_REDIS_PORT.
const Redis = require("ioredis");

const PORT = Number(process.env.NOIDA_REDIS_PORT);
const failures = [];
let checks = 0;

function check(name, got, want) {
  checks += 1;
  const a = JSON.stringify(got);
  const b = JSON.stringify(want);
  if (a !== b) failures.push(`${name}: got ${a}, want ${b}`);
}

async function scenario(protocol) {
  const tag = `resp${protocol}`;
  const r = new Redis({ port: PORT, protocol, lazyConnect: false, maxRetriesPerRequest: 1 });
  await r.flushall();

  check(`${tag} ping`, await r.ping(), "PONG");
  check(`${tag} info`, (await r.info()).includes("redis_version"), true);

  // strings
  check(`${tag} set ex`, await r.set("k", "v", "EX", 100), "OK");
  check(`${tag} get`, await r.get("k"), "v");
  const ttl = await r.ttl("k");
  check(`${tag} ttl`, ttl > 0 && ttl <= 100, true);
  check(`${tag} set nx`, await r.set("k", "w", "NX"), null);
  check(`${tag} incrby`, await r.incrby("n", 5), 5);
  check(`${tag} mset/mget`, [await r.mset("a", 1, "b", 2), await r.mget("a", "b", "zz")], ["OK", ["1", "2", null]]);

  // hashes, lists, sets, sorted sets
  await r.hset("h", { x: "1", y: "2" });
  check(`${tag} hgetall`, await r.hgetall("h"), { x: "1", y: "2" });
  await r.rpush("l", "a", "b", "c");
  check(`${tag} lrange`, await r.lrange("l", 0, -1), ["a", "b", "c"]);
  await r.sadd("s", "x", "y", "z");
  check(`${tag} smembers`, (await r.smembers("s")).sort(), ["x", "y", "z"]);
  await r.zadd("z", 1.5, "a", 2, "b", 3, "c");
  check(`${tag} zrange withscores`, await r.zrange("z", 0, -1, "WITHSCORES"), ["a", "1.5", "b", "2", "c", "3"]);
  check(`${tag} zscore`, await r.zscore("z", "b"), "2");

  // scan
  for (let i = 0; i < 30; i++) await r.set(`scan:${i}`, i);
  const seen = new Set();
  await new Promise((resolve, reject) => {
    const stream = r.scanStream({ match: "scan:*", count: 7 });
    stream.on("data", (keys) => keys.forEach((k) => seen.add(k)));
    stream.on("end", resolve);
    stream.on("error", reject);
  });
  check(`${tag} scanStream`, seen.size, 30);

  // pipelines and transactions
  check(
    `${tag} pipeline`,
    (await r.pipeline().set("p1", 1).incr("p1").get("p1").exec()).map((x) => x[1]),
    ["OK", 2, "2"]
  );
  check(
    `${tag} multi`,
    (await r.multi().set("t1", "x").get("t1").exec()).map((x) => x[1]),
    ["OK", "x"]
  );

  // Lua: defineCommand is how most Node code uses scripts
  r.defineCommand("incrTwice", { numberOfKeys: 1, lua: "redis.call('incr', KEYS[1]); return redis.call('incr', KEYS[1])" });
  check(`${tag} defineCommand`, await r.incrTwice("lua"), 2);
  check(`${tag} eval table`, await r.eval("return {1, 'two', {3}}", 0), [1, "two", [3]]);

  // pub/sub
  const sub = new Redis({ port: PORT, protocol });
  const messages = [];
  await sub.subscribe("chan");
  sub.on("message", (channel, msg) => messages.push([channel, msg]));
  check(`${tag} publish reaches a subscriber`, await r.publish("chan", "hello"), 1);
  await new Promise((res) => setTimeout(res, 200));
  check(`${tag} pubsub message`, messages, [["chan", "hello"]]);
  sub.disconnect();

  // streams
  const id = await r.xadd("stream", "*", "f", "v");
  check(`${tag} xrange`, (await r.xrange("stream", "-", "+")).map((e) => e[1]), [["f", "v"]]);
  await r.xgroup("CREATE", "stream", "g", "0");
  const read = await r.xreadgroup("GROUP", "g", "c1", "COUNT", 1, "STREAMS", "stream", ">");
  check(`${tag} xreadgroup id`, read[0][1][0][0], id);
  check(`${tag} xack`, await r.xack("stream", "g", id), 1);

  // other types
  check(`${tag} pfadd/pfcount`, [await r.pfadd("hll", "a", "b", "c"), await r.pfcount("hll")], [1, 3]);
  await r.rpush("nums", 3, 1, 2);
  check(`${tag} sort`, await r.sort("nums"), ["1", "2", "3"]);

  // errors
  await r.set("str", "text");
  try {
    await r.incr("str");
    check(`${tag} error raised`, "no error", "ReplyError");
  } catch (e) {
    check(`${tag} error text`, e.message.includes("not an integer"), true);
  }

  // binary safety
  const blob = Buffer.from(Array.from({ length: 256 }, (_, i) => i));
  await r.set("blob", blob);
  check(`${tag} binary round trip`, Buffer.compare(await r.getBuffer("blob"), blob), 0);

  // a blocking pop served by another client
  const other = new Redis({ port: PORT, protocol });
  const popped = r.blpop("queue", 3);
  await new Promise((res) => setTimeout(res, 300));
  await other.rpush("queue", "job");
  check(`${tag} blpop`, await popped, ["queue", "job"]);
  other.disconnect();

  r.disconnect();
}

(async () => {
  for (const protocol of [2, 3]) await scenario(protocol);
  console.log(`ioredis: ${checks} checks, ${failures.length} failed`);
  failures.forEach((f) => console.log("  FAIL", f));
  process.exit(failures.length ? 1 : 0);
})().catch((e) => {
  console.log("ioredis crashed:", e);
  process.exit(1);
});
