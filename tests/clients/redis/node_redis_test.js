// node-redis (the official `redis` npm client, distinct from ioredis) against
// noida-db, over RESP2 and RESP3. Run through tests/clients/redis/run.sh,
// which starts the server and sets NOIDA_REDIS_PORT.
const { createClient } = require("redis");

const PORT = process.env.NOIDA_REDIS_PORT;
const failures = [];
let checks = 0;
function check(name, got, want) {
  checks += 1;
  const a = JSON.stringify(got);
  const b = JSON.stringify(want);
  if (a !== b) failures.push(`${name}: got ${a}, want ${b}`);
}

async function scenario(resp3) {
  const tag = resp3 ? "resp3" : "resp2";
  const r = createClient({ socket: { host: "127.0.0.1", port: PORT }, RESP: resp3 ? 3 : 2 });
  await r.connect();
  await r.flushAll();

  check(`${tag} ping`, await r.ping(), "PONG");
  check(`${tag} set/get`, [await r.set("k", "v", { EX: 100 }), await r.get("k")], ["OK", "v"]);
  const ttl = await r.ttl("k");
  check(`${tag} ttl`, ttl > 0 && ttl <= 100, true);
  check(`${tag} incrby`, await r.incrBy("n", 5), 5);
  check(`${tag} mset/mget`, [await r.mSet({ a: "1", b: "2" }), await r.mGet(["a", "b", "zz"])], ["OK", ["1", "2", null]]);

  await r.hSet("h", { x: "1", y: "2" });
  check(`${tag} hgetall`, await r.hGetAll("h"), { x: "1", y: "2" });
  await r.rPush("l", ["a", "b", "c"]);
  check(`${tag} lrange`, await r.lRange("l", 0, -1), ["a", "b", "c"]);
  await r.sAdd("s", ["x", "y", "z"]);
  check(`${tag} smembers`, (await r.sMembers("s")).sort(), ["x", "y", "z"]);
  await r.zAdd("z", [{ score: 1.5, value: "a" }, { score: 2, value: "b" }]);
  check(`${tag} zscore`, await r.zScore("z", "b"), 2);

  for (let i = 0; i < 30; i++) await r.set(`scan:${i}`, String(i));
  let seen = 0;
  // scanIterator yields one batch (array of keys) per SCAN call, not one key.
  for await (const batch of r.scanIterator({ MATCH: "scan:*", COUNT: 7 })) seen += batch.length;
  check(`${tag} scan iterator`, seen, 30);

  const multi = r.multi().set("p1", "1").incr("p1").get("p1");
  check(`${tag} multi`, await multi.exec(), ["OK", 2, "2"]);

  const script = { NUMBER_OF_KEYS: 1, SCRIPT: "return redis.call('incrby', KEYS[1], ARGV[1])" };
  check(`${tag} eval`, await r.eval(script.SCRIPT, { keys: ["lua"], arguments: ["3"] }), 3);

  const id = await r.xAdd("stream", "*", { f: "v" });
  const range = await r.xRange("stream", "-", "+");
  check(`${tag} xrange`, range.length === 1 && range[0].message.f === "v", true);

  check(`${tag} pfadd/pfcount`, [await r.pfAdd("hll", ["a", "b", "c"]), await r.pfCount("hll")], [1, 3]);
  await r.rPush("nums", ["3", "1", "2"]);
  check(`${tag} sort`, await r.sort("nums"), ["1", "2", "3"]);

  await r.set("str", "text");
  try {
    await r.incr("str");
    check(`${tag} error raised`, "no error", "yes error");
  } catch (e) {
    check(`${tag} error text`, e.message.includes("not an integer"), true);
  }

  const other = createClient({ socket: { host: "127.0.0.1", port: PORT }, RESP: resp3 ? 3 : 2 });
  await other.connect();
  const popped = r.blPop("queue", 3);
  await new Promise((res) => setTimeout(res, 300));
  await other.rPush("queue", "job");
  check(`${tag} blpop`, await popped, { key: "queue", element: "job" });
  await other.quit();
  await r.quit();
}

(async () => {
  for (const resp3 of [false, true]) await scenario(resp3);
  console.log(`node-redis: ${checks} checks, ${failures.length} failed`);
  failures.forEach((f) => console.log("  FAIL", f));
  process.exit(failures.length ? 1 : 0);
})().catch((e) => {
  console.log("node-redis crashed:", e);
  process.exit(1);
});
