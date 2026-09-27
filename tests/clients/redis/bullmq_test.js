// BullMQ (the job queue most Node apps use) against noida-db. BullMQ is
// script-heavy: it runs ~50 Lua scripts, streams, sorted sets and blocking
// commands, so it exercises the server much harder than plain commands.
// Run through tests/clients/redis/run.sh, which starts the server and sets
// NOIDA_REDIS_PORT.
const { Queue, Worker, QueueEvents } = require("bullmq");

const connection = { host: "127.0.0.1", port: Number(process.env.NOIDA_REDIS_PORT) };
const failures = [];
let checks = 0;
const check = (name, got, want) => {
  checks += 1;
  const a = JSON.stringify(got), b = JSON.stringify(want);
  if (a !== b) failures.push(`${name}: got ${a}, want ${b}`);
};
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const until = async (fn, ms = 8000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (await fn()) return true; await sleep(50); }
  return false;
};

(async () => {
  const name = "q" + Date.now();
  const queue = new Queue(name, { connection });
  await queue.obliterate({ force: true }).catch(() => {});

  // add and read back
  const job = await queue.add("email", { to: "a@b.c" });
  check("job has an id", typeof job.id, "string");
  check("counts after add", (await queue.getJobCounts("waiting")).waiting, 1);
  check("getJob", (await queue.getJob(job.id)).data, { to: "a@b.c" });

  // a worker processes it
  const done = [];
  const events = new QueueEvents(name, { connection });
  await events.waitUntilReady();
  const worker = new Worker(name, async (j) => { done.push(j.data); return { ok: true }; }, { connection });
  check("job processed", await until(() => done.length === 1), true);
  await sleep(200);
  check("job completed", (await queue.getJobCounts("completed")).completed, 1);
  check("return value stored", (await queue.getJob(job.id)).returnvalue, { ok: true });

  // several jobs, in order, with a concurrency of 1
  const order = [];
  const w2 = new Worker(name + "-o", async (j) => { order.push(j.data.n); }, { connection });
  const q2 = new Queue(name + "-o", { connection });
  await q2.addBulk([1, 2, 3, 4, 5].map((n) => ({ name: "n", data: { n } })));
  check("bulk processed", await until(() => order.length === 5), true);
  check("processed in order", order, [1, 2, 3, 4, 5]);

  // retries with backoff, then failure
  let attempts = 0;
  const w3 = new Worker(name + "-r", async () => { attempts += 1; throw new Error("boom"); }, { connection });
  const q3 = new Queue(name + "-r", { connection });
  await q3.add("bad", {}, { attempts: 3, backoff: 50 });
  check("retried 3 times", await until(() => attempts === 3), true);
  await sleep(300);
  const failed = await q3.getFailed();
  check("failed job kept", [failed.length, failed[0] && failed[0].failedReason], [1, "boom"]);

  // delayed job
  const q4 = new Queue(name + "-d", { connection });
  const seen = [];
  const w4 = new Worker(name + "-d", async (j) => { seen.push(Date.now()); }, { connection });
  const t0 = Date.now();
  await q4.add("later", {}, { delay: 400 });
  check("delayed counts", (await q4.getJobCounts("delayed")).delayed, 1);
  check("delayed job ran", await until(() => seen.length === 1), true);
  check("not before its delay", seen[0] - t0 >= 350, true);

  // priorities and removal
  const q5 = new Queue(name + "-p", { connection });
  await q5.add("low", {}, { priority: 10 });
  await q5.add("high", {}, { priority: 1 });
  const p = await q5.getJobs(["prioritized"]);
  check("priorities present", p.map((j) => j.name).sort(), ["high", "low"]);
  await p.find((j) => j.name === "high").remove();
  check("removed", (await q5.getJobCounts("prioritized")).prioritized, 1);

  // pause and resume
  await q5.pause();
  check("paused", await q5.isPaused(), true);
  await q5.resume();
  check("resumed", await q5.isPaused(), false);

  for (const c of [worker, w2, w3, w4]) await c.close();
  for (const c of [queue, q2, q3, q4, q5, events]) await c.close();

  console.log(`bullmq: ${checks} checks, ${failures.length} failed`);
  failures.forEach((f) => console.log("  FAIL", f));
  process.exit(failures.length ? 1 : 0);
})().catch((e) => { console.log("bullmq crashed:", e); process.exit(1); });
