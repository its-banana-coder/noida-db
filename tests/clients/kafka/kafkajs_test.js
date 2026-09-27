// kafkajs against noida-db: what most Node applications do with Kafka.
// Run through tests/clients/kafka/run.sh, which starts noida-db and sets
// NOIDA_KAFKA_PORT.
const { Kafka, Partitioners } = require("kafkajs");

const PORT = process.env.NOIDA_KAFKA_PORT;
const failures = [];
let checks = 0;
function check(name, got, want) {
  checks += 1;
  const a = JSON.stringify(got);
  const b = JSON.stringify(want);
  if (a !== b) failures.push(`${name}: got ${a}, want ${b}`);
}
const kafka = new Kafka({ clientId: "kafkajs-test", brokers: [`127.0.0.1:${PORT}`], retry: { retries: 2 } });

async function main() {
  const admin = kafka.admin();
  await admin.connect();

  // topic management
  const topic = "kafkajs-topic";
  await admin.createTopics({ topics: [{ topic, numPartitions: 3, replicationFactor: 1 }] });
  const meta = await admin.fetchTopicMetadata({ topics: [topic] });
  check("topic has 3 partitions", meta.topics[0].partitions.length, 3);
  const all = await admin.listTopics();
  check("internal topics listed", all.includes("__consumer_offsets"), true);

  // duplicate creation fails the real way
  let dupErr = null;
  try {
    await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }], validateOnly: false });
  } catch (e) {
    dupErr = e;
  }
  // kafkajs createTopics returns false rather than throwing when a topic
  // already exists and validateOnly isn't set; check via the actual API.
  const created = await admin.createTopics({ topics: [{ topic, numPartitions: 1, replicationFactor: 1 }] });
  check("recreating an existing topic is a no-op", created, false);

  // produce/consume, keyed messages land in the same partition
  const producer = kafka.producer({ createPartitioner: Partitioners.LegacyPartitioner });
  await producer.connect();
  await producer.send({
    topic,
    messages: [
      { key: "a", value: "1" },
      { key: "a", value: "2" },
      { key: "b", value: "3" },
    ],
  });

  const consumer = kafka.consumer({ groupId: "kafkajs-group" });
  await consumer.connect();
  await consumer.subscribe({ topic, fromBeginning: true });
  const received = [];
  await new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for messages")), 10000);
    consumer.run({
      eachMessage: async ({ message }) => {
        received.push(message.value.toString());
        if (received.length === 3) {
          clearTimeout(timer);
          resolve();
        }
      },
    });
  });
  check("all 3 messages consumed", received.sort(), ["1", "2", "3"]);
  // Give kafkajs a moment to resolve offsets internally before disconnect's
  // autocommit fires, so every partition's last message is actually covered.
  await new Promise((r) => setTimeout(r, 300));
  await consumer.disconnect();

  // a second consumer in the same group resumes from committed offsets
  const consumer2 = kafka.consumer({ groupId: "kafkajs-group" });
  await consumer2.connect();
  await consumer2.subscribe({ topic, fromBeginning: true });
  const receivedAgain = [];
  const p = new Promise((resolve) => {
    consumer2.run({
      eachMessage: async ({ message }) => {
        receivedAgain.push(message.value.toString());
        resolve();
      },
    });
  });
  await producer.send({ topic, messages: [{ key: "c", value: "4" }] });
  await p;
  check("resumes past committed offsets, not from the start", receivedAgain, ["4"]);
  await consumer2.disconnect();
  await producer.disconnect();

  // a consumer group of two splits partitions, then one leaves
  const groupId = "kafkajs-rebalance-group";
  const c1 = kafka.consumer({ groupId });
  const c2 = kafka.consumer({ groupId });
  await c1.connect();
  await c2.connect();
  await c1.subscribe({ topic, fromBeginning: false });
  await c2.subscribe({ topic, fromBeginning: false });
  const assignments = { c1: [], c2: [] };
  c1.on(c1.events.GROUP_JOIN, (e) => (assignments.c1 = e.payload.memberAssignment[topic] || []));
  c2.on(c2.events.GROUP_JOIN, (e) => (assignments.c2 = e.payload.memberAssignment[topic] || []));
  c1.run({ eachMessage: async () => {} });
  c2.run({ eachMessage: async () => {} });
  await new Promise((r) => setTimeout(r, 2000));
  const covered = new Set([...assignments.c1, ...assignments.c2]);
  check("two consumers split all 3 partitions between them", covered.size, 3);
  check("no partition assigned to both", assignments.c1.length + assignments.c2.length, 3);
  await c1.disconnect();
  await new Promise((r) => setTimeout(r, 1500)); // let the group rebalance
  await c2.disconnect();

  await admin.disconnect();

  console.log(`kafkajs: ${checks} checks, ${failures.length} failed`);
  failures.forEach((f) => console.log("  FAIL", f));
  process.exit(failures.length ? 1 : 0);
}

main().catch((e) => {
  console.log("kafkajs crashed:", e);
  process.exit(1);
});
