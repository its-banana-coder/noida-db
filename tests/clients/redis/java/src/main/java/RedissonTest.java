// Redisson against noida-db: its high-level distributed objects (map, list,
// set, atomic long, lock, bucket) over RESP2 and RESP3.
import org.redisson.Redisson;
import org.redisson.api.*;
import org.redisson.config.Config;
import org.redisson.config.Protocol;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class RedissonTest {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv("NOIDA_REDIS_PORT"));
        scenario(port, Protocol.RESP2, "resp2");
        scenario(port, Protocol.RESP3, "resp3");
        Check.report("redisson");
    }

    static void scenario(int port, Protocol proto, String tag) throws Exception {
        Config config = new Config();
        config.setProtocol(proto);
        config.useSingleServer().setAddress("redis://127.0.0.1:" + port);
        RedissonClient r = Redisson.create(config);
        try {
            r.getKeys().flushall();

            RBucket<String> bucket = r.getBucket("k");
            bucket.set("v");
            Check.check(tag + " bucket get", bucket.get(), "v");
            bucket.expire(java.time.Duration.ofSeconds(100));
            Check.check(tag + " bucket ttl", bucket.remainTimeToLive() > 0, true);

            RAtomicLong n = r.getAtomicLong("n");
            Check.check(tag + " atomic addAndGet", n.addAndGet(5), 5L);

            RMap<String, String> h = r.getMap("h");
            h.put("x", "1");
            h.put("y", "2");
            Check.check(tag + " map", new TreeMap<>(h), new TreeMap<>(Map.of("x", "1", "y", "2")));

            RList<String> l = r.getList("l");
            l.addAll(Arrays.asList("a", "b", "c"));
            Check.check(tag + " list", l.readAll(), Arrays.asList("a", "b", "c"));

            RSet<String> s = r.getSet("s");
            s.addAll(Arrays.asList("x", "y", "z"));
            List<String> members = new ArrayList<>(s.readAll());
            Collections.sort(members);
            Check.check(tag + " set", members, Arrays.asList("x", "y", "z"));

            RScoredSortedSet<String> z = r.getScoredSortedSet("z");
            z.add(1.5, "a");
            z.add(2.0, "b");
            Check.check(tag + " zscore", z.getScore("b"), 2.0);

            // a distributed lock, taken and released
            RLock lock = r.getLock("lock");
            boolean acquired = lock.tryLock(1, 5, TimeUnit.SECONDS);
            Check.check(tag + " lock acquired", acquired, true);
            Check.check(tag + " lock held", lock.isLocked(), true);
            lock.unlock();
            Check.check(tag + " lock released", lock.isLocked(), false);

            RScript script = r.getScript(org.redisson.client.codec.StringCodec.INSTANCE);
            Object ev = script.eval(RScript.Mode.READ_WRITE, "return redis.call('incrby', KEYS[1], ARGV[1])",
                    RScript.ReturnType.INTEGER, Collections.singletonList("lua"), "3");
            Check.check(tag + " eval", ev, 3L);

            RBucket<String> str = r.getBucket("str");
            str.set("text");
            RAtomicLong badNum = r.getAtomicLong("str");
            try {
                badNum.incrementAndGet();
                Check.check(tag + " error raised", "no error", "yes error");
            } catch (Exception e) {
                Check.check(tag + " error text", e.getMessage() != null && e.getMessage().contains("not an integer"), true);
            }
        } finally {
            r.shutdown();
        }
    }
}
