// Jedis against noida-db: what most Java applications do, over RESP2 (Jedis's
// default) and RESP3.
import redis.clients.jedis.*;
import redis.clients.jedis.params.*;
import redis.clients.jedis.resps.*;
import java.util.*;

public class JedisTest {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv("NOIDA_REDIS_PORT"));
        scenario(port, RedisProtocol.RESP2, "resp2");
        scenario(port, RedisProtocol.RESP3, "resp3");
        Check.report("jedis");
    }

    static void scenario(int port, RedisProtocol proto, String tag) throws Exception {
        HostAndPort addr = new HostAndPort("127.0.0.1", port);
        JedisClientConfig cfg = DefaultJedisClientConfig.builder().protocol(proto).build();
        try (Jedis j = new Jedis(addr, cfg)) {
            j.flushAll();
            Check.check(tag + " ping", j.ping(), "PONG");

            Check.check(tag + " set/get", j.set("k", "v", SetParams.setParams().ex(100)), "OK");
            Check.check(tag + " get", j.get("k"), "v");
            long ttl = j.ttl("k");
            Check.check(tag + " ttl", ttl > 0 && ttl <= 100, true);
            Check.check(tag + " incrby", j.incrBy("n", 5), 5L);
            Check.check(tag + " incrbyfloat", j.incrByFloat("f", 1.5), 1.5);

            j.hset("h", "x", "1"); j.hset("h", "y", "2");
            Map<String, String> h = new LinkedHashMap<>(); h.put("x", "1"); h.put("y", "2");
            Check.check(tag + " hgetall", j.hgetAll("h"), h);
            Check.check(tag + " hincrby", j.hincrBy("h", "x", 4), 5L);

            j.rpush("l", "a", "b", "c");
            Check.check(tag + " lrange", j.lrange("l", 0, -1), Arrays.asList("a", "b", "c"));
            j.sadd("s", "x", "y", "z");
            List<String> members = new ArrayList<>(j.smembers("s"));
            Collections.sort(members);
            Check.check(tag + " smembers", members, Arrays.asList("x", "y", "z"));

            j.zadd("z", 1.5, "a"); j.zadd("z", 2, "b"); j.zadd("z", 3, "c");
            Check.check(tag + " zscore", j.zscore("z", "b"), 2.0);
            Check.check(tag + " zrangebyscore", j.zrangeByScore("z", 2, 3), Arrays.asList("b", "c"));

            for (int i = 0; i < 30; i++) j.set("scan:" + i, String.valueOf(i));
            String cursor = "0"; int seen = 0;
            do {
                ScanResult<String> r = j.scan(cursor, new ScanParams().match("scan:*").count(7));
                seen += r.getResult().size();
                cursor = r.getCursor();
            } while (!cursor.equals("0"));
            Check.check(tag + " scan", seen, 30);

            Transaction t = j.multi();
            t.set("t1", "x");
            t.get("t1");
            List<Object> res = t.exec();
            Check.check(tag + " multi", res, Arrays.asList("OK", "x"));

            Object ev = j.eval("return redis.call('incrby', KEYS[1], ARGV[1])", Arrays.asList("lua"), Arrays.asList("3"));
            Check.check(tag + " eval", ev, 3L);

            j.xadd("stream", XAddParams.xAddParams(), Collections.singletonMap("f", "v"));
            List<StreamEntry> range = j.xrange("stream", (StreamEntryID) null, null);
            Check.check(tag + " xrange", range.size() == 1 && "v".equals(range.get(0).getFields().get("f")), true);

            Check.check(tag + " pfadd/pfcount", Arrays.asList(j.pfadd("hll", "a", "b", "c"), j.pfcount("hll")), Arrays.asList(1L, 3L));
            j.rpush("nums", "3", "1", "2");
            Check.check(tag + " sort", j.sort("nums"), Arrays.asList("1", "2", "3"));

            j.set("str", "text");
            try {
                j.incr("str");
                Check.check(tag + " error raised", "no error", "JedisDataException");
            } catch (redis.clients.jedis.exceptions.JedisDataException e) {
                Check.check(tag + " error text", e.getMessage().contains("not an integer"), true);
            }

            byte[] blob = new byte[256];
            for (int i = 0; i < 256; i++) blob[i] = (byte) i;
            j.set("blob".getBytes(), blob);
            Check.check(tag + " binary round trip", Arrays.equals(j.get("blob".getBytes()), blob), true);
        }
    }
}
