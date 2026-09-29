import io.lettuce.core.protocol.ProtocolVersion;
// Lettuce against noida-db: the async/reactive Java client, exercised through
// its synchronous facade, over RESP2 and RESP3.
import io.lettuce.core.*;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import java.util.*;

public class LettuceTest {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv("NOIDA_REDIS_PORT"));
        scenario(port, ProtocolVersion.RESP2, "resp2");
        scenario(port, ProtocolVersion.RESP3, "resp3");
        Check.report("lettuce");
    }

    static void scenario(int port, ProtocolVersion proto, String tag) throws Exception {
        RedisClient client = RedisClient.create("redis://127.0.0.1:" + port);
        client.setOptions(ClientOptions.builder().protocolVersion(proto).build());
        try (StatefulRedisConnection<String, String> conn = client.connect()) {
            RedisCommands<String, String> r = conn.sync();
            r.flushall();
            Check.check(tag + " ping", r.ping(), "PONG");

            Check.check(tag + " set", r.set("k", "v", SetArgs.Builder.ex(100)), "OK");
            Check.check(tag + " get", r.get("k"), "v");
            long ttl = r.ttl("k");
            Check.check(tag + " ttl", ttl > 0 && ttl <= 100, true);
            Check.check(tag + " incrby", r.incrby("n", 5), 5L);

            r.hset("h", "x", "1"); r.hset("h", "y", "2");
            Map<String, String> h = new LinkedHashMap<>(); h.put("x", "1"); h.put("y", "2");
            Check.check(tag + " hgetall", r.hgetall("h"), h);

            r.rpush("l", "a", "b", "c");
            Check.check(tag + " lrange", r.lrange("l", 0, -1), Arrays.asList("a", "b", "c"));
            r.sadd("s", "x", "y", "z");
            List<String> members = new ArrayList<>(r.smembers("s"));
            Collections.sort(members);
            Check.check(tag + " smembers", members, Arrays.asList("x", "y", "z"));

            r.zadd("z", 1.5, "a", 2.0, "b");
            Check.check(tag + " zscore", r.zscore("z", "b"), 2.0);

            r.multi();
            r.set("t1", "x");
            r.get("t1");
            TransactionResult tr = r.exec();
            Check.check(tag + " multi", Arrays.asList(tr.get(0), tr.get(1)), Arrays.asList("OK", "x"));

            Object ev = r.eval("return redis.call('incrby', KEYS[1], ARGV[1])", ScriptOutputType.INTEGER,
                    new String[]{"lua"}, "3");
            Check.check(tag + " eval", ev, 3L);

            Check.check(tag + " pfadd/pfcount", Arrays.asList(r.pfadd("hll", "a", "b", "c"), r.pfcount("hll")), Arrays.asList(1L, 3L));
            r.rpush("nums", "3", "1", "2");
            Check.check(tag + " sort", r.sort("nums"), Arrays.asList("1", "2", "3"));

            r.set("str", "text");
            try {
                r.incr("str");
                Check.check(tag + " error raised", "no error", "RedisCommandExecutionException");
            } catch (RedisCommandExecutionException e) {
                Check.check(tag + " error text", e.getMessage().contains("not an integer"), true);
            }
        } finally {
            client.shutdown();
        }
    }
}
