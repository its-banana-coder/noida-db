// Spring Data Redis against noida-db: the template API most Spring Boot apps
// use, built on Lettuce, over RESP2 and RESP3.
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import io.lettuce.core.protocol.ProtocolVersion;

import java.time.Duration;
import java.util.*;

public class SpringDataRedisTest {
    public static void main(String[] args) throws Exception {
        int port = Integer.parseInt(System.getenv("NOIDA_REDIS_PORT"));
        scenario(port, ProtocolVersion.RESP2, "resp2");
        scenario(port, ProtocolVersion.RESP3, "resp3");
        Check.report("spring-data-redis");
    }

    static void scenario(int port, ProtocolVersion proto, String tag) throws Exception {
        RedisStandaloneConfiguration conf = new RedisStandaloneConfiguration("127.0.0.1", port);
        LettuceClientConfiguration clientConfig = LettuceClientConfiguration.builder()
                .clientOptions(io.lettuce.core.ClientOptions.builder().protocolVersion(proto).build())
                .build();
        LettuceConnectionFactory factory = new LettuceConnectionFactory(conf, clientConfig);
        factory.afterPropertiesSet();
        try {
            StringRedisTemplate r = new StringRedisTemplate(factory);
            r.afterPropertiesSet();
            r.getConnectionFactory().getConnection().serverCommands().flushAll();

            Check.check(tag + " set/get", r.opsForValue().get("k"), null);
            r.opsForValue().set("k", "v", Duration.ofSeconds(100));
            Check.check(tag + " get", r.opsForValue().get("k"), "v");
            long ttl = r.getExpire("k");
            Check.check(tag + " ttl", ttl > 0 && ttl <= 100, true);
            Check.check(tag + " incrby", r.opsForValue().increment("n", 5), 5L);

            r.opsForHash().put("h", "x", "1");
            r.opsForHash().put("h", "y", "2");
            Map<Object, Object> h = r.opsForHash().entries("h");
            Check.check(tag + " hgetall", new TreeMap<>(h), new TreeMap<>(Map.of("x", "1", "y", "2")));

            r.opsForList().rightPushAll("l", "a", "b", "c");
            Check.check(tag + " lrange", r.opsForList().range("l", 0, -1), Arrays.asList("a", "b", "c"));

            r.opsForSet().add("s", "x", "y", "z");
            List<String> members = new ArrayList<>(r.opsForSet().members("s"));
            Collections.sort(members);
            Check.check(tag + " smembers", members, Arrays.asList("x", "y", "z"));

            r.opsForZSet().add("z", "a", 1.5);
            r.opsForZSet().add("z", "b", 2.0);
            Check.check(tag + " zscore", r.opsForZSet().score("z", "b"), 2.0);

            for (int i = 0; i < 30; i++) r.opsForValue().set("scan:" + i, String.valueOf(i));
            int seen = 0;
            try (Cursor<byte[]> cursor = r.getConnectionFactory().getConnection()
                    .keyCommands().scan(ScanOptions.scanOptions().match("scan:*").count(7).build())) {
                while (cursor.hasNext()) {
                    cursor.next();
                    seen++;
                }
            }
            Check.check(tag + " scan", seen, 30);

            List<Object> tx = r.execute(new org.springframework.data.redis.core.SessionCallback<List<Object>>() {
                @Override
                @SuppressWarnings("unchecked")
                public List<Object> execute(org.springframework.data.redis.core.RedisOperations ops) {
                    ops.multi();
                    ops.opsForValue().set("t1", "x");
                    ops.opsForValue().get("t1");
                    return ops.exec();
                }
            });
            Check.check(tag + " multi", tx, Arrays.asList(true, "x"));

            Object ev = r.execute(new org.springframework.data.redis.core.script.DefaultRedisScript<>(
                    "return redis.call('incrby', KEYS[1], ARGV[1])", Long.class),
                    Collections.singletonList("lua"), "3");
            Check.check(tag + " eval", ev, 3L);

            r.opsForValue().set("str", "text");
            try {
                r.opsForValue().increment("str");
                Check.check(tag + " error raised", "no error", "yes error");
            } catch (Exception e) {
                Throwable c = e;
                StringBuilder chain = new StringBuilder();
                while (c != null) { chain.append(c.getClass().getSimpleName()).append(": ").append(c.getMessage()).append(" | "); c = c.getCause(); }
                Check.check(tag + " error text", chain.toString().contains("not an integer"), true);
            }
        } finally {
            factory.destroy();
        }
    }
}
