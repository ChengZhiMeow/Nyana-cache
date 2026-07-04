package net.nyana.cache.redis;

import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import net.nyana.cache.NyanaCache;
import net.nyana.cache.redis.client.RedisClient;
import net.nyana.cache.serialization.CacheSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@EnabledIfSystemProperty(named = RedisTestSupport.ENABLED_PROPERTY, matches = "true")
class RedisCacheServiceTest {
    private static void log(String namespace, String action, Object value) {
        System.out.println("[redis][" + namespace + "] " + action + ": " + value);
    }

    @Test
    void cacheOperations() {
        String namespace = RedisTestSupport.namespace();
        RedisCacheServiceTest.log(namespace, "namespace", namespace);

        try (
                RedisClient client = RedisTestSupport.client();
                RedisCacheService<String> service = new RedisCacheService<>(
                        new NyanaCache(),
                        client,
                        namespace,
                        false,
                        false
                )
        ) {
            service.clear();
            RedisCacheServiceTest.log(namespace, "initial entries", service.entries());
            assertEquals(Map.of(), service.entries());

            service.put("name", "nyana");
            RedisCacheServiceTest.log(namespace, "after put name", service.entries());
            assertEquals(Map.of("name", "nyana"), service.entries());
            RedisCacheServiceTest.log(namespace, "get name from redis", service.get("name"));
            assertEquals("nyana", service.get("name"));

            service.remove("name");
            RedisCacheServiceTest.log(namespace, "after remove name", service.entries());
            assertEquals(Map.of(), service.entries());
            RedisCacheServiceTest.log(namespace, "get removed name", service.get("name"));
            assertNull(service.get("name"));

            service.put("name", "nyana");
            service.put("type", "cache");
            RedisCacheServiceTest.log(namespace, "after put two entries", service.entries());
            assertEquals(Map.of(
                    "name", "nyana",
                    "type", "cache"
            ), service.entries());

            RedisCacheServiceTest.log(namespace, "match na*", service.match(namespace + ":data:na*"));
            assertEquals(Map.of("name", "nyana"), service.match(namespace + ":data:na*"));

            service.clear();
            RedisCacheServiceTest.log(namespace, "after clear", service.entries());
            assertEquals(Map.of(), service.entries());
        }
    }

    @Test
    void expiredEntriesAreIgnored() throws InterruptedException {
        String namespace = RedisTestSupport.namespace();
        RedisCacheServiceTest.log(namespace, "namespace", namespace);

        try (
                RedisClient client = RedisTestSupport.client();
                RedisCacheService<String> service = new RedisCacheService<>(
                        new NyanaCache(),
                        client,
                        namespace,
                        false,
                        false
                )
        ) {
            service.clear();
            service.put("name", "nyana");
            service.put("expired", "value", 1L);
            RedisCacheServiceTest.log(namespace, "before expire", service.entries());

            RedisTestSupport.await(() -> service.get("expired") == null);

            RedisCacheServiceTest.log(namespace, "after expire", service.entries());
            assertEquals(Map.of("name", "nyana"), service.entries());
            RedisCacheServiceTest.log(namespace, "get live name from redis", service.get("name"));
            assertEquals("nyana", service.get("name"));
            RedisCacheServiceTest.log(namespace, "get expired from redis", service.get("expired"));
            assertNull(service.get("expired"));
            RedisCacheServiceTest.log(namespace, "match all after expire", service.match(namespace + ":data:*"));
            assertEquals(Map.of("name", "nyana"), service.match(namespace + ":data:*"));
        }
    }

    @Test
    void ttlMetadataIsStoredInVersionTwoNamespace() {
        String namespace = RedisTestSupport.namespace();
        RedisCacheServiceTest.log(namespace, "namespace", namespace);

        try (
                RedisClient client = RedisTestSupport.client();
                RedisCacheService<String> service = new RedisCacheService<>(
                        new NyanaCache(),
                        client,
                        namespace,
                        false,
                        false
                );
                StatefulRedisConnection<String, byte[]> connection = client.connect()
        ) {
            RedisCommands<String, byte[]> commands = connection.sync();
            CacheSerializer<Integer> versionSerializer = (CacheSerializer<Integer>) service.getCache().serializationRegistry.get(Integer.class);
            CacheSerializer<Long> ttlSerializer = (CacheSerializer<Long>) service.getCache().serializationRegistry.get(Long.class);
            service.clear();
            service.put("name", "nyana");
            service.put("expired", "value", 30L);

            assertEquals(2, versionSerializer.byBytes(commands.get(namespace + ":version")));
            assertEquals(-1L, ttlSerializer.byBytes(commands.get(namespace + ":ttl:name")));
            assertEquals(-1L, commands.ttl(namespace + ":ttl:name"));
            assertNotNull(commands.get(namespace + ":ttl:expired"));
            assertTrue(commands.ttl(namespace + ":ttl:expired") > 0L);
            assertEquals(-1L, service.remainingExpireSeconds("name"));
            assertEquals(
                    List.of("expired", "name"),
                    service.remainingExpireSeconds().keySet().stream().sorted().toList()
            );
            assertEquals(
                    List.of("expired", "name"),
                    service.remainingExpireSeconds(List.of("name", "expired")).keySet().stream().sorted().toList()
            );
        }
    }

    @Test
    void streamEntriesAreTrimmedAndExpireAfterSixtySeconds() {
        String namespace = RedisTestSupport.namespace();
        String streamKey = namespace + ":stream";
        RedisCacheServiceTest.log(namespace, "namespace", namespace);

        try (
                RedisClient client = RedisTestSupport.client();
                StatefulRedisConnection<String, byte[]> connection = client.connect()
        ) {
            RedisCommands<String, byte[]> commands = connection.sync();
            commands.xadd(
                    streamKey,
                    new XAddArgs().id("1-0"),
                    Map.of("op", "old".getBytes(StandardCharsets.UTF_8))
            );

            try (RedisCacheService<String> service = new RedisCacheService<>(
                    new NyanaCache(),
                    client,
                    namespace,
                    false,
                    false
            )) {
                service.put("name", "nyana");
            }

            List<StreamMessage<String, byte[]>> messages = commands.xrange(streamKey, Range.unbounded());
            assertEquals(1, messages.size());
            assertArrayEquals(RedisStreamOperation.PUT.value(), messages.getFirst().getBody().get("op"));
            Long ttl = commands.ttl(streamKey);
            assertTrue(ttl > 0L && ttl <= 60L);
        }
    }

    @Test
    void missingVersionNamespaceIsMigratedToVersionTwo() {
        String namespace = RedisTestSupport.namespace();
        RedisCacheServiceTest.log(namespace, "namespace", namespace);

        try (
                RedisClient client = RedisTestSupport.client();
                StatefulRedisConnection<String, byte[]> connection = client.connect()
        ) {
            RedisCommands<String, byte[]> commands = connection.sync();
            commands.del(namespace + ":version");
            commands.set(namespace + ":data:name", "nyana".getBytes(StandardCharsets.UTF_8));
            commands.setex(namespace + ":data:expired", 30L, "value".getBytes(StandardCharsets.UTF_8));

            NyanaCache cache = new NyanaCache();
            CacheSerializer<Integer> versionSerializer = (CacheSerializer<Integer>) cache.serializationRegistry.get(Integer.class);
            CacheSerializer<Long> ttlSerializer = (CacheSerializer<Long>) cache.serializationRegistry.get(Long.class);
            try (RedisCacheService<String> service = new RedisCacheService<>(
                    cache,
                    client,
                    namespace,
                    false,
                    false
            )) {
                assertEquals(2, versionSerializer.byBytes(commands.get(namespace + ":version")));
                assertEquals(-1L, ttlSerializer.byBytes(commands.get(namespace + ":ttl:name")));
                assertEquals(-1L, commands.ttl(namespace + ":ttl:name"));
                assertNotNull(commands.get(namespace + ":ttl:expired"));
                assertEquals("nyana", service.get("name"));
                assertEquals("value", service.get("expired"));
                assertEquals(-1L, service.remainingExpireSeconds("name"));
                assertEquals(
                        List.of("expired", "name"),
                        service.remainingExpireSeconds().keySet().stream().sorted().toList()
                );
            }
        }
    }
}
