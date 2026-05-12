package net.nyana.cache.redis;

import net.nyana.cache.NyanaCache;
import net.nyana.cache.redis.client.RedisClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

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
}
