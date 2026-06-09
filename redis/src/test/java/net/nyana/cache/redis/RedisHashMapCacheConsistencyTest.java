package net.nyana.cache.redis;

import net.nyana.cache.NyanaCache;
import net.nyana.cache.redis.client.RedisClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@EnabledIfSystemProperty(named = RedisTestSupport.ENABLED_PROPERTY, matches = "true")
class RedisHashMapCacheConsistencyTest {
    private static void log(String namespace, String action, Object value) {
        System.out.println("[redis-hashmap][" + namespace + "] " + action + ": " + value);
    }

    @Test
    void streamKeepsLocalCachesConsistent() throws InterruptedException {
        String namespace = RedisTestSupport.namespace();
        RedisHashMapCacheConsistencyTest.log(namespace, "namespace", namespace);

        try (
                RedisClient client = RedisTestSupport.client();
                TrackingRedisHashMapCacheService localA = new TrackingRedisHashMapCacheService(
                        new NyanaCache(),
                        client,
                        namespace
                );
                TrackingRedisHashMapCacheService localB = new TrackingRedisHashMapCacheService(
                        new NyanaCache(),
                        client,
                        namespace
                )
        ) {
            localA.init();
            localB.init();
            RedisHashMapCacheConsistencyTest.log(namespace, "localA init entries", localA.entries());
            RedisHashMapCacheConsistencyTest.log(namespace, "localB init entries", localB.entries());

            localA.put("name", "nyana");
            RedisTestSupport.await(() -> localA.entries().equals(Map.of("name", "nyana")));
            RedisTestSupport.await(() -> localB.entries().equals(Map.of("name", "nyana")));
            RedisHashMapCacheConsistencyTest.log(namespace, "localA after put name", localA.entries());
            RedisHashMapCacheConsistencyTest.log(namespace, "localB after stream put name", localB.entries());

            assertEquals("nyana", localB.get("name"));
            assertEquals(CacheLayer.HASHMAP, localB.lastReadLayer);
            RedisHashMapCacheConsistencyTest.log(namespace, "localB get name layer", localB.lastReadLayer);

            localA.remove("name");
            RedisTestSupport.await(() -> localA.entries().isEmpty());
            RedisTestSupport.await(() -> localB.entries().isEmpty());
            RedisHashMapCacheConsistencyTest.log(namespace, "localA after remove name", localA.entries());
            RedisHashMapCacheConsistencyTest.log(namespace, "localB after stream remove name", localB.entries());
            assertNull(localB.get("name"));
            RedisHashMapCacheConsistencyTest.log(namespace, "localB get removed name layer", localB.lastReadLayer);

            localA.put("name", "nyana");
            localA.put("type", "cache");
            RedisTestSupport.await(() -> localB.entries().equals(Map.of(
                    "name", "nyana",
                    "type", "cache"
            )));
            RedisHashMapCacheConsistencyTest.log(namespace, "localB after stream put two entries", localB.entries());

            localA.clear();
            RedisTestSupport.await(() -> localA.entries().isEmpty());
            RedisTestSupport.await(() -> localB.entries().isEmpty());
            RedisHashMapCacheConsistencyTest.log(namespace, "localA after clear", localA.entries());
            RedisHashMapCacheConsistencyTest.log(namespace, "localB after stream clear", localB.entries());
        }
    }

    @Test
    void canReadThroughRedisWithoutUpdatingLocalCache() throws InterruptedException {
        String namespace = RedisTestSupport.namespace();
        RedisHashMapCacheConsistencyTest.log(namespace, "namespace", namespace);

        try (
                RedisClient client = RedisTestSupport.client();
                RedisCacheService<String> redis = new RedisCacheService<>(
                        new NyanaCache(),
                        client,
                        namespace,
                        false,
                        false
                );
                TrackingRedisHashMapCacheService local = new TrackingRedisHashMapCacheService(
                        new NyanaCache(),
                        client,
                        namespace
                )
        ) {
            redis.clear();
            redis.put("name", "nyana");
            RedisHashMapCacheConsistencyTest.log(namespace, "redis put name", redis.entries());
            RedisHashMapCacheConsistencyTest.log(namespace, "local before read-through", local.entries());

            assertEquals("nyana", local.get("name"));
            assertEquals(CacheLayer.REDIS, local.lastReadLayer);
            RedisHashMapCacheConsistencyTest.log(namespace, "local get name layer", local.lastReadLayer);
            assertEquals(Map.of(), local.entries());
            RedisHashMapCacheConsistencyTest.log(namespace, "local after read-through", local.entries());
        }
    }

    @Test
    void missKeysAreRecordedWhenEnabled() {
        String namespace = RedisTestSupport.namespace();
        RedisHashMapCacheConsistencyTest.log(namespace, "namespace", namespace);

        try (
                RedisClient client = RedisTestSupport.client();
                RedisCacheService<String> redis = new RedisCacheService<>(
                        new NyanaCache(),
                        client,
                        namespace,
                        false,
                        false
                );
                TrackingRedisHashMapCacheService local = new TrackingRedisHashMapCacheService(
                        new NyanaCache(),
                        client,
                        namespace
                )
        ) {
            redis.clear();

            assertNull(local.get("missing"));
            assertTrue(local.getMiss().contains("missing"));
            RedisHashMapCacheConsistencyTest.log(namespace, "miss after missing get", local.getMiss());

            assertFalse(local.containsKey("other"));
            assertTrue(local.getMiss().contains("other"));
            RedisHashMapCacheConsistencyTest.log(namespace, "miss after missing contains", local.getMiss());

            redis.put("target", null);
            assertNull(local.get("target"));
            assertFalse(local.getMiss().contains("target"));
            RedisHashMapCacheConsistencyTest.log(namespace, "miss after null target get", local.getMiss());

            local.put("missing", "value");
            assertFalse(local.getMiss().contains("missing"));
            RedisHashMapCacheConsistencyTest.log(namespace, "miss after put missing", local.getMiss());
        }
    }

    @Test
    void readsCanBeLimitedToLocalHashMap() {
        String namespace = RedisTestSupport.namespace();
        RedisHashMapCacheConsistencyTest.log(namespace, "namespace", namespace);

        try (
                RedisClient client = RedisTestSupport.client();
                RedisCacheService<String> redis = new RedisCacheService<>(
                        new NyanaCache(),
                        client,
                        namespace,
                        false,
                        false
                );
                TrackingRedisHashMapCacheService local = new TrackingRedisHashMapCacheService(
                        new NyanaCache(),
                        client,
                        namespace,
                        true
                )
        ) {
            redis.clear();
            redis.put("name", "nyana");

            assertNull(local.get("name"));
            assertEquals(CacheLayer.NONE, local.lastReadLayer);
            assertTrue(local.getMiss().contains("name"));
            assertFalse(local.containsKey("name"));
            assertEquals(Map.of(), local.entries());
            assertEquals(0, local.size());
            assertEquals("nyana", redis.get("name"));
            RedisHashMapCacheConsistencyTest.log(namespace, "local-only reads miss", local.getMiss());

            local.setAlwaysReadFromHashMap(false);
            assertEquals("nyana", local.get("name"));
            assertEquals(CacheLayer.REDIS, local.lastReadLayer);
            RedisHashMapCacheConsistencyTest.log(namespace, "read-through get layer", local.lastReadLayer);
        }
    }

    @Test
    void expiredEntriesAreIgnoredAfterStreamSync() throws InterruptedException {
        String namespace = RedisTestSupport.namespace();
        RedisHashMapCacheConsistencyTest.log(namespace, "namespace", namespace);

        try (
                RedisClient client = RedisTestSupport.client();
                TrackingRedisHashMapCacheService localA = new TrackingRedisHashMapCacheService(
                        new NyanaCache(),
                        client,
                        namespace
                );
                TrackingRedisHashMapCacheService localB = new TrackingRedisHashMapCacheService(
                        new NyanaCache(),
                        client,
                        namespace
                )
        ) {
            localA.init();
            localB.init();
            RedisHashMapCacheConsistencyTest.log(namespace, "localA init entries", localA.entries());
            RedisHashMapCacheConsistencyTest.log(namespace, "localB init entries", localB.entries());

            localA.put("name", "nyana");
            localA.put("expired", "value", 1L);

            RedisTestSupport.await(() -> localA.entries().equals(Map.of(
                    "name", "nyana",
                    "expired", "value"
            )));
            RedisTestSupport.await(() -> localB.entries().equals(Map.of(
                    "name", "nyana",
                    "expired", "value"
            )));
            RedisHashMapCacheConsistencyTest.log(namespace, "localA before expire", localA.entries());
            RedisHashMapCacheConsistencyTest.log(namespace, "localB before expire", localB.entries());
            RedisTestSupport.await(() -> localB.get("expired") == null);
            RedisHashMapCacheConsistencyTest.log(namespace, "localB get expired layer", localB.lastReadLayer);

            assertEquals(Map.of("name", "nyana"), localA.entries());
            assertEquals(Map.of("name", "nyana"), localB.entries());
            RedisHashMapCacheConsistencyTest.log(namespace, "localA after expire", localA.entries());
            RedisHashMapCacheConsistencyTest.log(namespace, "localB after expire", localB.entries());
        }
    }

    private enum CacheLayer {
        HASHMAP,
        REDIS,
        NONE
    }

    private static final class TrackingRedisHashMapCacheService extends RedisHashMapCacheService<String> {
        private boolean alwaysReadFromHashMap;
        private CacheLayer lastReadLayer;

        private TrackingRedisHashMapCacheService(
                NyanaCache cache,
                RedisClient client,
                String namespace
        ) {
            this(cache, client, namespace, false);
        }

        private TrackingRedisHashMapCacheService(
                NyanaCache cache,
                RedisClient client,
                String namespace,
                boolean alwaysReadFromHashMap
        ) {
            super(cache, client, namespace, alwaysReadFromHashMap);
            this.alwaysReadFromHashMap = alwaysReadFromHashMap;
        }

        @Override
        public void setAlwaysReadFromHashMap(boolean alwaysReadFromHashMap) {
            super.setAlwaysReadFromHashMap(alwaysReadFromHashMap);
            this.alwaysReadFromHashMap = alwaysReadFromHashMap;
        }

        @Override
        protected String doGet(String key) {
            if (super.doEntries().containsKey(key)) this.lastReadLayer = CacheLayer.HASHMAP;
            else this.lastReadLayer = this.alwaysReadFromHashMap ? CacheLayer.NONE : CacheLayer.REDIS;
            String value = super.doGet(key);
            System.out.println("[redis-hashmap] get key=" + key + ", layer=" + this.lastReadLayer + ", value=" + value);
            return value;
        }
    }
}
