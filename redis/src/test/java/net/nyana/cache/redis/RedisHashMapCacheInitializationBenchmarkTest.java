package net.nyana.cache.redis;

import net.nyana.cache.NyanaCache;
import net.nyana.cache.redis.client.RedisClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import static org.junit.jupiter.api.Assertions.assertEquals;

@EnabledIfSystemProperty(named = RedisTestSupport.BENCHMARK_PROPERTY, matches = "true")
class RedisHashMapCacheInitializationBenchmarkTest {
    private static final int ENTRY_COUNT = 100_000;
    private static final long EXPIRE_SECONDS = 3_600L;

    @Test
    void initializesTtlMetadataForOneHundredThousandEntriesAfterRestart() {
        String namespace = RedisTestSupport.namespace();
        System.out.println("[redis-benchmark][" + namespace + "] preparing " + ENTRY_COUNT + " ttl entries");

        try (RedisClient client = RedisTestSupport.client()) {
            RedisCacheService<String> redis = new RedisCacheService<>(
                    new NyanaCache(),
                    client,
                    namespace,
                    false,
                    false
            );
            try {
                redis.clear();

                long writeStart = System.nanoTime();
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    redis.put("key-" + i, "value-" + i, EXPIRE_SECONDS);
                }
                long writeMillis = (System.nanoTime() - writeStart) / 1_000_000L;
                System.out.println("[redis-benchmark][" + namespace + "] writeMillis=" + writeMillis);
            } finally {
                redis.close();
            }

            try (RedisHashMapCacheService<String> local = new RedisHashMapCacheService<>(
                    new NyanaCache(),
                    client,
                    namespace
            )) {
                long initStart = System.nanoTime();
                local.init();
                long initMillis = (System.nanoTime() - initStart) / 1_000_000L;

                assertEquals(ENTRY_COUNT, local.size());
                System.out.println("[redis-benchmark][" + namespace + "] initMillis=" + initMillis);
                System.out.println("[redis-benchmark][" + namespace + "] entries=" + local.size());
            } finally {
                try (RedisCacheService<String> cleanup = new RedisCacheService<>(
                        new NyanaCache(),
                        client,
                        namespace,
                        false,
                        false
                )) {
                    cleanup.clear();
                }
            }
        }
    }
}
