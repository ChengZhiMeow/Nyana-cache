package net.nyana.cache.redis;

import net.nyana.cache.redis.client.RedisClient;
import net.nyana.cache.redis.client.RedisConfig;

import java.util.UUID;
import java.util.function.BooleanSupplier;

final class RedisTestSupport {
    static final String ENABLED_PROPERTY = "nyana.redis.tests";

    static RedisClient client() {
        return new RedisClient(RedisConfig.builder(System.getProperty("nyana.redis.host", "localhost"))
                .port(Integer.parseInt(System.getProperty("nyana.redis.port", "6379")))
                .database(Integer.parseInt(System.getProperty("nyana.redis.database", "0")))
                .build());
    }

    static String namespace() {
        return "nyana-cache-test:" + UUID.randomUUID();
    }

    static void await(BooleanSupplier condition) throws InterruptedException {
        long deadline = System.currentTimeMillis() + 5000L;
        while (System.currentTimeMillis() < deadline) {
            if (condition.getAsBoolean()) return;
            Thread.sleep(50L);
        }
        throw new AssertionError("Condition was not met before timeout.");
    }

    private RedisTestSupport() {
    }
}
