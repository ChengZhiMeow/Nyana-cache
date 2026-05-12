package net.nyana.cache.redis.client;

import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.StringCodec;
import org.jetbrains.annotations.NotNull;

public class RedisClient implements AutoCloseable {
    private final io.lettuce.core.RedisClient redisClient;

    public RedisClient(@NotNull RedisConfig config) {
        RedisURI uri = RedisURI.Builder.redis(config.getHost(), config.getPort())
                .withDatabase(config.getDatabase())
                .withSsl(config.isSsl())
                .build();
        if (config.getUsername() != null && !config.getUsername().isEmpty())
            uri.setUsername(config.getUsername());
        if (config.getPassword() != null && !config.getPassword().isEmpty())
            uri.setPassword(config.getPassword().toCharArray());
        this.redisClient = io.lettuce.core.RedisClient.create(uri);
    }

    public @NotNull StatefulRedisConnection<String, byte[]> connect() {
        return this.redisClient.connect(RedisCodec.of(StringCodec.UTF8, ByteArrayCodec.INSTANCE));
    }

    @Override
    public void close() {
        this.redisClient.close();
    }
}
