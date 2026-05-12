package net.nyana.cache.redis;

import io.lettuce.core.KeyScanCursor;
import io.lettuce.core.ScanArgs;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import net.nyana.cache.NyanaCache;
import net.nyana.cache.redis.client.RedisClient;
import net.nyana.cache.serialization.CacheSerializer;
import net.nyana.cache.serialization.SerializationRegistry;
import net.nyana.cache.service.CacheService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class RedisCacheService<V> extends CacheService<String, V> implements AutoCloseable {
    protected final String streamKey;
    private final RedisClient client;
    private final StatefulRedisConnection<String, byte[]> connection;
    private final RedisCommands<String, byte[]> commands;
    private final String namespace;
    private final CacheSerializer<V> serializer;

    public RedisCacheService(
            @NotNull NyanaCache cache,
            @NotNull RedisClient client,
            @NotNull String namespace,
            boolean sizeByParent,
            boolean entriesByParent
    ) {
        this(cache, null, client, namespace, sizeByParent, entriesByParent, (Class<V>) String.class);
    }

    public RedisCacheService(
            @NotNull NyanaCache cache,
            @NotNull RedisClient client,
            @NotNull String namespace,
            boolean sizeByParent,
            boolean entriesByParent,
            @NotNull Class<V> type
    ) {
        this(cache, null, client, namespace, sizeByParent, entriesByParent, type);
    }

    public RedisCacheService(
            @NotNull NyanaCache cache,
            @Nullable CacheService<String, V> parent,
            @NotNull RedisClient client,
            @NotNull String namespace,
            boolean sizeByParent,
            boolean entriesByParent
    ) {
        this(cache, parent, client, namespace, sizeByParent, entriesByParent, (Class<V>) String.class);
    }

    public RedisCacheService(
            @NotNull NyanaCache cache,
            @Nullable CacheService<String, V> parent,
            @NotNull RedisClient client,
            @NotNull String namespace,
            boolean sizeByParent,
            boolean entriesByParent,
            @NotNull Class<V> type
    ) {
        super(cache, parent, sizeByParent, entriesByParent);
        this.client = client;
        this.connection = client.connect();
        this.commands = this.connection.sync();
        this.namespace = namespace;
        this.streamKey = this.namespace + ":stream";

        CacheSerializer<V> serializer = (CacheSerializer<V>) this.getCache().serializationRegistry.get(type);
        if (serializer == null) throw new IllegalArgumentException("No serializer registered for " + type);
        this.serializer = serializer;
    }

    public @NotNull RedisClient getClient() {
        return this.client;
    }

    public @NotNull String getNamespace() {
        return this.namespace;
    }

    private @NotNull String dataKey(@NotNull String key) {
        return this.namespace + ":data:" + key;
    }

    public @Nullable Long remainingExpireSeconds(@NotNull String key) {
        synchronized (this) {
            Long ttl = this.commands.ttl(this.dataKey(key));
            if (ttl == null || ttl < 0L) return null;
            return ttl;
        }
    }

    public @Nullable V bytesToValue(byte @Nullable [] bytes) {
        if (bytes == null || Arrays.equals(bytes, SerializationRegistry.NULL_VALUE)) return null;
        return this.serializer.byBytes(bytes);
    }

    public @NotNull Map<String, V> match(@NotNull String pattern) {
        Map<String, V> entries = new HashMap<>();

        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = new ScanArgs().match(pattern);
        while (!cursor.isFinished()) {
            KeyScanCursor<String> scan = this.commands.scan(cursor, args);
            for (String redisKey : scan.getKeys()) {
                byte[] rawValue = this.commands.get(redisKey);
                if (rawValue == null) continue;

                String prefix = this.namespace + ":data:";
                String key = redisKey.startsWith(prefix) ? redisKey.substring(prefix.length()) : redisKey;
                entries.put(key, this.bytesToValue(rawValue));
            }
            cursor = scan;
        }

        return entries;
    }

    @Override
    protected void doPut(@NotNull String key, @Nullable V value, @Nullable Long expireSeconds) {
        boolean infinite = expireSeconds == null || expireSeconds <= 0L;

        byte[] keyData = key.getBytes(StandardCharsets.UTF_8);
        if (this.serializer == null) throw new IllegalStateException("Cache value type not resolved.");
        byte[] data = value == null ? SerializationRegistry.NULL_VALUE : this.serializer.toBytes(value);

        if (infinite) this.commands.set(this.dataKey(key), data);
        else this.commands.setex(this.dataKey(key), expireSeconds, data);

        // 广播操作
        Map<String, byte[]> body = new HashMap<>();
        body.put("op", RedisStreamOperation.PUT.value());
        body.put("key", keyData);
        body.put("value", data);
        if (!infinite) body.put("expireSeconds", String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8));
        this.commands.xadd(this.streamKey, body);
    }

    @Override
    protected void doRemove(@NotNull String key) {
        byte[] keyData = key.getBytes(StandardCharsets.UTF_8);

        this.commands.del(this.dataKey(key));

        // 广播操作
        this.commands.xadd(this.streamKey, Map.of(
                "op", RedisStreamOperation.REMOVE.value(),
                "key", keyData
        ));
    }

    @Override
    protected void doClear() {
        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = new ScanArgs().match(this.namespace + ":data:*");
        while (!cursor.isFinished()) {
            KeyScanCursor<String> scan = this.commands.scan(cursor, args);
            for (String key : scan.getKeys()) this.commands.del(key);

            cursor = scan;
        }

        // 广播操作
        this.commands.xadd(this.streamKey, Map.of(
                "op", RedisStreamOperation.CLEAR.value()
        ));
    }

    @Override
    protected boolean doContainsKey(@NotNull String key) {
        return this.commands.exists(this.dataKey(key)) > 0L;
    }

    @Override
    protected @Nullable V doGet(@NotNull String key) {
        return this.bytesToValue(this.commands.get(this.dataKey(key)));
    }

    @Override
    protected @NotNull Map<String, V> doEntries() {
        return this.match(this.namespace + ":data:*");
    }

    @Override
    protected int doSize() {
        int i = 0;

        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = new ScanArgs().match(this.namespace + ":data:*");
        while (!cursor.isFinished()) {
            KeyScanCursor<String> scan = this.commands.scan(cursor, args);
            i += scan.getKeys().size();
            cursor = scan;
        }

        return i;
    }

    @Override
    public synchronized void close() {
        this.connection.close();
    }
}
