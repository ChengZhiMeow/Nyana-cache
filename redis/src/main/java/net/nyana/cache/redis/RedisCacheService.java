package net.nyana.cache.redis;

import io.lettuce.core.*;
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
import java.util.*;

public class RedisCacheService<V> extends CacheService<String, V> implements AutoCloseable {
    private static final int STORAGE_VERSION = 2;
    private static final long STREAM_EXPIRE_SECONDS = 60L;
    private static final long STREAM_RETENTION_MILLIS = STREAM_EXPIRE_SECONDS * 1000L;

    protected final String streamKey;
    private final RedisClient client;
    private final StatefulRedisConnection<String, byte[]> connection;
    private final RedisCommands<String, byte[]> commands;
    private final String namespace;
    private final CacheSerializer<V> serializer;
    private final CacheSerializer<Integer> versionSerializer;
    private final CacheSerializer<Long> ttlSerializer;

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

        this.versionSerializer = (CacheSerializer<Integer>) this.getCache().serializationRegistry.get(Integer.class);
        this.ttlSerializer = (CacheSerializer<Long>) this.getCache().serializationRegistry.get(Long.class);
        this.migrateStorageIfNeeded();
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

    private @NotNull String ttlKey(@NotNull String key) {
        return this.namespace + ":ttl:" + key;
    }

    private @Nullable Long remainingSeconds(byte @Nullable [] expireAtMillisData, long nowMillis) {
        if (expireAtMillisData == null) return null;
        long expireAtMillis = this.ttlSerializer.byBytes(expireAtMillisData);
        if (expireAtMillis < 0L) return expireAtMillis;
        long remainingMillis = expireAtMillis - nowMillis;
        if (remainingMillis <= 0L) return null;
        return Math.max(1L, (remainingMillis + 999L) / 1000L);
    }

    private synchronized void migrateStorageIfNeeded() {
        String versionKey = this.namespace + ":version";
        byte[] versionData = this.commands.get(versionKey);
        int version = versionData == null ? 1 : this.versionSerializer.byBytes(versionData);

        if (version == 1) {
            this.commands.eval(
                    """
                            local version_key = KEYS[1]
                            local storage_version = ARGV[1]
                            local data_prefix = ARGV[2]
                            local ttl_prefix = ARGV[3]
                            local infinite_ttl = ARGV[4]
                            local now_millis = tonumber(ARGV[5])
                            local function long_bytes(value)
                                local bytes = {}
                                for i = 8, 1, -1 do
                                    bytes[i] = string.char(math.floor(value % 256))
                                    value = math.floor(value / 256)
                                end
                                return table.concat(bytes)
                            end
                            local cursor = '0'
                            repeat
                                local result = redis.call('SCAN', cursor, 'MATCH', data_prefix .. '*', 'COUNT', 1000)
                                cursor = result[1]
                                for _, data_key in ipairs(result[2]) do
                                    local ttl = redis.call('TTL', data_key)
                                    local ttl_key = ttl_prefix .. string.sub(data_key, string.len(data_prefix) + 1)
                                    if ttl > 0 then
                                        redis.call('SETEX', ttl_key, ttl, long_bytes(now_millis + ttl * 1000))
                                    elseif ttl == -1 then
                                        redis.call('SET', ttl_key, infinite_ttl)
                                    else
                                        redis.call('DEL', ttl_key)
                                    end
                                end
                            until cursor == '0'
                            redis.call('SET', version_key, storage_version)
                            return 1
                            """,
                    ScriptOutputType.INTEGER,
                    new String[]{versionKey},
                    this.versionSerializer.toBytes(RedisCacheService.STORAGE_VERSION),
                    (this.namespace + ":data:").getBytes(StandardCharsets.UTF_8),
                    (this.namespace + ":ttl:").getBytes(StandardCharsets.UTF_8),
                    this.ttlSerializer.toBytes(-1L),
                    String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8)
            );
        }
    }

    public @Nullable Long remainingExpireSeconds(@NotNull String key) {
        synchronized (this) {
            return this.remainingSeconds(this.commands.get(this.ttlKey(key)), System.currentTimeMillis());
        }
    }

    public @NotNull Map<String, Long> remainingExpireSeconds() {
        synchronized (this) {
            Map<String, Long> expires = new HashMap<>();
            ScanCursor cursor = ScanCursor.INITIAL;
            String ttlPrefix = this.namespace + ":ttl:";
            ScanArgs args = new ScanArgs().match(ttlPrefix + "*").limit(1000);
            while (!cursor.isFinished()) {
                KeyScanCursor<String> scan = this.commands.scan(cursor, args);
                if (!scan.getKeys().isEmpty()) {
                    long nowMillis = System.currentTimeMillis();
                    List<KeyValue<String, byte[]>> values = this.commands.mget(scan.getKeys().toArray(String[]::new));
                    for (KeyValue<String, byte[]> keyValue : values) {
                        if (!keyValue.hasValue()) continue;
                        Long remaining = this.remainingSeconds(keyValue.getValue(), nowMillis);
                        if (remaining == null) continue;

                        String redisKey = keyValue.getKey();
                        String key = redisKey.startsWith(ttlPrefix) ? redisKey.substring(ttlPrefix.length()) : redisKey;
                        expires.put(key, remaining);
                    }
                }
                cursor = scan;
            }

            return expires;
        }
    }

    public @NotNull Map<String, Long> remainingExpireSeconds(@NotNull Collection<String> keys) {
        synchronized (this) {
            Map<String, Long> expires = new HashMap<>();
            if (keys.isEmpty()) return expires;

            List<String> keyList = List.copyOf(keys);
            long nowMillis = System.currentTimeMillis();
            List<KeyValue<String, byte[]>> ttlList = this.commands.mget(
                    keyList.stream().map(this::ttlKey).toArray(String[]::new)
            );

            for (int i = 0; i < keyList.size(); i++) {
                KeyValue<String, byte[]> keyValue = ttlList.get(i);
                if (!keyValue.hasValue()) continue;
                Long ttl = this.remainingSeconds(keyValue.getValue(), nowMillis);
                if (ttl != null) expires.put(keyList.get(i), ttl);
            }
            return expires;
        }
    }

    public @Nullable V bytesToValue(byte @Nullable [] bytes) {
        if (bytes == null || Arrays.equals(bytes, SerializationRegistry.NULL_VALUE)) return null;
        return this.serializer.byBytes(bytes);
    }

    private void publishStream(@NotNull Map<String, byte[]> body) {
        String streamId = this.commands.xadd(this.streamKey, body);
        this.trimStream(streamId);
        this.commands.expire(this.streamKey, RedisCacheService.STREAM_EXPIRE_SECONDS);
    }

    private void trimStream(@NotNull String streamId) {
        long streamMillis = this.streamMillis(streamId);
        long minMillis = Math.max(0L, streamMillis - RedisCacheService.STREAM_RETENTION_MILLIS);
        this.commands.xtrim(this.streamKey, new XTrimArgs().minId(minMillis + "-0"));
    }

    private long streamMillis(@NotNull String streamId) {
        int separator = streamId.indexOf('-');
        String millis = separator >= 0 ? streamId.substring(0, separator) : streamId;
        try {
            return Long.parseLong(millis);
        } catch (NumberFormatException e) {
            return System.currentTimeMillis();
        }
    }

    public @NotNull Map<String, V> match(@NotNull String pattern) {
        Map<String, V> entries = new HashMap<>();

        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = new ScanArgs().match(pattern).limit(1000);
        while (!cursor.isFinished()) {
            KeyScanCursor<String> scan = this.commands.scan(cursor, args);
            if (scan.getKeys().isEmpty()) {
                cursor = scan;
                continue;
            }

            List<KeyValue<String, byte[]>> values = this.commands.mget(scan.getKeys().toArray(String[]::new));
            for (KeyValue<String, byte[]> keyValue : values) {
                if (!keyValue.hasValue()) continue;
                byte[] rawValue = keyValue.getValue();

                if (rawValue == null) continue;

                String prefix = this.namespace + ":data:";
                String redisKey = keyValue.getKey();
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

        if (infinite) {
            this.commands.set(this.dataKey(key), data);
            this.commands.set(this.ttlKey(key), this.ttlSerializer.toBytes(-1L));
        } else {
            this.commands.setex(this.dataKey(key), expireSeconds, data);
            this.commands.setex(
                    this.ttlKey(key),
                    expireSeconds,
                    this.ttlSerializer.toBytes(System.currentTimeMillis() + expireSeconds * 1000L)
            );
        }

        // 广播操作
        Map<String, byte[]> body = new HashMap<>();
        body.put("op", RedisStreamOperation.PUT.value());
        body.put("key", keyData);
        body.put("value", data);
        if (!infinite) body.put("expireSeconds", String.valueOf(expireSeconds).getBytes(StandardCharsets.UTF_8));
        this.publishStream(body);
    }

    @Override
    protected void doRemove(@NotNull String key) {
        byte[] keyData = key.getBytes(StandardCharsets.UTF_8);

        this.commands.del(this.dataKey(key));
        this.commands.del(this.ttlKey(key));

        // 广播操作
        this.publishStream(Map.of(
                "op", RedisStreamOperation.REMOVE.value(),
                "key", keyData
        ));
    }

    @Override
    protected void doClear() {
        ScanCursor cursor = ScanCursor.INITIAL;
        ScanArgs args = new ScanArgs().match(this.namespace + ":data:*").limit(1000);
        while (!cursor.isFinished()) {
            KeyScanCursor<String> scan = this.commands.scan(cursor, args);
            for (String key : scan.getKeys()) this.commands.del(key);

            cursor = scan;
        }

        cursor = ScanCursor.INITIAL;
        args = new ScanArgs().match(this.namespace + ":ttl:*").limit(1000);
        while (!cursor.isFinished()) {
            KeyScanCursor<String> scan = this.commands.scan(cursor, args);
            for (String key : scan.getKeys()) this.commands.del(key);

            cursor = scan;
        }

        // 广播操作
        this.publishStream(Map.of(
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
        ScanArgs args = new ScanArgs().match(this.namespace + ":data:*").limit(1000);
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
