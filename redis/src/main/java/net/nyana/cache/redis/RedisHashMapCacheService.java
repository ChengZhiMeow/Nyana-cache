package net.nyana.cache.redis;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.StreamMessage;
import io.lettuce.core.XReadArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import net.nyana.cache.NyanaCache;
import net.nyana.cache.hashmap.HashMapCacheService;
import net.nyana.cache.redis.client.RedisClient;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RedisHashMapCacheService<V> extends HashMapCacheService<String, V> implements AutoCloseable {
    private final ExecutorService executor = Executors.newSingleThreadExecutor();

    private final Set<String> miss = ConcurrentHashMap.newKeySet();
    private final RedisCacheService<V> redis;
    private final StatefulRedisConnection<String, byte[]> streamConnection;
    private final RedisCommands<String, byte[]> streamCommands;
    private volatile boolean recordMiss;
    private volatile boolean closed;
    private volatile String lastStreamId = "$";

    public RedisHashMapCacheService(
            @NotNull NyanaCache cache,
            @NotNull RedisClient client,
            @NotNull String namespace
    ) {
        this(cache, client, namespace, true);
    }

    public RedisHashMapCacheService(
            @NotNull NyanaCache cache,
            @NotNull RedisClient client,
            @NotNull String namespace,
            boolean recordMiss
    ) {
        super(cache);
        this.redis = new RedisCacheService<>(cache, client, namespace, false, false);
        this.streamConnection = client.connect();
        this.streamCommands = this.streamConnection.sync();
        this.recordMiss = recordMiss;
    }

    public @NotNull Set<String> getMiss() {
        return this.miss;
    }

    public void setRecordMiss(boolean recordMiss) {
        this.recordMiss = recordMiss;
    }

    @Override
    public void init() {
        for (Map.Entry<String, V> entry : this.redis.entries().entrySet()) {
            super.doPut(entry.getKey(), entry.getValue(), this.redis.remainingExpireSeconds(entry.getKey()));
        }
        this.lastStreamId = this.lastStreamId();

        Thread thread = new Thread(() -> {
            while (!this.closed) {
                try {
                    List<StreamMessage<String, byte[]>> messages = this.streamCommands.xread(
                            XReadArgs.Builder.block(Duration.ofMillis(1000L)),
                            XReadArgs.StreamOffset.from(this.redis.getNamespace() + ":stream", this.lastStreamId)
                    );

                    for (StreamMessage<String, byte[]> message : messages) {
                        this.lastStreamId = message.getId();
                        this.accept(message.getBody());
                    }
                } catch (Throwable e) {
                    if (!this.closed) throw e;
                }
            }
        }, this.redis.getNamespace() + "-stream-listener");
        thread.setDaemon(true);
        thread.start();
    }

    private @NotNull String lastStreamId() {
        if (this.streamCommands.xlen(this.redis.streamKey) <= 0L) return "$";

        List<StreamMessage<String, byte[]>> messages = this.streamCommands.xrevrange(
                this.redis.streamKey,
                Range.unbounded(),
                Limit.from(1L)
        );
        if (messages.isEmpty()) return "$";
        return messages.getFirst().getId();
    }

    @Override
    protected void doPut(
            @NotNull String key,
            @Nullable V value,
            @Nullable Long expireSeconds
    ) {
        this.miss.remove(key);
        CompletableFuture.runAsync(() -> this.redis.doPut(key, value, expireSeconds), this.executor)
                .thenRun(() -> super.doPut(key, value, expireSeconds));
    }

    @Override
    protected void doRemove(@NotNull String key) {
        this.miss.remove(key);
        CompletableFuture.runAsync(() -> this.redis.doRemove(key), this.executor)
                .thenRun(() -> super.doRemove(key));
    }

    @Override
    protected void doClear() {
        this.miss.clear();
        CompletableFuture.runAsync(this.redis::doClear, this.executor)
                .thenRun(super::doClear);
    }

    @Override
    protected boolean doContainsKey(@NotNull String key) {
        if (super.doContainsKey(key)) return true;
        boolean contains = this.redis.doContainsKey(key);
        if (!contains && this.recordMiss) this.miss.add(key);
        return contains;
    }

    @Override
    protected @Nullable V doGet(@NotNull String key) {
        if (super.doContainsKey(key)) return super.doGet(key);
        if (!this.doContainsKey(key)) return null;
        return this.redis.doGet(key);
    }

    private void accept(@NotNull Map<String, byte[]> body) {
        byte[] op = body.get("op");
        byte[] keyData = body.get("key");

        if (Arrays.equals(op, RedisStreamOperation.CLEAR.value())) {
            this.miss.clear();
            super.doClear();
            return;
        }

        if (keyData == null) return;
        String key = new String(keyData, StandardCharsets.UTF_8);
        if (Arrays.equals(op, RedisStreamOperation.PUT.value())) {
            byte[] expireData = body.get("expireSeconds");
            Long expire = expireData != null ? Long.parseLong(new String(expireData, StandardCharsets.UTF_8)) : null;

            this.miss.remove(key);
            super.doPut(key, this.redis.bytesToValue(body.get("value")), expire);
        } else if (Arrays.equals(op, RedisStreamOperation.REMOVE.value())) {
            this.miss.remove(key);
            super.doRemove(key);
        }
    }

    @Override
    public void close() {
        this.closed = true;
        this.executor.shutdownNow();
        this.streamConnection.close();
        this.redis.close();
    }
}
