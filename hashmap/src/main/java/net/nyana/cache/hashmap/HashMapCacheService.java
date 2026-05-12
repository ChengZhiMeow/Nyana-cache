package net.nyana.cache.hashmap;

import net.nyana.cache.NyanaCache;
import net.nyana.cache.service.CacheService;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class HashMapCacheService<K, V> extends CacheService<K, V> {
    private static @Nullable Long toExpireAtMillis(@Nullable Long expireSeconds) {
        if (expireSeconds == null || expireSeconds <= 0L) return null;
        return System.currentTimeMillis() + expireSeconds * 1000L;
    }

    private final Map<K, HashMapCacheValue<V>> cache = new ConcurrentHashMap<>();

    public HashMapCacheService(@NotNull NyanaCache cache) {
        super(cache);
    }

    public HashMapCacheService(@NotNull NyanaCache cache, @Nullable CacheService<K, V> parent) {
        super(cache, parent);
    }

    protected HashMapCacheService(
            @NotNull NyanaCache cache,
            @Nullable CacheService<K, V> parent,
            boolean sizeByParent,
            boolean entriesByParent
    ) {
        super(cache, parent, sizeByParent, entriesByParent);
    }

    @Override
    protected void doPut(@NotNull K key, @Nullable V value, @Nullable Long expireSeconds) {
        this.cache.put(key, new HashMapCacheValue<>(value, HashMapCacheService.toExpireAtMillis(expireSeconds)));
    }

    @Override
    protected void doRemove(@NotNull K key) {
        this.cache.remove(key);
    }

    @Override
    protected void doClear() {
        this.cache.clear();
    }

    @Override
    protected boolean doContainsKey(@NotNull K key) {
        HashMapCacheValue<V> entry = this.cache.get(key);
        if (entry == null) return false;
        if (entry.isExpired()) {
            this.cache.remove(key, entry);
            return false;
        }

        return true;
    }

    @Override
    protected @Nullable V doGet(@NotNull K key) {
        HashMapCacheValue<V> entry = this.cache.get(key);
        if (entry == null) return null;
        if (entry.isExpired()) {
            this.cache.remove(key, entry);
            return null;
        }

        return entry.value();
    }

    @Override
    protected @NotNull Map<K, V> doEntries() {
        Map<K, V> entries = new HashMap<>();
        for (Map.Entry<K, HashMapCacheValue<V>> entry : this.cache.entrySet()) {
            HashMapCacheValue<V> value = entry.getValue();
            if (value.isExpired()) continue;

            entries.put(entry.getKey(), value.value());
        }
        return entries;
    }

    @Override
    protected int doSize() {
        return this.doEntries().size();
    }
}
