package net.nyana.cache.service;

import net.nyana.cache.NyanaCache;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;

public abstract class CacheService<K, V> {
    protected final CacheService<K, V> parent;
    private final NyanaCache cache;
    private final boolean sizeByParent;
    private final boolean entriesByParent;

    protected CacheService(@NotNull NyanaCache cache) {
        this(cache, null);
    }

    protected CacheService(@NotNull NyanaCache cache, @Nullable CacheService<K, V> parent) {
        this(cache, parent, false, false);
    }

    protected CacheService(
            @NotNull NyanaCache cache,
            @Nullable CacheService<K, V> parent,
            boolean sizeByParent,
            boolean entriesByParent
    ) {
        this.cache = cache;
        this.parent = parent;
        this.sizeByParent = sizeByParent;
        this.entriesByParent = entriesByParent;
    }

    public CacheService<K, V> getParent() {
        return this.parent;
    }

    public @NotNull NyanaCache getCache() {
        return this.cache;
    }

    public void init() {
    }

    public void put(@NotNull K key, @Nullable V value) {
        this.put(key, value, null);
    }

    public void put(@NotNull K key, @Nullable V value, @Nullable Long expireSeconds) {
        if (this.parent != null) this.parent.put(key, value, expireSeconds);
        this.doPut(key, value, expireSeconds);
    }

    public void remove(@NotNull K key) {
        if (this.parent != null) this.parent.remove(key);
        this.doRemove(key);
    }

    public void clear() {
        if (this.parent != null) this.parent.clear();
        this.doClear();
    }

    public boolean containsKey(@NotNull K key) {
        boolean contains = this.doContainsKey(key);
        if (!contains && this.parent != null) return this.parent.containsKey(key);
        return contains;
    }

    @Nullable
    public V get(@NotNull K key) {
        V value = this.doGet(key);
        if (value == null && this.parent != null) return this.parent.get(key);
        return value;
    }

    @NotNull
    public Map<K, V> entries() {
        if (this.entriesByParent && this.parent != null) return new HashMap<>(this.parent.entries());
        return new HashMap<>(this.doEntries());
    }

    public int size() {
        if (this.sizeByParent && this.parent != null) return this.parent.doSize();
        return this.doSize();
    }

    protected abstract void doPut(@NotNull K key, @Nullable V value, @Nullable Long expireSeconds);

    protected abstract void doRemove(@NotNull K key);

    protected abstract void doClear();

    protected abstract boolean doContainsKey(@NotNull K key);

    @Nullable
    protected abstract V doGet(@NotNull K key);

    @NotNull
    protected abstract Map<K, V> doEntries();

    protected abstract int doSize();
}
