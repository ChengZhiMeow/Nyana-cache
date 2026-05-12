package net.nyana.cache.registry;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class Registry<K, V> {
    private final Map<K, V> map = new ConcurrentHashMap<>();
    private final boolean replace;

    public Registry(boolean replace) {
        this.replace = replace;
    }

    public Registry() {
        this(false);
    }

    public @NotNull Map<K, V> getMap() {
        return new HashMap<>(this.map);
    }

    public void clear() {
        this.map.clear();
    }

    public void register(@NotNull K key, @NotNull V value) {
        if (!this.replace && this.map.containsKey(key))
            throw new IllegalStateException("Key " + key + " is already registered");
        this.map.put(key, value);
    }

    public void unregister(@NotNull K key) {
        if (!this.map.containsKey(key))
            throw new IllegalStateException("Key " + key + " is not registered");
        this.map.remove(key);
    }

    public @Nullable V get(@NotNull K key) {
        return this.map.get(key);
    }
}
