package net.nyana.cache.hashmap;

import org.jetbrains.annotations.Nullable;

public record HashMapCacheValue<V>(@Nullable V value, @Nullable Long expireAtMillis) {
    @Override
    public @Nullable V value() {
        return this.value;
    }

    @Override
    public @Nullable Long expireAtMillis() {
        return this.expireAtMillis;
    }

    public boolean isExpired() {
        return this.expireAtMillis != null && this.expireAtMillis <= System.currentTimeMillis();
    }
}
