package net.nyana.cache.serialization;

import org.jetbrains.annotations.NotNull;

public interface CacheSerializer<T> {
    byte @NotNull [] toBytes(@NotNull T value);

    @NotNull T byBytes(byte @NotNull [] bytes);
}
