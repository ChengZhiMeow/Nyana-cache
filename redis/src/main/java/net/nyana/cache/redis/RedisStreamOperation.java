package net.nyana.cache.redis;

import org.jetbrains.annotations.NotNull;

import java.nio.charset.StandardCharsets;

public enum RedisStreamOperation {
    PUT("put"),
    REMOVE("remove"),
    CLEAR("clear");

    private final byte[] value;

    RedisStreamOperation(@NotNull String value) {
        this.value = value.getBytes(StandardCharsets.UTF_8);
    }

    public byte @NotNull [] value() {
        return this.value;
    }
}
