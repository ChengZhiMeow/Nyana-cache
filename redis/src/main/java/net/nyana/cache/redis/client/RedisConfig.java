package net.nyana.cache.redis.client;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class RedisConfig {
    public static @NotNull Builder builder(@NotNull String host) {
        return new Builder(host);
    }

    private final String host;
    private final int port;
    private final int database;
    private final String username;
    private final String password;
    private final boolean ssl;

    private RedisConfig(@NotNull Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.database = builder.database;
        this.username = builder.username;
        this.password = builder.password;
        this.ssl = builder.ssl;
    }

    public @NotNull String getHost() {
        return this.host;
    }

    public int getPort() {
        return this.port;
    }

    public int getDatabase() {
        return this.database;
    }

    public @Nullable String getUsername() {
        return this.username;
    }

    public @Nullable String getPassword() {
        return this.password;
    }

    public boolean isSsl() {
        return this.ssl;
    }

    public static class Builder {
        private final String host;
        private int port = 6379;
        private int database = 0;
        private String username;
        private String password;
        private boolean ssl = false;

        private Builder(@NotNull String host) {
            this.host = host;
        }

        public @NotNull Builder port(int port) {
            this.port = port;
            return this;
        }

        public @NotNull Builder database(int database) {
            this.database = database;
            return this;
        }

        public @NotNull Builder username(@Nullable String username) {
            this.username = username;
            return this;
        }

        public @NotNull Builder password(@Nullable String password) {
            this.password = password;
            return this;
        }

        public @NotNull Builder auth(@Nullable String username, @Nullable String password) {
            this.username = username;
            this.password = password;
            return this;
        }

        public @NotNull Builder ssl(boolean ssl) {
            this.ssl = ssl;
            return this;
        }

        public @NotNull RedisConfig build() {
            return new RedisConfig(this);
        }
    }
}
