package net.nyana.cache;

import net.nyana.cache.serialization.SerializationRegistry;

public class NyanaCache {
    public final SerializationRegistry serializationRegistry;

    public NyanaCache() {
        this.serializationRegistry = new SerializationRegistry();
    }
}
