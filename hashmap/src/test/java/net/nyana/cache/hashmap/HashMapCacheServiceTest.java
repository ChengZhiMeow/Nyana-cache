package net.nyana.cache.hashmap;

import net.nyana.cache.NyanaCache;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class HashMapCacheServiceTest {
    private static void log(String action, Object value) {
        System.out.println("[hashmap] " + action + ": " + value);
    }

    @Test
    void cacheOperations() {
        HashMapCacheService<String, String> service = new HashMapCacheService<>(new NyanaCache());

        HashMapCacheServiceTest.log("initial entries", service.entries());
        assertEquals(Map.of(), service.entries());

        service.put("name", "nyana");
        HashMapCacheServiceTest.log("after put name", service.entries());
        assertEquals(Map.of("name", "nyana"), service.entries());
        HashMapCacheServiceTest.log("get name", service.get("name"));
        assertEquals("nyana", service.get("name"));

        service.remove("name");
        HashMapCacheServiceTest.log("after remove name", service.entries());
        assertEquals(Map.of(), service.entries());
        HashMapCacheServiceTest.log("get removed name", service.get("name"));
        assertNull(service.get("name"));

        service.put("name", "nyana");
        service.put("type", "cache");
        HashMapCacheServiceTest.log("after put two entries", service.entries());
        assertEquals(Map.of(
                "name", "nyana",
                "type", "cache"
        ), service.entries());

        service.clear();
        HashMapCacheServiceTest.log("after clear", service.entries());
        assertEquals(Map.of(), service.entries());
    }

    @Test
    void expiredEntriesAreIgnored() throws InterruptedException {
        HashMapCacheService<String, String> service = new HashMapCacheService<>(new NyanaCache());

        service.put("name", "nyana");
        service.put("expired", "value", 1L);
        HashMapCacheServiceTest.log("before expire", service.entries());

        Thread.sleep(1100L);

        HashMapCacheServiceTest.log("after expire", service.entries());
        assertEquals(Map.of("name", "nyana"), service.entries());
        HashMapCacheServiceTest.log("get live name", service.get("name"));
        assertEquals("nyana", service.get("name"));
        HashMapCacheServiceTest.log("get expired", service.get("expired"));
        assertNull(service.get("expired"));
    }
}
