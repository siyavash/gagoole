import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public class LruCache {
    final static long MAXIMUM_SIZE = 10 * 1000 * 1000;

    private Cache<String, Object> cache = CacheBuilder.newBuilder()
            .maximumSize(MAXIMUM_SIZE)
            .expireAfterWrite(30, TimeUnit.SECONDS)
            .build();
    private Object defaultObject = new Object();

    public boolean checkIfExist(String key) {
        if (cache.getIfPresent(key) != null) {
            return true;
        } else {
            cache.put(key, defaultObject);
            return false;
        }
    }

    public long size() {
        return cache.size();
    }

}
