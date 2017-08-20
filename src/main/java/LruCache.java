import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public class LruCache {
    final static long MAXIMUM_SIZE = 1000000;

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

    public long getStats() {
        return cache.size();
    }

    public static void main(String[] args) throws InterruptedException {
        String url = "http://hello.com";
        System.out.println((new StringBuilder(url).insert(4, 's')).toString());
    }
}
