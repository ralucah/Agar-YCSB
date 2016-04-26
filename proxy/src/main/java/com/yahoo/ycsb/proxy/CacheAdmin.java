package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

public class CacheAdmin {
    protected static Logger logger = Logger.getLogger(CacheAdmin.class);

    // full data size, in bytes
    private final long FIELDLENGTH;
    private final long CACHE_SIZE_MAX;

    // <key, cache>
    private Map<String, String> registry;

    private MemcachedConnection memConnection;

    public CacheAdmin(String memHost, long cachesizeMax, long fieldlength) {
        try {
            memConnection = new MemcachedConnection(memHost);
        } catch (ClientException e) {
            logger.error("Error establishing connection to Memcached.");
        }

        CACHE_SIZE_MAX = cachesizeMax;
        FIELDLENGTH = fieldlength;

        registry = new HashMap<String, String>();
    }

    /*public String printCacheRegistry() {
        // TODO
        return "";
    }*/

    /*private Cache mapToCache(String key) {
        int cacheId = Math.abs(key.hashCode()) % caches.size();
        return caches.get(cacheId);
    }

    // TODO periodically
    private void adjutPopularities() {
        for (Map.Entry<String, Stats> entry : registry.entrySet()) {
            Stats info = entry.getValue();
            double newPopularity = (info.getAccesses() * 100) / totalAccesses;
            info.setPopularity(newPopularity);
        }
    }

    // TODO monitor cache and do this when cache is full
    private void degrade() {
        // keys should be sorted by popularity
        // get the least popular item
    }

    public ProxyReply computeReply(String key) {
        ProxyReply reply = new ProxyReply();

        Stats info = registry.get(key);
        totalAccesses++;
        // if key not yet in cache registry
        if (info == null) {

            // map key to cache
            Cache cache = mapToCache(key);

            // if the cache has room for a new item
            if (cache.isFull() == false) {
                // prepare for new cache entry!
                cache.increment();
                info = new Stats(key, cache.getName());
                registry.put(key, info);
                reply.setKeyToCache(info.getKeyToCache());
            } else
                // no room for new item, instruct client to not cache
                reply.setCacheFull();
        } else {
            // data already in cache registry, just copy it to reply
            reply.setKeyToCache(info.getKeyToCache());
            info.incrementAccesses();
        }

        return reply;
    }*/
}
