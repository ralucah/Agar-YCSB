package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.common.communication.ProxyReply;
import org.apache.log4j.Logger;

import java.util.*;

public class CacheAdmin {
    protected static Logger logger = Logger.getLogger(CacheAdmin.class);
    // max cache size
    private final long CACHE_SIZE_MAX;
    // data size, in bytes
    private final long fieldlength;
    // map keys to cache servers
    private NavigableMap<String, String> cacheRegistry;
    // cache size accessed from multiple threads
    private List<Cache> caches;

    public CacheAdmin(List<String> memHosts, long cachesizeMax, long fieldlength) {
        this.CACHE_SIZE_MAX = cachesizeMax;
        this.fieldlength = fieldlength;

        caches = new ArrayList<Cache>();
        for (String memHost : memHosts) {
            caches.add(new Cache(memHost));
        }

        cacheRegistry = new TreeMap<String, String>();
    }

    public String printCacheRegistry() {
        String str = "CacheRegistry: [\n";
        for (Map.Entry<String, String> entry : cacheRegistry.entrySet()) {
            str += entry.getKey() + " " + entry.getValue() + "\n";
        }
        str += "]";
        return str;
    }

    public ProxyReply computeReply(String key) {
        ProxyReply reply = new ProxyReply();

        // get set of keys starting with prefix key
        SortedMap<String, String> keyMatch = cacheRegistry.subMap(key, key + Character.MAX_VALUE);

        // if key not yet in cache registry
        if (keyMatch.size() == 0) {

            // map key to cache
            int cacheId = Math.abs(key.hashCode()) % caches.size();
            Cache cache = caches.get(cacheId);

            // if the cache has room for a new item
            long newSize = cache.getSize() + fieldlength;
            if (newSize <= CACHE_SIZE_MAX) {
                // prepare for new cache entry!
                String name = cache.getName();
                cacheRegistry.put(key, name);
                reply.addPair(key, name);
                cache.setSize(newSize);
            } else
                // no room for new item, instruct client to not cache
                reply.setCacheFull();
        } else {
            // data already in cache registry, just copy it to reply
            for (Map.Entry<String, String> entry : keyMatch.entrySet()) {
                reply.addPair(entry.getKey(), entry.getValue());
            }
        }

        return reply;
    }
}
