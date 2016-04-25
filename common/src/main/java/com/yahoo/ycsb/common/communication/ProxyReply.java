package com.yahoo.ycsb.common.communication;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Raluca on 24.04.16.
 */
public class ProxyReply implements Serializable {
    private static final long serialVersionUID = 7526472295622776148L;

    private Map<String, String> keyToCache;
    private CacheStatus cacheStatus;

    public ProxyReply() {
        keyToCache = new HashMap<>();
        cacheStatus = CacheStatus.CACHE_OK;
    }

    public void addPair(String key, String cache) {
        keyToCache.put(key, cache);
    }

    public Map<String, String> getKeyToCache() {
        return keyToCache;
    }

    public CacheStatus getCacheStatus() {
        return cacheStatus;
    }

    public void setCacheFull() {
        this.cacheStatus = CacheStatus.CACHE_FULL;
    }

    public String prettyPrint() {
        Iterator it = keyToCache.entrySet().iterator();
        String str = "ProxyReply: ";
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            str += " (" + pair.getKey() + ", " + pair.getValue() + ") ";
        }
        str += cacheStatus;
        return str;
    }
}
