package com.yahoo.ycsb.proxy;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/* Manages the cache address registry, i.e., <key, cache-address> pairs */
public class CacheAddressManager {
    public static Map<String, String> cacheAddressRegistry;

    public CacheAddressManager() {
        cacheAddressRegistry = new HashMap<String, String>();
    }

    public String getCacheAddress(String key) {
        return cacheAddressRegistry.get(key);
    }

    public void setCacheAddress(String key, String address) {
        cacheAddressRegistry.put(key, address);
    }

    public void updateRegistry(Map<String, String> keyToHost) {
        Iterator it = keyToHost.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> pair = (Map.Entry<String, String>) it.next();
            cacheAddressRegistry.put(pair.getKey(), pair.getValue());
        }
    }
}
