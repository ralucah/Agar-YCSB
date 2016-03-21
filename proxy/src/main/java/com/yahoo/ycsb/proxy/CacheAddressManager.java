package com.yahoo.ycsb.proxy;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Raluca on 10.03.16.
 */
public class CacheAddressManager {
    public static Map<String, String> blocksToCaches;
    private List<String> memcachedServers;
    private List<String> proxies; /* including self! */

    public CacheAddressManager(List<String> memcachedServers, List<String> proxies) {
        blocksToCaches = new HashMap<String, String>();
        this.memcachedServers = memcachedServers;
        this.proxies = proxies;
    }

    /* in the beginning, use some consistent hashing dummy function
    * and ignore everything else (workload patterns, server load) */
    public String assignToCacheServer(String key) {
        int serverNum = Math.abs(key.hashCode()) % memcachedServers.size();
        return memcachedServers.get(serverNum);
    }

    /* broadcast to other proxies info about data cached in this data center
    * i.e., try to keep blocksToCaches in sync */
    private void broadcast(String key, String address) {
        System.out.println("Broadcast to other servers about (" + key + ", " + address + ")");
    }

    public String getCacheServer(String key) {
        return blocksToCaches.get(key);
    }

    public String setCacheServer(String key) {
        String address = assignToCacheServer(key);
        blocksToCaches.put(key, address);

        return address;
    }

    public void update(Map<String, String> keyToHost) {
        Iterator it = keyToHost.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, String> pair = (Map.Entry<String, String>) it.next();
            blocksToCaches.put(pair.getKey(), pair.getValue());
        }
    }
}
