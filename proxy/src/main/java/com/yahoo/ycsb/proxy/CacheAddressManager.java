package com.yahoo.ycsb.proxy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Raluca on 10.03.16.
 */
public class CacheAddressManager {
    private Map<String, String> blocksToCaches;
    private List<String> memcachedServers;
    private List<String> otherProxies;

    public CacheAddressManager(List<String> memcachedServers, List<String> otherProxies) {
        blocksToCaches = new HashMap<String, String>();
        this.memcachedServers = memcachedServers;
        this.otherProxies = otherProxies;
    }

    /* in the beginning, use some consistent hashing dummy function
    * and ignore everything else (workload patterns, server load) */
    private String assignBlockToCache(String blockKey) {
        int serverNum = Math.abs(blockKey.hashCode()) % memcachedServers.size();
        return memcachedServers.get(serverNum);
    }

    /* broadcast to other proxies info about data cached in this data center
    * i.e., try to keep blocksToCaches in sync */
    private void broadcast(String blockKey, String cache) {
        System.out.println("Broadcast to other servers!");
    }

    public String getCacheServer(String blockKey) {
        return blocksToCaches.get(blockKey);
    }

    public void setCacheServer(String blockKey) {
        String address = assignBlockToCache(blockKey);
        blocksToCaches.put(blockKey, address);
    }
}
