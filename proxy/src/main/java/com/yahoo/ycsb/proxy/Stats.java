package com.yahoo.ycsb.proxy;

import java.util.HashMap;
import java.util.Map;

public class Stats {
    private Map<String, String> keyToCache;
    private double popularity;
    private int accesses;

    public Stats(String key, String host) {
        keyToCache = new HashMap<String, String>();
        keyToCache.put(key, host);
        popularity = 0;
        accesses = 0;
    }

    public Map<String, String> getKeyToCache() {
        return keyToCache;
    }

    public void setPopularity(double popularity) {
        this.popularity = popularity;
    }

    public void incrementAccesses() {
        accesses++;
    }

    public int getAccesses() {
        return accesses;
    }

    public String prettyPrint() {
        String str = "popularity:" + popularity + " accesses:" + accesses + " [ ";
        for (Map.Entry<String, String> entry : keyToCache.entrySet()) {
            str += entry.getKey() + ":" + entry.getValue() + " ";
        }
        str += "]";
        return str;
    }
}
