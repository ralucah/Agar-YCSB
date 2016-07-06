package com.yahoo.ycsb.dual.policy;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Raluca on 31.03.16.
 */
public class StorageItem {
    /* key of data item */
    private String key;

    /* where it is stored in cache */
    private Set<StorageSubitem> cache;

    /* where it is stored in the backend */
    private Set<StorageSubitem> backend;

    /* where the client should fetch it from */
    private Set<StorageSubitem> strategy;

    public StorageItem(String key) {
        this.key = key;
        backend = new HashSet<StorageSubitem>();
        cache = new HashSet<StorageSubitem>();
        strategy = new HashSet<StorageSubitem>();
    }

    public String getKey() {
        return key;
    }

    public boolean isEncodedInBackend() {
        return backend.size() > 1;
    }

    public boolean isEncodedInCache() {
        return cache.size() > 1;
    }

    public void addToCache(String itemKey, String host) {
        StorageSubitem storageSubitem = new StorageSubitem(itemKey, host, StorageLayer.CACHE);
        cache.add(storageSubitem);
    }

    public void addToBackend(String itemKey, String bucket) {
        StorageSubitem storageSubitem = new StorageSubitem(itemKey, bucket, StorageLayer.BACKEND);
        backend.add(storageSubitem);
    }

    public void addToStrategy(StorageSubitem storageSubitem) {
        strategy.add(storageSubitem);
    }

    public void addToStrategy(Set<StorageSubitem> set) {
        strategy.addAll(set);
    }

    public void setStrategyToCache() {
        strategy = cache;
    }

    public void setStrategyToBackend() {
        strategy = backend;
    }

    public Set<StorageSubitem> getCacheSet() {
        return cache;
    }

    public Set<StorageSubitem> getBackendSet() {
        return backend;
    }

    public Set<StorageSubitem> getStrategy() {
        return strategy;
    }

    public int getCacheSetSize() {
        return cache.size();
    }

    // add num new items from backend to stratedy
    public void supplementStrategy() {
        strategy.addAll(backend);
    }

    public boolean isEncodedInStrategy() {
        return strategy.size() > 1;
    }

    public void removeFromStrategy(StorageSubitem storageSubitem) {
        strategy.remove(storageSubitem);
    }
}
