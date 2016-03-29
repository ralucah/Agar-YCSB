package com.yahoo.ycsb.common;

import java.io.Serializable;

/**
 * Created by Raluca on 14.03.16.
 */
public class CacheInfo implements Serializable {
    private String cacheAddress;
    private boolean isCached;

    public CacheInfo(String cacheServer, boolean isCached) {
        this.cacheAddress = cacheServer;
        this.isCached = isCached;
    }

    public String getCacheAddress() {
        return cacheAddress;
    }

    public void setCacheAddress(String cacheAddress) {
        this.cacheAddress = cacheAddress;
    }

    public boolean isCached() {
        return isCached;
    }

    public void setCached(boolean cached) {
        isCached = cached;
    }
}
