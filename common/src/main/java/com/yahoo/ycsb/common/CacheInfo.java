package com.yahoo.ycsb.common;

import java.io.Serializable;

/**
 * Created by Raluca on 14.03.16.
 */
//TODO serializable??? A: YUP!
public class CacheInfo implements Serializable {
    private String cacheServer;
    private boolean isCached;

    public CacheInfo(String cacheServer, boolean isCached) {
        this.cacheServer = cacheServer;
        this.isCached = isCached;
    }

    public String getCacheServer() {
        return cacheServer;
    }

    public void setCacheServer(String cacheServer) {
        this.cacheServer = cacheServer;
    }

    public boolean isCached() {
        return isCached;
    }

    public void setCached(boolean cached) {
        isCached = cached;
    }
}
