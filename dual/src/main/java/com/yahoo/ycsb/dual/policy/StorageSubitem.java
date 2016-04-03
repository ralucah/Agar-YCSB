package com.yahoo.ycsb.dual.policy;

/**
 * Created by Raluca on 30.03.16.
 */
public class StorageSubitem {
    /* key of the data item (might be full data or encoded block) */
    private String key;

    /* which connection to cache / backend to use? */
    private String host;

    /* cache or backend */
    private StorageLayer layer;

    public StorageSubitem(String key, String host, StorageLayer layer) {
        this.key = key;
        this.host = host;
        this.layer = layer;
    }

    public String getHost() {
        return host;
    }

    public String getKey() {
        return key;
    }

    public StorageLayer getLayer() {
        return layer;
    }

    @Override
    public boolean equals(Object other) {
        StorageSubitem otherSubitem = (StorageSubitem) other;
        return key.equals(otherSubitem.getKey());
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }
}
