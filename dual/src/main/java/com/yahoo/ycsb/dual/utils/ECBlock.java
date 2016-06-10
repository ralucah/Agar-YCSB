package com.yahoo.ycsb.dual.utils;

/**
 * Created by Raluca on 14.04.16.
 */
public class ECBlock {
    private int id;
    private String key;
    private byte[] bytes;
    private Storage storage;

    public ECBlock(String key, int id, byte[] bytes, Storage storage) {
        this.key = key;
        this.id = id;
        this.bytes = bytes;
        this.storage = storage;
    }

    public int getId() {
        return id;
    }

    public String getKey() {
        return key + id;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public Storage getStorage() {
        return storage;
    }

}
