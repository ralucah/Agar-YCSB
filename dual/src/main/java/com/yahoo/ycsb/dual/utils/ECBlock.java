package com.yahoo.ycsb.dual.utils;

/**
 * Created by Raluca on 14.04.16.
 */
public class ECBlock {
    private int id;
    private String key;
    private byte[] bytes;

    public ECBlock(int id, String key, byte[] bytes) {
        this.id = id;
        this.key = key;
        this.bytes = bytes;
    }

    public int getId() {
        return id;
    }

    public String getKey() {
        return key;
    }

    public byte[] getBytes() {
        return bytes;
    }

}
