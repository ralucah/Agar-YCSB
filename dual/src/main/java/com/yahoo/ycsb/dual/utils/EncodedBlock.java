package com.yahoo.ycsb.dual.utils;

/**
 * Created by ubuntu on 08.02.16.
 */
public class EncodedBlock {
    private String key;
    private byte[] bytes;
    private int id; // it's the last character of the key string

    public EncodedBlock(String key) {
        this.key = key;
        id = Integer.parseInt(key.substring(key.length() - 1));
    }

    public String getKey() {
        return key;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public int getId() {
        return id;
    }
}
