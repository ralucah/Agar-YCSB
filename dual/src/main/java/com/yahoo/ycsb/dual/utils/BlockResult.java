package com.yahoo.ycsb.dual.utils;

/**
 * Created by ubuntu on 08.02.16.
 */
public class BlockResult {
    private String key;
    private byte[] bytes;
    private int id;

    public BlockResult(String key) {
        this.key = key;
    }

    public BlockResult(String key, byte[] bytes) {
        this.key = key;
        this.bytes = bytes;
    }

    public BlockResult(String key, int id, byte[] bytes) {
        this.key = key;
        this.id = id;
        this.bytes = bytes;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public String getKey() {
        return key;
    }
}
