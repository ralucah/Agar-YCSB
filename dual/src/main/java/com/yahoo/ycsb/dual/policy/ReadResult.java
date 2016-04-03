package com.yahoo.ycsb.dual.policy;

/**
 * Created by ubuntu on 08.02.16.
 */
public class ReadResult {
    private String key;
    private byte[] bytes;

    public ReadResult(String key) {
        this.key = key;
        //id = Integer.parseInt(key.substring(key.length() - 1));
    }

    public ReadResult(String key, byte[] bytes) {
        this.key = key;
        this.bytes = bytes;
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
}
