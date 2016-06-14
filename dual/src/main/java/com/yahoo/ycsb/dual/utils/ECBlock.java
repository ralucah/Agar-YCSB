package com.yahoo.ycsb.dual.utils;

// An ecblock represents a block of erasure coded data
public class ECBlock {
    private String baseKey; // base key of the data the block is part of
    private int id; // id of block within the data
    private byte[] bytes; // encoded bytes
    private Storage storage; // where the block was read from: cache or backend; used for stats

    public ECBlock(String baseKey, int id, byte[] bytes, Storage storage) {
        this.baseKey = baseKey;
        this.id = id;
        this.bytes = bytes;
        this.storage = storage;
    }

    public int getId() {
        return id;
    }

    public String getBaseKey() {
        return baseKey + id;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public Storage getStorage() {
        return storage;
    }

}
