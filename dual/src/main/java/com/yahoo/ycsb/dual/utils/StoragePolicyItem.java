package com.yahoo.ycsb.dual.utils;

/**
 * Created by Raluca on 24.03.16.
 */
public class StoragePolicyItem {
    private String key;
    private StoragePolicyType type;

    private int memcachedId;
    private int s3Id;

    public StoragePolicyItem(String key) {
        this.key = key;
    }

    /*public StoragePolicyItem(String key, StoragePolicyType type) {
        this.key = key;
        this.type = type;
    }*/

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public StoragePolicyType getType() {
        return type;
    }

    public void setType(StoragePolicyType type) {
        this.type = type;
    }

    public int getMemcachedId() {
        return memcachedId;
    }

    public void setMemcachedId(int memcachedId) {
        this.memcachedId = memcachedId;
    }

    public int getS3Id() {
        return s3Id;
    }

    public void setS3Id(int s3Id) {
        this.s3Id = s3Id;
    }
}
