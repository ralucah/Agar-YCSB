package com.yahoo.ycsb.dual.policy;

import com.yahoo.ycsb.dual.Mode;

public abstract class StoragePolicy {
    protected int numRegions;

    protected StoragePolicy(int numRegions) {
        this.numRegions = numRegions;
    }

    public abstract int assignToRegion(String key, int id, Mode mode);
}
