package com.yahoo.ycsb.dual.policy;

import com.yahoo.ycsb.dual.Mode;
import org.apache.log4j.Logger;

public abstract class StoragePolicy {
    protected static Logger logger = Logger.getLogger(Class.class);
    protected int numRegions;

    protected StoragePolicy(int numRegions) {
        this.numRegions = numRegions;
    }

    public abstract int assignToRegion(String key, int id, Mode mode);
}
