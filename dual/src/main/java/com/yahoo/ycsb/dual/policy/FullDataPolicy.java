package com.yahoo.ycsb.dual.policy;

import com.yahoo.ycsb.dual.Mode;

/**
 * Created by ubuntu on 02.02.16.
 */
public class FullDataPolicy extends StoragePolicy {

    public FullDataPolicy(int numRegions) {
        super(numRegions);
    }

    public int assignToRegion(String key, int id, Mode mode) {
        int regionNum = Math.abs(key.hashCode()) % numRegions;

        if (mode.equals(Mode.MEMCACHED))
            regionNum = (regionNum + 1) % numRegions;

        return regionNum;
    }
}
