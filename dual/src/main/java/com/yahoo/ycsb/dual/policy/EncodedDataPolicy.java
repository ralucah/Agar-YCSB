package com.yahoo.ycsb.dual.policy;

import com.yahoo.ycsb.dual.DualClient;
import com.yahoo.ycsb.dual.Mode;

public class EncodedDataPolicy extends StoragePolicy {
    private int numBlocks;

    public EncodedDataPolicy(int numRegions, int numBlocks) {
        super(numRegions);
        this.numBlocks = numBlocks;
    }

    public int assignToRegion(String key, int id, Mode mode) {
        int regionNum = -1;

        for (int i = 1; i <= numRegions; i++) {
            int upperBound = i * (int) Math.round((double) (numBlocks / (double) numRegions));
            System.out.println(i + ": " + upperBound);
            if (id < upperBound) {
                regionNum = i;
                break;
            }
        }

        if (regionNum == -1)
            DualClient.logger.error("Invalid region for block " + id);

        if (mode.equals(Mode.MEMCACHED))
            regionNum = (regionNum + 1) % numRegions;

        return regionNum;
    }
}