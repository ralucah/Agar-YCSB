package com.yahoo.ycsb.dual.utils;

import org.apache.log4j.Logger;

public class StoragePolicy {
    private static Logger logger = Logger.getLogger(StoragePolicy.class);
    private int numRegions;
    private int numBlocks;

    public StoragePolicy(int numRegions) {
        this.numRegions = numRegions;
    }

    public StoragePolicy(int numRegions, int numBlocks) {
        this.numRegions = numRegions;
        this.numBlocks = numBlocks;
    }

    public int assignFullDataToRegion(String key, Mode mode) {
        int regionNum = Math.abs(key.hashCode()) % numRegions;

        if (mode.equals(Mode.MEMCACHED))
            regionNum = (regionNum + 1) % numRegions;

        logger.trace(key + ":" + mode + " mapped to " + regionNum);
        return regionNum;
    }

    public int assignEncodedBlockToRegion(String blockKey, int blockId, Mode mode) {
        int blocksPerRegion = (int) Math.round((double) (numBlocks / (double) numRegions));
        int regionNum = (int) (blockId / blocksPerRegion);

        if (mode.equals(Mode.MEMCACHED))
            regionNum = (regionNum + 1) % numRegions;
        logger.trace(blockKey + ":" + blockId + ":" + mode + " mapped to " + regionNum);

        return regionNum;
    }
}
