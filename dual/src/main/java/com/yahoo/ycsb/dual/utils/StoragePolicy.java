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

    public int assignFullDataToRegion(String key) {
        int regionNum = Math.abs(key.hashCode()) % numRegions;
        logger.trace(key + " mapped to " + regionNum);
        return regionNum;
    }

    public int assignEncodedBlockToRegion(String blockKey, int blockId) {
        int blocksPerRegion = (int) Math.round((double) (numBlocks / (double) numRegions));
        int regionNum = (int) (blockId / blocksPerRegion);
        logger.trace(blockKey + ":" + blockId + " mapped to " + regionNum);
        return regionNum;
    }
}
