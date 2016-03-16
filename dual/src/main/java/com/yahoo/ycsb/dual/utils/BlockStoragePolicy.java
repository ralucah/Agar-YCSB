package com.yahoo.ycsb.dual.utils;

import org.apache.log4j.Logger;

public class BlockStoragePolicy {
    private static Logger logger = Logger.getLogger(BlockStoragePolicy.class);
    private int numS3Clients;
    private int numBlocks;

    public BlockStoragePolicy(int numS3Clients, int numBlocks) {
        this.numS3Clients = numS3Clients;
        this.numBlocks = numBlocks;
    }

    /*public int assignFullDataToRegion(String key) {
        int regionNum = Math.abs(key.hashCode()) % numS3Clients;
        logger.trace(key + " mapped to " + regionNum);
        return regionNum;
    }*/

    /**
     * @param key
     * @param id
     * @return position of S3 client in the S3 clients list
     */
    public int assignBlockToS3Client(String key, int id) {
        int blocksPerRegion = (int) Math.round((double) (numBlocks / (double) numS3Clients));
        int regionNum = (int) (id / blocksPerRegion);
        logger.trace(key + ":" + id + " mapped to " + regionNum);
        return regionNum;
    }
}
