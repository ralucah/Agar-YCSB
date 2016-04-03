package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Raluca on 26.03.16.
 */
public class CacheOracle {
    public static Logger logger = Logger.getLogger(CacheOracle.class);

    private List<String> memcachedHosts;
    private boolean memEncode;

    /* in case the data is encoded in the cache */
    private int numBlocks;

    public CacheOracle(List<String> memcachedHosts, boolean memEncode) {
        this.memcachedHosts = memcachedHosts;
        this.memEncode = memEncode;
        if (memEncode == true)
            numBlocks = ProxyConstants.MEMCACHED_NUM_BLOCKS_DEFAULT;
    }

    public void setNumBlocks(int numBlocks) {
        this.numBlocks = numBlocks;
    }

    public List<String> computeCacheKeys(String key) {
        List<String> keys = new ArrayList<String>();
        if (memEncode == false) {
            keys.add(key);
        } else {
            for (int i = 0; i < numBlocks; i++)
                keys.add(key + i);
        }
        return keys;
    }

    // TODO currently it overlaps the s3 storage policy
    public String assignCacheAddress(String key) {
        String address = null;
        int hostNum;
        int memHostsSize = memcachedHosts.size();

        hostNum = Math.abs(key.hashCode()) % memHostsSize;
        address = memcachedHosts.get(hostNum);

        return address;
    }
}
