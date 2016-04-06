package com.yahoo.ycsb.dual.policy;

import com.yahoo.ycsb.dual.utils.LonghairLib;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StorageOracle {
    private static Logger logger = Logger.getLogger(StorageOracle.class);

    /* list of S3 buckets to identify connections to the backend */
    List<String> s3Buckets;

    /* list of memcached hosts to identify connections to the cache servers */
    List<String> memcachedHosts;

    /* if s3 stores data encoded or not */
    private boolean s3Encode;

    public StorageOracle(List<String> s3Buckets, List<String> memcachedHosts, boolean s3Encode) {
        this.s3Buckets = s3Buckets;
        this.memcachedHosts = memcachedHosts;
        this.s3Encode = s3Encode;
    }

    private List<String> computeBlockKeys(String key) {
        List<String> keys = new ArrayList<String>();
        if (s3Encode == false) {
            keys.add(key);
        } else {
            for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++)
                keys.add(key + i);
        }
        return keys;
    }

    public boolean getS3Encode() {
        return s3Encode;
    }

    /* assign key to an s3 bucket */
    public String assignS3Bucket(String key) {
        int connNum = Math.abs(key.hashCode()) % s3Buckets.size();
        String bucket = s3Buckets.get(connNum);
        logger.debug(key + " assigned to " + bucket);
        return bucket;
    }

    public StorageItem compileStorageInfo(String itemKey, Map<String, String> keyToCacheHost) {
        StorageItem storageItem = new StorageItem(itemKey);

        /* compute cache set */
        Iterator iter = keyToCacheHost.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> pair = (Map.Entry<String, String>) iter.next();
            storageItem.addToCache(pair.getKey(), pair.getValue());
        }

        /* compute backend set */
        if (s3Encode == false) {
            storageItem.addToBackend(itemKey, assignS3Bucket(itemKey));
        } else {
            for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
                String blockKey = itemKey + i;
                storageItem.addToBackend(blockKey, assignS3Bucket(blockKey));
            }
        }

        /* set storage strategy to cache priority */
        storageItem.setStrategyToCache();

        return storageItem;
    }

    /*public void adjustStrategy(StorageItem storageItem, Set<String> availableKeys) {
        Set<StorageSubitem> strategy = storageItem.getStrategy();
        Set<StorageSubitem> backend = storageItem.getBackendSet();
        // remove what it is not available
        for (StorageSubitem ssitem : strategy) {
            if (availableKeys.contains(ssitem.getKey()) == false)
                storageItem.removeFromStrategy(ssitem);
        }
        // fill in with backend elements
        for (StorageSubitem ssitem : backend) {
            if (availableKeys.contains(ssitem.getKey()) == false)
                storageItem.addToStrategy(ssitem);
        }
    }*/

    /* */
    /*private List<ReadPolicyItem> processCacheInfo(Map<String, CacheInfo> keyToCacheInfo) {
        List<ReadPolicyItem>  rpItems = new ArrayList<ReadPolicyItem>();

        Iterator iter = keyToCacheInfo.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, CacheInfo> pair = (Map.Entry<String, CacheInfo>) iter.next();
            String key = pair.getKey();
            CacheInfo cinfo = pair.getValue();
            ReadPolicyItem rpItem;
            if (cinfo.isCached() == true) {
                rpItem = new ReadPolicyItem(key, cinfo.getCacheAddress(), StorageLayer.CACHE);
            } else {
                rpItem = new ReadPolicyItem(key, assignS3Bucket(key), StorageLayer.BACKEND);
            }
            rpItems.add(rpItem);
        }
        return rpItems;
    }*/

    /*private void readFromBackend(List<ReadPolicyItem>  rpItems) {

    }*/

    /**
     * Whenever possible, get data from the cache
     */
    /*private List<ReadPolicyItem> computeCachePriority(Map<String, CacheInfo> keyToCacheInfo) {
        // keys of data items that are likely in cache
        List<String> cached = new ArrayList<String>();
        // keys of data items that are likely not in cache
        List<String> notCached = new ArrayList<String>();

        // process cache info
        Iterator iter = keyToCacheInfo.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, CacheInfo> entry = (Map.Entry<String, CacheInfo>) iter.next();
            String key = entry.getKey();
            CacheInfo cacheInfo = entry.getValue();

            if (cacheInfo.isCached() == true)
                cached.add(key);
            else
                notCached.add(key);
        }

        return null;
    }*/

    /**
     * Whenever possible, read full data
     */
    /*private void fullDataPriority() {

    }*/

    /*
    * Get data from the nearest sites
    */
    /*private void proximityPriority() {

    }*/

    /**
     * Uses cache info + what it knows about the backend storage
     */
    /*public List<ReadPolicyItem> decideReadPolicy(Map<String, CacheInfo> keyToCacheInfo) {
        //return cachePriority(keyToCacheInfo);
        return null;
    }*/
}
