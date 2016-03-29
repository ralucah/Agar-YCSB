package com.yahoo.ycsb.dual.utils;

import com.yahoo.ycsb.dual.MemClient;
import com.yahoo.ycsb.dual.S3Client;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class StoragePolicy {
    private static Logger logger = Logger.getLogger(StoragePolicy.class);

    /* connections to s3 */
    private List<S3Client> s3Conns;
    /* eager connections to memcached */
    private List<MemClient> memConns;

    /* total number of S3 connections */
    private int s3ConnsNum;

    /* if s3 stores data encoded or not */
    private boolean s3Encode;

    /* if cache stores data encoded or not -> inferred based on info from proxy */
    //private boolean memEncode;

    public StoragePolicy(List<S3Client> s3Conns, List<MemClient> memConns, boolean s3Encode) {
        this.s3Conns = s3Conns;
        this.memConns = memConns;
        this.s3Encode = s3Encode;
        s3ConnsNum = s3Conns.size();
    }

    public List<S3Client> assignToS3(String key) {
        List<S3Client> assignedS3Conns = new ArrayList<S3Client>();
        int connNum;

        /* full data */
        if (s3Encode == false) {
            connNum = Math.abs(key.hashCode()) % s3ConnsNum;
            assignedS3Conns.add(s3Conns.get(connNum));
        }
        /* for each encoded block */
        else {
            for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
                connNum = i % s3ConnsNum;
                assignedS3Conns.add(s3Conns.get(connNum));
            }
        }

        return assignedS3Conns;
    }

    // key, cachehost, s3host, iscached?
    public void assign(String key) {

    }

    /*
    public void assign(Map<String, CacheInfo> keyToMemcached, boolean s3Encode) {
        List<StoragePolicyItem> storagePolicyItems = new ArrayList<StoragePolicyItem>();

        /* maybe cache stores the full item */
        /*if (keyToMemcached.size() == 1) {
            Map.Entry<String, CacheInfo> entry = keyToMemcached.entrySet().iterator().next();
            String key = entry.getKey();
            CacheInfo cacheInfo = entry.getValue();
            StoragePolicyItem spitem = new StoragePolicyItem(key);
            /* full item should be in the cache! */
            /*if (cacheInfo.isCached() == true) {
                spitem.setType(StoragePolicyType.MEMCACHED);
            } else {
                spitem.setType(StoragePolicyType.AWSS3);
            }
            //spitem.setMemcachedId(); // map host to memcached connection
            // spitem.setS3Id(); // map to s3 connection
        }
        /* maybe cache stores blocks of encoded data */
        /*else {

        }
    }*/
}
