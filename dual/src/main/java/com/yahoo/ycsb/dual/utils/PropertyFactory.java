package com.yahoo.ycsb.dual.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyFactory {
    public static String S3_REGIONS_PROPERTY = "s3.regions";
    public static String S3_ENDPOINTS_PROPERTY = "s3.endpoints";
    public static String S3_BUCKETS_PROPERTY = "s3.buckets";

    public static String MEMCACHED_SERVER_PROPERTY = "memcached.server";

    public static String LONGHAIR_K_PROPERTY = "longhair.k";
    public static String LONGHAIR_M_PROPERTY = "longhair.m";

    public static String EXECUTOR_THREADS_PROPERTY = "executor.threads";

    /* eccache client */
    public static String BLOCKS_IN_CACHE = "blocksincache";

    /* all known properties */
    public static Map<String, String> propertiesMap;

    public PropertyFactory(Properties properties) {
        if (propertiesMap == null)
            propertiesMap = new HashMap<String, String>();

        propertiesMap.put(LONGHAIR_K_PROPERTY, properties.getProperty(LONGHAIR_K_PROPERTY));
        propertiesMap.put(LONGHAIR_M_PROPERTY, properties.getProperty(LONGHAIR_M_PROPERTY));

        propertiesMap.put(S3_REGIONS_PROPERTY, properties.getProperty(S3_REGIONS_PROPERTY));
        propertiesMap.put(S3_ENDPOINTS_PROPERTY, properties.getProperty(S3_ENDPOINTS_PROPERTY));
        propertiesMap.put(S3_BUCKETS_PROPERTY, properties.getProperty(S3_BUCKETS_PROPERTY));

        propertiesMap.put(MEMCACHED_SERVER_PROPERTY, properties.getProperty(MEMCACHED_SERVER_PROPERTY));

        propertiesMap.put(BLOCKS_IN_CACHE, properties.getProperty(BLOCKS_IN_CACHE));

        propertiesMap.put(EXECUTOR_THREADS_PROPERTY, properties.getProperty(EXECUTOR_THREADS_PROPERTY));
    }
}
