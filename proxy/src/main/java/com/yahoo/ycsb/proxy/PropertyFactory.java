package com.yahoo.ycsb.proxy;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyFactory {
    public static String EXECUTOR_THREADS_PROPERTY = "executor.threads";
    public static String PACKET_SIZE_PROPERTY = "packetsize";
    public static String CACHE_SIZE_PROPERTY = "cachesize";
    public static String FIELD_LENGTH_PROPERTY = "fieldlength";
    public static String MEMCACHED_SERVER_PROPERTY = "memcached.server";
    public static String PROXY_PROPERTY = "proxy";
    public static String LONGHAIR_K_PROPERTY = "longhair.k";
    public static String LONGHAIR_M_PROPERTY = "longhair.m";
    public static String S3_REGIONS_PROPERTY = "s3.regions";
    public static String S3_ENDPOINTS_PROPERTY = "s3.endpoints";

    public static Map<String, String> propertiesMap;

    protected PropertyFactory(Properties properties) {
        if (propertiesMap == null) {
            propertiesMap = new HashMap<String, String>();

            propertiesMap.put(EXECUTOR_THREADS_PROPERTY, properties.getProperty(EXECUTOR_THREADS_PROPERTY));
            propertiesMap.put(PACKET_SIZE_PROPERTY, properties.getProperty(PACKET_SIZE_PROPERTY));
            propertiesMap.put(CACHE_SIZE_PROPERTY, properties.getProperty(CACHE_SIZE_PROPERTY));
            propertiesMap.put(FIELD_LENGTH_PROPERTY, properties.getProperty(FIELD_LENGTH_PROPERTY));
            propertiesMap.put(MEMCACHED_SERVER_PROPERTY, properties.getProperty(MEMCACHED_SERVER_PROPERTY));
            propertiesMap.put(PROXY_PROPERTY, properties.getProperty(PROXY_PROPERTY));
            propertiesMap.put(LONGHAIR_K_PROPERTY, properties.getProperty(LONGHAIR_K_PROPERTY));
            propertiesMap.put(LONGHAIR_M_PROPERTY, properties.getProperty(LONGHAIR_M_PROPERTY));
            propertiesMap.put(S3_REGIONS_PROPERTY, properties.getProperty(S3_REGIONS_PROPERTY));
            propertiesMap.put(S3_ENDPOINTS_PROPERTY, properties.getProperty(S3_ENDPOINTS_PROPERTY));
        }
    }
}
