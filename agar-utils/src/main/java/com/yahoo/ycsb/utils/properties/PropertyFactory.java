/**
 * Copyright 2016 [Agar]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.ycsb.utils.properties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class PropertyFactory {
    /* which cache manager to use */
    public static final String CACHE_MANAGER_PROPERTY = "cachemanager";
    /* ip:port e.g., 127.0.0.1:11000 */
    public static String PROXY_PROPERTY = "proxy";
    /* size of UDP packets exchanged with client e.g., 1024 */
    public static String PACKET_SIZE_PROPERTY = "packetsize";
    /* size of cache in MB e.g., 64 (mind the slab size, set using -I <size>) */
    public static String CACHE_SIZE_PROPERTY = "cachesize";
    /* how often to recompute the cache configuration, in ms e.g., 10000 */
    public static String PERIOD_PROPERTY = "period";
    /* weighted popularity coefficient, between 0 and 1; controls impact of old popularity value */
    public static String ALPHA_PROPERTY = "alpha";
    /* size of data record */
    public static String FIELD_LENGTH_PROPERTY = "fieldlength";
    /* ip:port of memcached server */
    //public static String MEMCACHED_SERVER_PROPERTY = "memcached.server";
    /* erasure-coding parameter; number of data chunks */
    public static String LONGHAIR_K_PROPERTY = "longhair.k";
    /* erasure-coding parameter; number of redundant chunks */
    public static String LONGHAIR_M_PROPERTY = "longhair.m";
    /* AWS S3 region names */
    public static String S3_REGIONS_PROPERTY = "s3.regions";
    /* AWS S3 region endpoints */
    public static String S3_ENDPOINTS_PROPERTY = "s3.endpoints";
    /* number of processing threads in the proxy's executor pool */
    public static String EXECUTOR_THREADS_PROPERTY = "executor.threads";
    public static String S3_BUCKETS_PROPERTY = "s3.buckets";
    public static String MEMCACHED_SERVER_PROPERTY = "memcached.server";
    /* eccache client */
    public static String BLOCKS_IN_CACHE = "blocksincache";

    /* number of send retries */
    public static String REQUEST_RETRIES = "retries";
    /* request timeout in ms */
    public static String REQUEST_TIMEOUT = "timeout";
    /* packet size in bytes */
    public static String REQUEST_PACKETSIZE = "packetsize";

    /* demo mode */
    public static boolean DEMO = false;

    /* all known properties */
    public static Map<String, String> propertiesMap;

    public PropertyFactory(Properties properties) {
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
            propertiesMap.put(S3_BUCKETS_PROPERTY, properties.getProperty(S3_BUCKETS_PROPERTY));
            propertiesMap.put(PERIOD_PROPERTY, properties.getProperty(PERIOD_PROPERTY));
            propertiesMap.put(ALPHA_PROPERTY, properties.getProperty(ALPHA_PROPERTY));
            propertiesMap.put(BLOCKS_IN_CACHE, properties.getProperty(BLOCKS_IN_CACHE));
            propertiesMap.put(CACHE_MANAGER_PROPERTY, properties.getProperty(CACHE_MANAGER_PROPERTY));
            propertiesMap.put(REQUEST_RETRIES, properties.getProperty(REQUEST_RETRIES));
            propertiesMap.put(REQUEST_TIMEOUT, properties.getProperty(REQUEST_TIMEOUT));
            propertiesMap.put(REQUEST_PACKETSIZE, properties.getProperty(REQUEST_PACKETSIZE));
            if (properties.getProperty("demo") != null)
                DEMO = true;
            else
                DEMO = false;
        }
    }
}
