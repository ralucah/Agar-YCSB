package com.yahoo.ycsb.proxy;

/**
 * Created by Raluca on 26.03.16.
 */
public class ProxyConstants {
    public static String PROXY_PROPERTIES = "proxy.properties";
    public static String PROXIES = "proxy.hosts";
    public static String PROXY_ID = "proxy.id";
    public static String THREADS_NUM = "executor.threads";
    public static String PACKET_SIZE = "packet.size";
    public static String MEMCACHED_HOSTS = "memcached.hosts";
    public static String MEMCACHED_ENCODE = "memcached.encode";
    public static String MEMCACHED_NUM_BLOCKS = "memcached.num_blocks";

    public static int MEMCACHED_NUM_BLOCKS_DEFAULT = 5;
    public static int WHO_AM_I;
}
