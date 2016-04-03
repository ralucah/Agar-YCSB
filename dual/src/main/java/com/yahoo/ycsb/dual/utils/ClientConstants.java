package com.yahoo.ycsb.dual.utils;

/**
 * Created by Raluca on 26.03.16.
 */
public final class ClientConstants {
    /* property names */
    public static String PROPERTIES_FILE = "client.properties";
    public static String PROXY_HOST = "proxy";
    public static String S3_REGIONS = "s3.regions";
    public static String S3_ENDPOINTS = "s3.endpoints";
    public static String S3_BUCKETS = "s3.buckets";
    public static String S3_ENCODE = "s3.encode";
    public static String MEMCACHED_HOSTS = "memcached.hosts";
    public static String LONGHAIR_K = "longhair.k";
    public static String LONGHAIR_M = "longhair.m";
    public static String THREADS_NUM = "executor.threads";
    public static String PACKET_SIZE = "socket.packet_size";
    public static String SOCKET_TIMEOUT = "socket.timeout"; // in ms
    public static String SOCKET_RETRIES = "socket.retries";

    /* default values for properties */
    public static String S3_ENCODE_DEFAULT = "false";
    public static String LONGHAIR_K_DEFAULT = "3";
    public static String LONGHAIR_M_DEFAULT = "2";
    public static String THREADS_NUM_DEFAULT = "5";
    public static String PACKET_SIZE_DEFAULT = "1024";
    public static String SOCKET_TIMEOUT_DEFAULT = "1000";
    public static String SOCKET_RETRIES_DEFAULT = "3";
}
