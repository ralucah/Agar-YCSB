package com.yahoo.ycsb.dual.clients;

import com.yahoo.ycsb.ClientBlueprint;
import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.communication.CacheStatus;
import com.yahoo.ycsb.common.communication.ProxyReply;
import com.yahoo.ycsb.common.liberasure.LonghairLib;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import com.yahoo.ycsb.dual.connections.ProxyConnection;
import com.yahoo.ycsb.dual.connections.S3Connection;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

/**
 * Created by Raluca on 23.04.16.
 */
public class SmartCacheClient extends ClientBlueprint {
    public static String S3_REGIONS_PROPERTIES = "s3.regions";
    public static String S3_ENDPOINTS_PROPERTIES = "s3.endpoints";
    public static String S3_BUCKETS_PROPERTIES = "s3.buckets";
    public static String MEMCACHED_SERVERS_PROPERTY = "memcached.servers";
    public static String LONGHAIR_K_PROPERTY = "longhair.k";
    public static String LONGHAIR_K_DEFAULT = "3";
    public static String LONGHAIR_M_PROPERTY = "longhair.m";
    public static String LONGHAIR_M_DEFAULT = "2";
    public static String EXECUTOR_THREADS_PROPERTY = "executor.threads";
    public static String EXECUTOR_THREADS_DEFAULT = "5";
    protected static Logger logger = Logger.getLogger(SmartCacheClient.class);
    private Properties properties;

    private List<S3Connection> s3Connections;
    private Map<String, MemcachedConnection> memConnections; // (ip:port, connection) pairs
    private ProxyConnection proxyConnection;
    private ExecutorService executor;

    private void initS3() {
        List<String> regions = Arrays.asList(properties.getProperty(S3_REGIONS_PROPERTIES).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(properties.getProperty(S3_ENDPOINTS_PROPERTIES).split("\\s*,\\s*"));
        List<String> s3Buckets = Arrays.asList(properties.getProperty(S3_BUCKETS_PROPERTIES).split("\\s*,\\s*"));
        if (s3Buckets.size() != endpoints.size() || endpoints.size() != regions.size())
            logger.error("Configuration error: #buckets = #regions = #endpoints");
        s3Connections = new ArrayList<S3Connection>();
        for (int i = 0; i < s3Buckets.size(); i++) {
            String bucket = s3Buckets.get(i);
            String region = regions.get(i);
            String endpoint = endpoints.get(i);
            try {
                S3Connection client = new S3Connection(s3Buckets.get(i), regions.get(i), endpoints.get(i));
                s3Connections.add(client);
                logger.debug("S3 connection " + i + " " + bucket + " " + region + " " + endpoint);
            } catch (ClientException e) {
                logger.error("Error connecting to " + s3Buckets.get(i));
            }
        }
    }

    private void initLonghair() {
        LonghairLib.k = Integer.valueOf(properties.getProperty(LONGHAIR_K_PROPERTY, LONGHAIR_K_DEFAULT));
        LonghairLib.m = Integer.valueOf(properties.getProperty(LONGHAIR_M_PROPERTY, LONGHAIR_M_DEFAULT));
        logger.debug("k: " + LonghairLib.k + " m: " + LonghairLib.m);
        // check k >= 0 and k < 256
        if (LonghairLib.k < 0 || LonghairLib.k >= 256) {
            logger.error("Invalid Longhair.k: k should be >= 0 and < 256.");
        }
        // check m >=0 and m <= 256 - k
        if (LonghairLib.m < 0 || LonghairLib.m > 256 - LonghairLib.k) {
            logger.error("Invalid Longhair.m: m should be >= 0 and <= 256 - k.");
        }
        if (LonghairLib.Longhair.INSTANCE._cauchy_256_init(2) != 0) {
            logger.error("Error initializing longhair");
        }
    }

    private void initCache() throws ClientException {
        proxyConnection = new ProxyConnection(properties);

        // memcached connections are eagerly established
        memConnections = new HashMap<String, MemcachedConnection>();
        List<String> memHosts = Arrays.asList(properties.getProperty(MEMCACHED_SERVERS_PROPERTY).split("\\s*,\\s*"));
        for (String memHost : memHosts) {
            MemcachedConnection memConnection = new MemcachedConnection(memHost);
            memConnections.put(memHost, memConnection);
            logger.debug("Memcached connection " + memHost);
        }
    }

    public void init() throws ClientException {
        logger.debug("SmartCacheClient.init() start");
        properties = getProperties();
        initS3();
        initLonghair();
        initCache();
        // executor service
        final int threadsNum = Integer.valueOf(properties.getProperty(EXECUTOR_THREADS_PROPERTY, EXECUTOR_THREADS_DEFAULT));
        logger.debug("threads num: " + threadsNum);
        executor = Executors.newFixedThreadPool(threadsNum);
        logger.debug("SmartCacheClient.init() end");
    }

    // read encoded block blockNum of item with given key
    private byte[] readBlock(String key, int blockNum) {
        String blockKey = key + blockNum;
        S3Connection s3Connection = s3Connections.get(blockNum);
        byte[] block = s3Connection.read(blockKey);
        logger.debug("Read " + key + " block" + blockNum + " " + block.length + "B bucket" + blockNum);
        return block;
    }

    // read encoded blocks from the backend, in parallel
    private byte[] readBackend(final String key) {
        CompletionService<byte[]> completionService = new ExecutorCompletionService<byte[]>(executor);
        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            final int blockNumFin = i;
            completionService.submit(new Callable<byte[]>() {
                @Override
                public byte[] call() throws Exception {
                    return readBlock(key, blockNumFin);
                }
            });
        }

        int success = 0;
        int errors = 0;
        Set<byte[]> blocks = new HashSet<byte[]>();
        while (success < LonghairLib.k) {
            try {
                Future<byte[]> resultFuture = completionService.take();
                byte[] block = resultFuture.get();
                if (block != null) {
                    blocks.add(block);
                    success++;
                } else
                    errors++;
            } catch (Exception e) {
                errors++;
                logger.debug("Exception reading block.");
            }
            if (errors > LonghairLib.m)
                break;
        }

        byte[] data = null;
        if (success >= LonghairLib.k) {
            data = LonghairLib.decode(blocks);
        }

        return data;
    }

    @Override
    public byte[] read(final String key) {
        ProxyReply reply = proxyConnection.sendRequest(key);
        logger.info(reply.prettyPrint());
        final Map<String, String> keyToCache = reply.getKeyToCache();

        // read from cache (full data)
        byte[] data = null;
        for (Map.Entry<String, String> entry : keyToCache.entrySet()) {
            String entryKey = entry.getKey();
            String entryCache = entry.getValue();
            MemcachedConnection memConn = memConnections.get(entryCache);
            data = memConn.read(entryKey);
        }

        // if miss, get from backend and decode
        if (data == null) {
            data = readBackend(key);

            if (data != null)
                logger.info("Read BACKEND " + key + " " + data.length + "B");

            // cache, if necessary
            if (reply.getCacheStatus().equals(CacheStatus.CACHE_OK)) {
                final byte[] dataFin = data;
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        String memHost = keyToCache.get(key);
                        MemcachedConnection memConn = memConnections.get(memHost);
                        Status status = memConn.insert(key, dataFin);
                        logger.debug("Cache " + key + " " + memConn.getHost());
                    }
                });
            }
        } else
            logger.info("Read CACHE " + key + " " + data.length + "B");

        return data;
    }

    @Override
    public Status update(String key, byte[] value) {
        return null;
    }

    @Override
    public Status insert(String key, byte[] value) {
        return null;
    }

    @Override
    public Status delete(String key) {
        return null;
    }
}
