package com.yahoo.ycsb.dual.clients;

import com.yahoo.ycsb.ClientBlueprint;
import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.communication.ProxyReply;
import com.yahoo.ycsb.common.liberasure.LonghairLib;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import com.yahoo.ycsb.dual.connections.ProxyConnection;
import com.yahoo.ycsb.dual.connections.S3Connection;
import com.yahoo.ycsb.dual.utils.ECBlock;
import com.yahoo.ycsb.dual.utils.Storage;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

/**
 * Created by Raluca on 23.04.16.
 */
public class SmartCacheClient extends ClientBlueprint {
    public static String S3_REGIONS_PROPERTIES = "s3.regions";
    public static String S3_ENDPOINTS_PROPERTIES = "s3.endpoints";
    public static String S3_BUCKETS_PROPERTIES = "s3.buckets";
    public static String MEMCACHED_SERVER_PROPERTY = "memcached.server";
    public static String LONGHAIR_K_PROPERTY = "longhair.k";
    public static String LONGHAIR_K_DEFAULT = "3";
    public static String LONGHAIR_M_PROPERTY = "longhair.m";
    public static String LONGHAIR_M_DEFAULT = "2";
    public static String EXECUTOR_THREADS_PROPERTY = "executor.threads";
    public static String EXECUTOR_THREADS_DEFAULT = "10";
    protected static Logger logger = Logger.getLogger(SmartCacheClient.class);
    private Properties properties;

    private List<S3Connection> s3Connections;
    private MemcachedConnection memConnection;
    private ProxyConnection proxyConnection;
    private ExecutorService executor;
    private int blocksPerRegion;

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

        String memHost = properties.getProperty(MEMCACHED_SERVER_PROPERTY);
        memConnection = new MemcachedConnection(memHost);
        logger.debug("Memcached connection " + memHost);
    }

    public void init() throws ClientException {
        logger.debug("SmartCacheClient.init() start");
        properties = getProperties();
        initS3();
        initLonghair();
        initCache();
        blocksPerRegion = (LonghairLib.k + LonghairLib.m) / s3Connections.size();
        final int threadsNum = Integer.valueOf(properties.getProperty(EXECUTOR_THREADS_PROPERTY, EXECUTOR_THREADS_DEFAULT));
        logger.debug("threads num: " + threadsNum);
        executor = Executors.newFixedThreadPool(threadsNum);
        logger.debug("SmartCacheClient.init() end");
    }


    @Override
    public void cleanup() throws ClientException {
        super.cleanup();
    }

    private ECBlock readS3Block(String key, int blockId, int s3ConnId) {
        String blockKey = key + blockId;
        S3Connection s3Connection = s3Connections.get(s3ConnId);
        byte[] bytes = s3Connection.read(blockKey);

        ECBlock ecblock = null;
        if (bytes != null)
            ecblock = new ECBlock(blockId, blockKey, bytes, Storage.BACKEND);
        else
            logger.error("[Error] ReadBlockBackend " + key + " block" + blockId + " from bucket" + s3ConnId);

        return ecblock;
    }


    private List<ECBlock> readS3(final String key, String region, int blocks) {
        // get s3 connection id
        int s3connId = Integer.MIN_VALUE;
        for (int i = 0; i < s3Connections.size(); i++) {
            String name = s3Connections.get(i).getRegion();
            if (name.equals(region)) {
                s3connId = i;
                break;
            }
        }
        if (s3connId == Integer.MIN_VALUE)
            logger.error("Invalid s3 connection id!");

        // request blocks in parallel
        final int s3ConnIdFin = s3connId;
        CompletionService<ECBlock> completionService = new ExecutorCompletionService<ECBlock>(executor);
        int s3ConnNum = s3Connections.size();
        int blocksPerRegion = (LonghairLib.k + LonghairLib.m) / s3ConnNum;
        for (int i = 0; i < s3ConnNum; i++) {
            final int blockId = i * blocksPerRegion + s3connId;
            completionService.submit(new Callable<ECBlock>() {
                @Override
                public ECBlock call() throws Exception {
                    return readS3Block(key, blockId, s3ConnIdFin);
                }
            });
        }

        int success = 0;
        int errors = 0;
        List<ECBlock> ecblocks = new ArrayList<ECBlock>();
        while (success < blocks) {
            try {
                Future<ECBlock> resultFuture = completionService.take();
                ECBlock ecblock = resultFuture.get();
                if (ecblock != null) {
                    ecblocks.add(ecblock);
                    success++;
                } else
                    errors++;
            } catch (Exception e) {
                errors++;
                logger.debug("Exception reading block.");
            }
            if (errors > 0)
                break;
        }

        return ecblocks;
    }

    @Override
    public byte[] read(final String key, final int keyNum) {
        ProxyReply reply = proxyConnection.sendRequest(key);
        logger.info(reply.prettyPrint());

        /*for (Map.Entry<String, Integer> entry : reply.getReadRecipe().entrySet()) {
            String location = entry.getKey();
            String blocks = entry.getKey();
            if (location.equals("cache")) {
                System.out.println("Read from memcached");
            } else {
                //System.out.println("Read from s3 client " + getS3ConnId(location));
            }
        }*/
        /*int blocks = reply.getBlocksToCache();
        byte[] data = null;
        if (blocks == 0) {
            // read from backend, nothing to cache
            //data = readFromS3(key);
        } else {
            if (blocks < LonghairLib.k) {
                // read from backend + cache
                // if cache miss => read from backend and then cache blocks in the background
            } else {
                // read from cache
                // if cache miss => read from backend and cache in the background
            }
        }*/

        // get data from backend
        /*byte[] data = readS3(key);

        // cache data
        Status insertst = memConnection.insert(key, data);
        logger.info("insert: " + insertst);

        // remove data
        Status delst = memConnection.delete(key);
        logger.info("delete: " + delst);*/

        /*try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        return null;
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
