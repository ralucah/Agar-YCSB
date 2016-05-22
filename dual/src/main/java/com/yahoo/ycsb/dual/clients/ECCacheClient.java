package com.yahoo.ycsb.dual.clients;

/*
  IntelliJ
  Main: com.yahoo.ycsb.Client
  VM options: -Xmx3g
  Program arguments: -client com.yahoo.ycsb.dual.clients.ECCacheClient -p fieldlength=4194304 -P workloads/myworkload
  Working directory: /home/ubuntu/work/repos/YCSB
  Use classpath of module: root
  JRE: 1.8
*/

/*
   Command line:
   cd YCSB
   mvn clean package
   bin/ycsb run eccache -threads 2 -p fieldlength=4194304 -P workloads/myworkload
*/

import com.yahoo.ycsb.ClientBlueprint;
import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.liberasure.LonghairLib;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import com.yahoo.ycsb.dual.connections.S3Connection;
import com.yahoo.ycsb.dual.utils.ECBlock;
import com.yahoo.ycsb.dual.utils.PropertyFactory;
import com.yahoo.ycsb.dual.utils.Storage;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ECCacheClient extends ClientBlueprint {
    public static AtomicInteger cacheHits;
    public static AtomicInteger cachePartialHits;
    public static AtomicInteger cacheMisses;
    public static PropertyFactory propertyFactory;

    private static Logger logger = Logger.getLogger(ECCacheClient.class);
    private List<S3Connection> s3Connections;
    private MemcachedConnection memConnection;
    private int blocksincache;
    private ExecutorService executor;

    // TODO Assumption: one bucket per region (num regions = num endpoints = num buckets)
    private void initS3() {
        List<String> regions = Arrays.asList(propertyFactory.propertiesMap.get(PropertyFactory.S3_REGIONS_PROPERTY).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(propertyFactory.propertiesMap.get(PropertyFactory.S3_ENDPOINTS_PROPERTY).split("\\s*,\\s*"));
        List<String> s3Buckets = Arrays.asList(propertyFactory.propertiesMap.get(PropertyFactory.S3_BUCKETS_PROPERTY).split("\\s*,\\s*"));
        if (s3Buckets.size() != endpoints.size() || endpoints.size() != regions.size())
            logger.error("Configuration error: #buckets = #regions = #endpoints");

        // establish S3 connections
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
        // erasure coding-related configuration
        LonghairLib.k = Integer.valueOf(propertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        LonghairLib.m = Integer.valueOf(propertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_M_PROPERTY));
        logger.debug("k: " + LonghairLib.k + " m: " + LonghairLib.m);

        // check k >= 0 and k < 256
        if (LonghairLib.k < 0 || LonghairLib.k >= 256) {
            logger.error("Invalid Longhair.k: k should be >= 0 and < 256.");
        }
        // check m >=0 and m <= 256 - k
        if (LonghairLib.m < 0 || LonghairLib.m > 256 - LonghairLib.k) {
            logger.error("Invalid Longhair.m: m should be >= 0 and <= 256 - k.");
        }

        // init longhair
        if (LonghairLib.Longhair.INSTANCE._cauchy_256_init(2) != 0) {
            logger.error("Error initializing longhair");
        }
    }

    private void initCache() throws ClientException {
        String memHost = propertyFactory.propertiesMap.get(PropertyFactory.MEMCACHED_SERVER_PROPERTY);
        memConnection = new MemcachedConnection(memHost);
        logger.debug("Memcached connection " + memHost);
        blocksincache = Integer.valueOf(propertyFactory.propertiesMap.get(PropertyFactory.BLOCKS_IN_CACHE));
        logger.debug("blocksincache: " + blocksincache);
    }

    @Override
    public void init() throws ClientException {
        logger.debug("DualClient.init() start");

        if (cacheHits == null)
            cacheHits = new AtomicInteger(0);
        if (cacheMisses == null)
            cacheMisses = new AtomicInteger(0);
        if (cachePartialHits == null)
            cachePartialHits = new AtomicInteger(0);

        propertyFactory = new PropertyFactory(getProperties());

        initS3();
        initLonghair();
        initCache();

        // init executor service
        if (executor == null) {
            final int threadsNum = Integer.valueOf(propertyFactory.propertiesMap.get(PropertyFactory.EXECUTOR_THREADS_PROPERTY));
            logger.debug("threads num: " + threadsNum);
            executor = Executors.newFixedThreadPool(threadsNum);
        }

        logger.debug("DualClient.init() end");
    }

    @Override
    public void cleanup() throws ClientException {
        logger.error("Hits: " + cacheHits + " Misses: " + cacheMisses + " PartialHits: " + cachePartialHits);
        if (executor.isTerminated())
            executor.shutdown();
    }

    private ECBlock readBlockParallel(String key, int blockId) {
        String blockKey = key + blockId;
        byte[] bytes = memConnection.read(blockKey);

        ECBlock ecblock = null;
        if (bytes != null) {
            logger.debug("CacheHit " + key + " block " + blockId);
            ecblock = new ECBlock(blockId, blockKey, bytes, Storage.CACHE);
        } else {
            logger.debug("CacheMiss " + key + " block " + blockId);
            ecblock = readBlockBackend(key, blockId);
        }

        return ecblock;
    }

    private ECBlock readBlockBackend(String key, int blockId) {
        String blockKey = key + blockId;
        int s3ConnNum = blockId % s3Connections.size();
        S3Connection s3Connection = s3Connections.get(s3ConnNum);
        byte[] bytes = s3Connection.read(blockKey);

        ECBlock ecblock = null;
        if (bytes != null)
            ecblock = new ECBlock(blockId, blockKey, bytes, Storage.BACKEND);
        else
            logger.error("[Error] ReadBlockBackend " + key + " block" + blockId + " from bucket" + s3ConnNum);

        return ecblock;
    }

    private List<ECBlock> readParallel(final String key) {
        List<ECBlock> ecblocks = new ArrayList<ECBlock>();

        // read blocks in parallel from cache and backend
        CompletionService<ECBlock> completionService = new ExecutorCompletionService<ECBlock>(executor);
        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            final int blockNumFin = i;
            completionService.submit(new Callable<ECBlock>() {
                @Override
                public ECBlock call() throws Exception {
                    return readBlockParallel(key, blockNumFin);
                }
            });
        }

        int success = 0;
        int errors = 0;
        Set<byte[]> blocks = new HashSet<byte[]>();
        while (success < LonghairLib.k) {
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
            if (errors > LonghairLib.m)
                break;
        }

        return ecblocks;
    }

    @Override
    public byte[] read(final String key, final int keyNum) {
        byte[] data = null;
        // read from cache
        final List<ECBlock> ecblocks = readParallel(key);
        Set<byte[]> blockBytes = new HashSet<byte[]>();

        int fromCache = 0;
        int fromBackend = 0;
        for (ECBlock ecblock : ecblocks) {
            blockBytes.add(ecblock.getBytes());
            if (ecblock.getStorage() == Storage.CACHE)
                fromCache++;
            else if (ecblock.getStorage() == Storage.BACKEND)
                fromBackend++;
        }

        // cache in background
        if (fromCache == 0 && fromBackend > 0) {
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    int counter = 0;
                    for (int i = ecblocks.size() - 1; i >= 0; i--) {
                        ECBlock ecblock = ecblocks.get(i);
                        cacheBlock(key, ecblock);
                        counter++;
                        if (counter == blocksincache)
                            break;
                    }
                }
            });
        }

        // stats
        if (fromCache == blocksincache)
            cacheHits.incrementAndGet();
        else if (fromCache > 0 && fromBackend > 0)
            cachePartialHits.incrementAndGet();
        else if (fromCache == 0 && fromBackend > 0)
            cacheMisses.incrementAndGet();

        data = LonghairLib.decode(blockBytes);
        logger.info("Read " + key + " " + data.length + " bytes Cache: " + fromCache + " Backend: " + fromBackend);

        return data;
    }

    private void cacheBlock(String key, ECBlock ecblock) {
        Status status = memConnection.insert(ecblock.getKey(), ecblock.getBytes());
        if (status == Status.OK)
            logger.debug("Cache  " + key + " block " + ecblock.getId() + " at " + memConnection.getHost());
        else
            logger.debug("[Error] Cache  " + key + " block " + ecblock.getId() + " at " + memConnection.getHost());
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
