package com.yahoo.ycsb.dual.clients;

import com.yahoo.ycsb.ClientBlueprint;
import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.liberasure.LonghairLib;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import com.yahoo.ycsb.common.properties.PropertyFactory;
import com.yahoo.ycsb.common.s3.S3Connection;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/*
  IntelliJ
  Main: com.yahoo.ycsb.Client
  VM options: -Xmx3g
  Program arguments: -client com.yahoo.ycsb.dual.clients.LocalCacheClient -p fieldlength=4194304 -P workloads/myworkload
  Working directory: /home/ubuntu/work/repos/YCSB
  Use classpath of module: root
  JRE: 1.8
*/

/*
   Command line:
   cd YCSB
   mvn clean package
   bin/ycsb run localcache -threads 1 -p fieldlength=1048576 -P workloads/myworkload
*/

public class LocalCacheClient extends ClientBlueprint {
    public static AtomicInteger cacheHits;
    public static AtomicInteger cacheMisses;

    public static PropertyFactory propertyFactory;
    protected static Logger logger = Logger.getLogger(LocalCacheClient.class);

    // S3 bucket names mapped to connections to AWS S3 buckets
    private List<S3Connection> s3Connections;
    private MemcachedConnection memConnection;
    private ExecutorService executorRead, executorCache;

    private Future cacheTask;

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
    }

    @Override
    public void init() throws ClientException {
        logger.debug("LocalCacheClient.init() BEGIN");
        if (cacheHits == null)
            cacheHits = new AtomicInteger(0);
        if (cacheMisses == null)
            cacheMisses = new AtomicInteger(0);

        propertyFactory = new PropertyFactory(getProperties());

        initS3();
        initLonghair();
        initCache();

        // init executor service
        final int threadsNum = Integer.valueOf(propertyFactory.propertiesMap.get(PropertyFactory.EXECUTOR_THREADS_PROPERTY));
        logger.debug("threads num: " + threadsNum);
        executorRead = Executors.newFixedThreadPool(threadsNum);
        executorCache = Executors.newFixedThreadPool(threadsNum);
        logger.debug("LocalCacheClient.init() END");
    }

    @Override
    public void cleanup() throws ClientException {
        logger.error(memConnection.getHost() + " Hits: " + cacheHits + " Misses: " + cacheMisses);
        executorRead.shutdownNow();
        executorCache.shutdownNow();
    }

    @Override
    public void cleanupRead() {
        System.out.println("cleanup cache!");
        if (cacheTask != null) {
            while (cacheTask.isDone() == false) ;
        }
    }

    private byte[] readBlock(String baseKey, int blockNum) throws InterruptedException {
        String blockKey = baseKey + blockNum;
        int s3ConnNum = blockNum % s3Connections.size();
        S3Connection s3Connection = s3Connections.get(s3ConnNum);
        byte[] block = s3Connection.read(blockKey);
        if (block != null)
            logger.debug("Read " + baseKey + " block" + blockNum + " bucket" + blockNum);
        //logger.debug("ReadBlock " + blockNum + " " + blockKey + " " + ClientUtils.bytesToHash(block));
        return block;
    }

    private byte[] readFromBackend(final String key) {
        List<Future> tasks = new ArrayList<Future>();
        // read blocks in parallel
        CompletionService<byte[]> completionService = new ExecutorCompletionService<byte[]>(executorRead);
        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            final int blockNumFin = i;
            Future newTask = completionService.submit(new Callable<byte[]>() {
                @Override
                public byte[] call() throws Exception {
                    return readBlock(key, blockNumFin);
                }
            });
            tasks.add(newTask);
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

        for (Future f : tasks) {
            f.cancel(true);
        }

        byte[] data = null;
        if (success >= LonghairLib.k) {
            data = LonghairLib.decode(blocks);
        }
        return data;
    }

    private byte[] readFromCache(String key) {
        byte[] data = memConnection.read(key);
        return data;
    }

    private void cacheData(String key, byte[] data) {
        Status status = memConnection.insert(key, data);
        logger.debug("Cache " + key + " " + memConnection.getHost());
    }

    @Override
    public byte[] read(final String key, final int keyNum) {
        cacheTask = null;
        byte[] data = readFromCache(key);
        if (data == null) {
            data = readFromBackend(key);
            if (data != null) {
                logger.info("Read BACKEND " + key + " " + data.length + "B"); // + ClientUtils.bytesToHash(data));
                cacheMisses.incrementAndGet();

                final byte[] dataFin = data;
                cacheTask = executorCache.submit(new Runnable() {
                    @Override
                    public void run() {
                        cacheData(key, dataFin);
                    }
                });
            }
        } else {
            logger.info("Read CACHE " + key + " " + data.length + "B"); // + ClientUtils.bytesToHash(data) + " " + memConnection.getHost());
            cacheHits.incrementAndGet();
        }

        if (data == null)
            logger.error("Error reading " + key);

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
