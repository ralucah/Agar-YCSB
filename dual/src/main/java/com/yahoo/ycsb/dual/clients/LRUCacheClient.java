package com.yahoo.ycsb.dual.clients;

/*
  IntelliJ
  Main: com.yahoo.ycsb.Client
  VM options: -Xmx3g
  Program arguments: -client com.yahoo.ycsb.dual.clients.LRUCacheClient -p fieldlength=1048576 -P workloads/myworkload
  Working directory: /home/ubuntu/work/repos/YCSB
  Use classpath of module: root
  JRE: 1.8
*/

/*
   Command line:
   cd YCSB
   mvn clean package
   bin/ycsb run lru -threads 1 -p fieldlength=1048576 -P workloads/myworkload
*/

import com.yahoo.ycsb.ClientBlueprint;
import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.liberasure.LonghairLib;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import com.yahoo.ycsb.common.properties.PropertyFactory;
import com.yahoo.ycsb.common.s3.S3Connection;
import com.yahoo.ycsb.dual.utils.ECBlock;
import com.yahoo.ycsb.dual.utils.Storage;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LRUCacheClient extends ClientBlueprint {
    public static AtomicInteger cacheHits;
    public static AtomicInteger cachePartialHits;
    public static AtomicInteger cacheMisses;
    public static PropertyFactory propertyFactory;

    private static Logger logger = Logger.getLogger(LRUCacheClient.class);
    private List<S3Connection> s3Connections;
    private MemcachedConnection memConnection;
    private int blocksincache;
    private ExecutorService executorRead, executorCache;

    private List<Future> cacheTasks;
    private CompletionService<Boolean> cacheCompletionService;

    // TODO Assumption: one bucket per region (num regions = num endpoints = num buckets)
    private void initS3() {
        List<String> regions = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_REGIONS_PROPERTY).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_ENDPOINTS_PROPERTY).split("\\s*,\\s*"));
        List<String> s3Buckets = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_BUCKETS_PROPERTY).split("\\s*,\\s*"));
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
        LonghairLib.k = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        LonghairLib.m = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_M_PROPERTY));
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
        String memHost = PropertyFactory.propertiesMap.get(PropertyFactory.MEMCACHED_SERVER_PROPERTY);
        memConnection = new MemcachedConnection(memHost);
        logger.debug("Memcached connection " + memHost);
        blocksincache = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.BLOCKS_IN_CACHE));
        logger.debug("blocksincache: " + blocksincache);
    }

    @Override
    public void init() throws ClientException {
        logger.debug("LRUCacheClient.init() start");

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
        final int threadsNum = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.EXECUTOR_THREADS_PROPERTY));
        logger.debug("threads num: " + threadsNum);
        executorRead = Executors.newFixedThreadPool(threadsNum);
        executorCache = Executors.newFixedThreadPool(threadsNum);
        cacheCompletionService = new ExecutorCompletionService<Boolean>(executorCache);
        cacheTasks = new ArrayList<Future>();

        logger.debug("LRUCacheClient.init() end");
    }

    @Override
    public void cleanup() throws ClientException {
        logger.error("Hits: " + cacheHits + " Misses: " + cacheMisses + " PartialHits: " + cachePartialHits);
        executorCache.shutdownNow();
        executorRead.shutdownNow();
    }

    @Override
    public void cleanupRead() {
        logger.debug("cleanup cache START");
        if (cacheTasks.size() > 0) {
            int success = 0;
            int errors = 0;
            while (success + errors < cacheTasks.size()) {
                //logger.debug("success: " + success + " errors: " + errors + " missingBlocks: " + missingBlocks);
                try {
                    Future<Boolean> resultFuture = cacheCompletionService.take();
                    Boolean result = resultFuture.get();
                    if (result == true) {
                        success++;
                    } else {
                        errors++;
                    }
                } catch (Exception e) {
                    logger.error("Exception while caching block.");
                }
            }
            for (Future f : cacheTasks) {
                f.cancel(true);
            }
        }
        logger.debug("cleanup cache END");
    }

    // read a block from backend
    private ECBlock readBlockBackend(String key, int blockId) throws InterruptedException {
        String blockKey = key + blockId;
        int s3ConnNum = blockId % s3Connections.size();
        S3Connection s3Connection = s3Connections.get(s3ConnNum);
        byte[] bytes = s3Connection.read(blockKey);

        ECBlock ecblock = null;
        if (bytes != null)
            ecblock = new ECBlock(key, blockId, bytes, Storage.BACKEND);
        else
            logger.error("[Error] ReadBlockBackend " + key + " block" + blockId + " from bucket" + s3ConnNum);

        return ecblock;
    }

    // try to read block from cache; on a miss, fall back to backend
    private ECBlock readBlockParallel(String key, int blockId) throws InterruptedException {
        String blockKey = key + blockId;
        byte[] bytes = memConnection.read(blockKey);

        ECBlock ecblock = null;
        if (bytes != null) {
            logger.debug("CacheHit " + key + " block " + blockId);
            ecblock = new ECBlock(key, blockId, bytes, Storage.CACHE);
        } else {
            logger.debug("CacheMiss " + key + " block " + blockId);
            ecblock = readBlockBackend(key, blockId);
        }

        return ecblock;
    }

    // send requests for k+m blocks in parallel, wait for the first k to arrive and cancel the other unnecessary requests
    private List<ECBlock> readParallel(final String key) {
        List<ECBlock> ecblocks = new ArrayList<ECBlock>();

        // read k+m blocks in parallel from cache and backend
        List<Future> tasks = new ArrayList<Future>();
        CompletionService<ECBlock> completionService = new ExecutorCompletionService<ECBlock>(executorRead);
        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            final int blockNumFin = i;
            Future newTask = completionService.submit(new Callable<ECBlock>() {
                @Override
                public ECBlock call() throws Exception {
                    return readBlockParallel(key, blockNumFin);
                }
            });
            tasks.add(newTask);
        }

        // wait for k blocks to be retrieved
        int success = 0;
        int errors = 0;
        Set<byte[]> blocks = new HashSet<byte[]>();
        while (success < LonghairLib.k && errors < LonghairLib.m) {
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
        }

        // cancel unnecessary requests
        for (Future f : tasks) {
            f.cancel(true);
        }

        return ecblocks;
    }

    private Boolean cacheBlock(ECBlock ecblock) {
        Status status = memConnection.insert(ecblock.getBaseKey(), ecblock.getBytes());
        if (status == Status.OK) {
            logger.debug("Cache  " + ecblock.getBaseKey() + " block " + ecblock.getId() + " at " + memConnection.getHost());
            return true;
        }
        logger.debug("[Error] Cache  " + ecblock.getBaseKey() + " block " + ecblock.getId() + " at " + memConnection.getHost());
        return false;
    }

    @Override
    public byte[] read(final String key, final int keyNum) {
        byte[] data = null;
        cacheTasks.clear();

        // read from cache and backend in parallel
        final List<ECBlock> ecblocks = readParallel(key);

        // extract bytes from read blocks and compute stats (how many blocks read from cache and how many from backend)
        Set<byte[]> blockBytes = new HashSet<byte[]>();
        int fromBackend = 0;
        int fromCache = 0;
        for (ECBlock ecblock : ecblocks) {
            blockBytes.add(ecblock.getBytes());
            if (ecblock.getStorage() == Storage.CACHE)
                fromCache++;
            else if (ecblock.getStorage() == Storage.BACKEND)
                fromBackend++;
        }

        // make sure there are "blocksincache" blocks in cache
        int missingBlocks = blocksincache - fromCache;
        if (fromCache < blocksincache) {
            if (missingBlocks > 0) {
                for (ECBlock ecblock : ecblocks) {
                    if (ecblock.getStorage() == Storage.BACKEND) {
                        // cache block in the background
                        final ECBlock ecblockFin = ecblock;
                        try {
                            Future newTask = cacheCompletionService.submit(new Callable<Boolean>() {
                                @Override
                                public Boolean call() throws Exception {
                                    return cacheBlock(ecblockFin);
                                }
                            });
                            cacheTasks.add(newTask);
                        } catch (RejectedExecutionException e) {
                            System.err.println("Exception thrown when caching blocks");
                        }
                        missingBlocks--;
                    }
                    if (missingBlocks == 0)
                        break;
                }
            }
        }

        // update stats: if data was entirely read from cache, backend or a mix
        if (fromCache >= LonghairLib.k)
            cacheHits.incrementAndGet();
        else if (fromCache > 0 && fromBackend > 0)
            cachePartialHits.incrementAndGet();
        else if (fromCache == 0 && fromBackend > 0)
            cacheMisses.incrementAndGet();

        // decode data
        if (blockBytes.size() >= LonghairLib.k) {
            data = LonghairLib.decode(blockBytes);
            logger.info("Read " + key + " " + data.length + " bytes Cache: " + fromCache + " Backend: " + fromBackend);
        }
        return data;
    }

    @Override
    public Status update(String key, byte[] value) {
        logger.warn("update not implemented");
        return null;
    }

    @Override
    public Status insert(String key, byte[] value) {
        logger.warn("insert not implemented");
        return null;
    }

    @Override
    public Status delete(String key) {
        logger.warn("delete not implemented");
        return null;
    }
}
