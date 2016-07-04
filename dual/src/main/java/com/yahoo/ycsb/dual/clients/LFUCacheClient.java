package com.yahoo.ycsb.dual.clients;

import com.yahoo.ycsb.ClientBlueprint;
import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.communication.ProxyReply;
import com.yahoo.ycsb.common.liberasure.LonghairLib;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import com.yahoo.ycsb.common.properties.PropertyFactory;
import com.yahoo.ycsb.common.s3.S3Connection;
import com.yahoo.ycsb.dual.connections.ProxyConnection;
import com.yahoo.ycsb.dual.utils.ECBlock;
import com.yahoo.ycsb.dual.utils.Storage;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class LFUCacheClient extends ClientBlueprint {
    public static Logger logger = Logger.getLogger(LFUCacheClient.class);

    public static AtomicInteger cacheHits;
    public static AtomicInteger cachePartialHits;
    public static AtomicInteger cacheMisses;

    public static PropertyFactory propertyFactory;

    private List<S3Connection> s3Connections;
    private MemcachedConnection memConnection;
    private ProxyConnection proxyConnection;
    private List<String> s3Buckets;
    private ExecutorService executorRead; // one executorRead per client thread
    private ExecutorService executorCache;

    private void initS3() {
        List<String> regions = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_REGIONS_PROPERTY).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_ENDPOINTS_PROPERTY).split("\\s*,\\s*"));
        s3Buckets = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_BUCKETS_PROPERTY).split("\\s*,\\s*"));
        if (s3Buckets.size() != endpoints.size() || endpoints.size() != regions.size())
            logger.error("Configuration error: #buckets = #regions = #endpoints");

        s3Connections = new ArrayList<>();
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
        if (LonghairLib.Longhair.INSTANCE._cauchy_256_init(2) != 0) {
            logger.error("Error initializing longhair");
        }
    }

    private void initCache() throws ClientException {
        // connection to closest proxy
        proxyConnection = new ProxyConnection(getProperties());

        // connection to closest memcached server
        String memHost = propertyFactory.propertiesMap.get(propertyFactory.MEMCACHED_SERVER_PROPERTY);
        memConnection = new MemcachedConnection(memHost);
        logger.debug("Memcached connection " + memHost);
    }

    public void init() throws ClientException {
        logger.debug("SmartCacheClient.init() start");

        // init counters for stats
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

        final int threadsNum = Integer.valueOf(propertyFactory.propertiesMap.get(propertyFactory.EXECUTOR_THREADS_PROPERTY));
        logger.debug("threads num: " + threadsNum);
        executorRead = Executors.newFixedThreadPool(threadsNum);
        executorCache = Executors.newFixedThreadPool(threadsNum);
        logger.debug("SmartCacheClient.init() end");
    }


    @Override
    public void cleanup() throws ClientException {
        logger.error("Hits: " + cacheHits + " Misses: " + cacheMisses + " PartialHits: " + cachePartialHits);
        executorRead.shutdownNow();
        executorCache.shutdownNow();
    }

    @Override
    public void cleanupRead() {

    }

    /**
     * Store a block in cache
     *
     * @param ecblock block to be cached
     */
    private Boolean cacheBlock(ECBlock ecblock) {
        String key = ecblock.getBaseKey();
        Status status = memConnection.insert(ecblock.getBaseKey(), ecblock.getBytes());
        if (status == Status.OK) {
            logger.debug("Cache  " + key + " block " + ecblock.getId() + " at " + memConnection.getHost());
            return true;
        } else {
            logger.debug("[Error] Cache  " + key + " block " + ecblock.getId() + " at " + memConnection.getHost());
            return false;
        }
        //System.out.print(" cacheBlock:" + (endtime - starttime));
    }

    /**
     * Read block from cache; if it is not possible, fall back to the backend and cache the block in the background
     *
     * @param key     base key of data the block is part of
     * @param blockId id of block to read
     * @return block (bytes of encoded data + where it was read from: cache or backend)
     */
    private ECBlock readCacheBlock(final String key, int blockId) throws InterruptedException {
        String blockKey = key + blockId;
        byte[] bytes = memConnection.read(blockKey);

        ECBlock ecblock;
        // try to read block from cache
        if (bytes != null) {
            logger.debug("CacheHit " + key + " block " + blockId);
            ecblock = new ECBlock(key, blockId, bytes, Storage.CACHE);
        } else {
            // if not possible, read block from backend
            logger.debug("CacheMiss " + key + " block " + blockId);
            ecblock = readBackendBlock(key, blockId);
        }

        return ecblock;
    }

    /**
     * Read blocks from cache in parallel, according to cache recipe
     *
     * @param key          base key of data to be read
     * @param cachedBlocks num blocks that are supposed to be in the cache
     * @return list of blocks
     */
    private List<ECBlock> readCache(final String key, final int cachedBlocks) {
        List<ECBlock> ecblocks = new ArrayList<ECBlock>();

        List<Future> tasks = new ArrayList<Future>();
        CompletionService<ECBlock> completionService = new ExecutorCompletionService<ECBlock>(executorRead);
        final AtomicInteger cached = new AtomicInteger(0);
        // for each region in cache recipe
        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            // try to read each block from cache; if not possible, readCache falls back to backend
            final int blockIdFin = i;
            try {
                Future newTask = completionService.submit(new Callable<ECBlock>() {
                    @Override
                    public ECBlock call() throws Exception {
                        return readCacheBlock(key, blockIdFin);
                    }
                });
                tasks.add(newTask);
            } catch (RejectedExecutionException e) {
            }
        }

        // wait for an answer for each block (success / error)
        int success = 0;
        int errors = 0;
        while (success + errors < LonghairLib.k + LonghairLib.m) {
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
                logger.warn("Error reading block for " + key);
            }
        }

        // cancel unnecessary tasks
        for (Future f : tasks) {
            f.cancel(true);
        }

        return ecblocks;
    }

    /**
     * Read a block from the backend
     *
     * @param key     base key of data the block is part of
     * @param blockId id of the block within the data
     * @return the block
     */
    private ECBlock readBackendBlock(String key, int blockId) throws InterruptedException {
        long starttime = System.currentTimeMillis();
        String blockKey = key + blockId;
        //return new ECBlock(blockId, blockKey, new byte[699072], Storage.BACKEND);
        int s3ConnNum = blockId % s3Connections.size();
        S3Connection s3Connection = s3Connections.get(s3ConnNum);
        byte[] bytes = s3Connection.read(blockKey);

        ECBlock ecblock = null;
        if (bytes != null)
            ecblock = new ECBlock(key, blockId, bytes, Storage.BACKEND);
        else
            logger.error("[Error] ReadBlockBackend " + key + " block" + blockId + " from bucket " + s3Buckets.get(s3ConnNum));

        return ecblock;
    }

    /**
     * Read blocks from backend, according to background recipe from proxy
     *
     * @param key base key of the data blocks are part of
     * @return list of blocks
     */
    public List<ECBlock> readBackend(final String key) {
        long starttime = System.currentTimeMillis();
        List<ECBlock> ecblocks = new ArrayList<>();

        List<Future> tasks = new ArrayList<Future>();
        CompletionService<ECBlock> completionService = new ExecutorCompletionService<>(executorRead);

        // ask for each block
        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            final int blockIdFin = i;
            try {
                Future newTask = completionService.submit(new Callable<ECBlock>() {
                    @Override
                    public ECBlock call() throws Exception {
                        return readBackendBlock(key, blockIdFin);
                    }
                });
            } catch (RejectedExecutionException e) {

            }
        }

        // wait to receive block num blocks
        int success = 0;
        int errors = 0;
        while (success + errors < LonghairLib.k + LonghairLib.m && success < LonghairLib.k) {
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
                logger.warn("Error reading block for " + key);
            }
        }

        // cancel unnecessary tasks
        for (Future f : tasks) {
            f.cancel(true);
        }

        return ecblocks;
    }

    @Override
    public byte[] read(String key, int keyNum) {
        final ProxyReply reply = proxyConnection.requestRecipe(key);
        if (reply == null)
            return null;

        int cachedBlocks = reply.getCachedBlocks();
        logger.debug("blocksincache: " + cachedBlocks);

        List<ECBlock> ecblocks;
        if (cachedBlocks > 0) {
            // fetch from cache, but fall back to backend whenever necessary
            ecblocks = readCache(key, cachedBlocks);
        } else {
            ecblocks = readBackend(key);
        }

        // extract bytes from blocks + count how many blocks were read from cache and how many from backend
        Set<byte[]> blockBytes = new HashSet<>();
        int fromCache = 0;
        int fromBackend = 0;
        for (ECBlock ecblock : ecblocks) {
            blockBytes.add(ecblock.getBytes());
            if (ecblock.getStorage() == Storage.CACHE)
                fromCache++;
            else if (ecblock.getStorage() == Storage.BACKEND)
                fromBackend++;
        }


        if (fromCache < cachedBlocks) {
            List<Future> tasks = new ArrayList<Future>();
            CompletionService<Boolean> completionService = new ExecutorCompletionService<Boolean>(executorCache);

            int missing = cachedBlocks - fromCache;
            if (missing > 0) {
                for (ECBlock ecblock : ecblocks) {
                    if (ecblock.getStorage() == Storage.BACKEND) {
                        // cache block in the background
                        final ECBlock ecblockFin = ecblock;
                        try {
                            Future newTask = completionService.submit(new Callable<Boolean>() {
                                @Override
                                public Boolean call() throws Exception {
                                    Boolean toRet = cacheBlock(ecblockFin);
                                    System.out.println("Returning " + toRet);
                                    return toRet;
                                }
                            });
                            tasks.add(newTask);
                        } catch (RejectedExecutionException e) {
                            System.err.println("Exception thrown when caching blocks");
                        }
                        missing--;
                        System.out.println("missing: " + missing);
                    }
                    if (missing == 0)
                        break;
                }
            }

            int success = 0;
            int errors = 0;
            System.out.println("cached: " + success + " errors: " + errors + " fromCache: " + fromCache + " cachedBlocks: " + cachedBlocks);
            while (success + errors + fromCache < cachedBlocks) {
                System.out.println("cached: " + success + " errors: " + errors + " fromCache: " + fromCache + " cachedBlocks: " + cachedBlocks);
                try {
                    Future<Boolean> resultFuture = completionService.take();

                    System.out.println("Took one");
                    Boolean result = resultFuture.get();
                    if (result == true) {
                        success++;
                        System.out.println("cached = " + success);
                    } else {
                        errors++;
                        System.out.println("result was false for cacheBlock");
                    }
                } catch (Exception e) {
                    System.out.println("exception");
                }
            }

            for (Future f : tasks) {
                f.cancel(true);
            }
        }

        // stats: this was a hit / miss / partial hit
        if (fromCache > 0) {
            if (fromBackend == 0) {
                // all blocks read from cache
                cacheHits.incrementAndGet();
            } else {
                // subset of blocks read from cache
                cachePartialHits.incrementAndGet();
            }
        } else {
            // all blocks read from backend
            cacheMisses.incrementAndGet();
        }

        // decode data + return it
        byte[] data = null;
        System.out.println("blockBytes.size: " + blockBytes.size());
        if (blockBytes.size() >= LonghairLib.k)
            data = LonghairLib.decode(blockBytes);

        if (data != null)
            logger.info("Read " + key + " " + data.length + " bytes Cache: " + fromCache + " Backend: " + fromBackend);

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
