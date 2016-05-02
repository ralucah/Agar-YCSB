package com.yahoo.ycsb.dual.clients;

// -db com.yahoo.ycsb.dual.clients.ECCacheClient2 -p fieldlength=100 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.clients.ECCacheClient1 -p fieldlength=100 -s -P workloads/myworkload

import com.yahoo.ycsb.ClientBlueprint;
import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.liberasure.LonghairLib;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import com.yahoo.ycsb.dual.connections.S3Connection;
import com.yahoo.ycsb.dual.utils.ECBlock;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class ECCacheClient extends ClientBlueprint {
    public static String S3_REGIONS_PROPERTIES = "s3.regions";
    public static String S3_ENDPOINTS_PROPERTIES = "s3.endpoints";
    public static String S3_BUCKETS_PROPERTIES = "s3.buckets";
    public static String MEMCACHED_PROPERTY = "memcached.server";
    public static String LONGHAIR_K_PROPERTY = "longhair.k";
    public static String LONGHAIR_K_DEFAULT = "6";
    public static String LONGHAIR_M_PROPERTY = "longhair.m";
    public static String LONGHAIR_M_DEFAULT = "3";
    public static String EXECUTOR_THREADS_PROPERTY = "executor.threads";
    public static String EXECUTOR_THREADS_DEFAULT = "10";
    public static String BLOCKS_TO_CACHE = "blocksincache";
    protected static Logger logger = Logger.getLogger(ECCacheClient.class);
    protected static AtomicInteger cacheHits = new AtomicInteger(0);
    protected static AtomicInteger cachePartialHits = new AtomicInteger(0);
    protected static AtomicInteger cacheMisses = new AtomicInteger(0);
    private Properties properties;
    // S3 bucket names mapped to connections to AWS S3 buckets
    private List<S3Connection> s3Connections;
    // for concurrent processing
    private ExecutorService executor;
    private MemcachedConnection memConnection;
    private int blocksincache;

    // TODO Assumption: one bucket per region (num regions = num endpoints = num buckets)
    private void initS3() {
        List<String> regions = Arrays.asList(properties.getProperty(S3_REGIONS_PROPERTIES).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(properties.getProperty(S3_ENDPOINTS_PROPERTIES).split("\\s*,\\s*"));
        List<String> s3Buckets = Arrays.asList(properties.getProperty(S3_BUCKETS_PROPERTIES).split("\\s*,\\s*"));
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

        // init longhair
        if (LonghairLib.Longhair.INSTANCE._cauchy_256_init(2) != 0) {
            logger.error("Error initializing longhair");
        }
    }

    private void initCache() throws ClientException {
        String memHost = properties.getProperty(MEMCACHED_PROPERTY);
        memConnection = new MemcachedConnection(memHost);
        logger.debug("Memcached connection " + memHost);
        blocksincache = Integer.valueOf(properties.getProperty(BLOCKS_TO_CACHE, new Integer(LonghairLib.k).toString()));
        logger.debug("blocksincache: " + blocksincache);
    }

    @Override
    public void init() throws ClientException {
        logger.debug("DualClient.init() start");
        properties = getProperties();

        initS3();
        initLonghair();
        initCache();

        // init executor service
        final int threadsNum = Integer.valueOf(properties.getProperty(EXECUTOR_THREADS_PROPERTY, EXECUTOR_THREADS_DEFAULT));
        logger.debug("threads num: " + threadsNum);
        executor = Executors.newFixedThreadPool(threadsNum);

        logger.debug("DualClient.init() end");
    }

    @Override
    public void cleanup() throws ClientException {
        executor.shutdown();
        logger.error("Hits: " + cacheHits + " Misses: " + cacheMisses + " PartialHits: " + cachePartialHits);
    }

    private ECBlock readBlockCache(String key, int blockId) {
        String blockKey = key + blockId;
        byte[] bytes = memConnection.read(blockKey);

        ECBlock ecblock = null;
        if (bytes != null) {
            ecblock = new ECBlock(blockId, blockKey, bytes);
            logger.debug("CacheHit " + key + " block " + blockId);
        } else
            logger.debug("CacheMiss " + key + " block " + blockId);

        return ecblock;
    }

    private List<ECBlock> readCache(final String key) {
        List<ECBlock> ecblocks = new ArrayList<ECBlock>();

        // read blocks in parallel
        CompletionService<ECBlock> completionService = new ExecutorCompletionService<ECBlock>(executor);
        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            final int blockNumFin = i;
            completionService.submit(new Callable<ECBlock>() {
                @Override
                public ECBlock call() throws Exception {
                    return readBlockCache(key, blockNumFin);
                }
            });
        }

        int success = 0;
        int errors = 0;
        Set<byte[]> blocks = new HashSet<byte[]>();
        while (success < blocksincache) {
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
            if (errors > (LonghairLib.m + LonghairLib.k - blocksincache))
                break;
        }

        return ecblocks;
    }

    private Set<Integer> getMissingIds(List<ECBlock> ecblocksCached) {
        Set<Integer> cached = new HashSet<Integer>();
        for (ECBlock ecblock : ecblocksCached) {
            cached.add(ecblock.getId());
        }

        Set<Integer> missing = new HashSet<Integer>();
        int id = 0;
        while (id < LonghairLib.k + LonghairLib.m) {
            if (cached.contains(id) == false)
                missing.add(id);
            id++;
        }
        return missing;
    }

    private ECBlock readBlockBackend(String key, int blockId) {
        String blockKey = key + blockId;
        int s3ConnNum = blockId % s3Connections.size();
        S3Connection s3Connection = s3Connections.get(s3ConnNum);
        byte[] bytes = s3Connection.read(blockKey);

        ECBlock ecblock = null;
        if (bytes != null) {
            ecblock = new ECBlock(blockId, blockKey, bytes);
            logger.debug("ReadBlockBackend " + key + " block" + blockId + " from bucket" + s3ConnNum);
        } else
            logger.error("[Error] ReadBlockBackend " + key + " block" + blockId + " from bucket" + s3ConnNum);

        return ecblock;
    }

    private List<ECBlock> readBackend(final String key, final int numMissing, Set<Integer> missingIds) {
        CompletionService<ECBlock> completionService = new ExecutorCompletionService<ECBlock>(executor);
        for (final int missingId : missingIds) {
            completionService.submit(new Callable<ECBlock>() {
                @Override
                public ECBlock call() throws Exception {
                    return readBlockBackend(key, missingId);
                }
            });
        }

        int success = 0;
        int errors = 0;
        int errorsMax = missingIds.size() - numMissing;
        List<ECBlock> ecblocks = new ArrayList<ECBlock>();
        while (success < numMissing) {
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
            if (errors > errorsMax)
                break;
        }

        return ecblocks;
    }

    @Override
    public byte[] read(final String key, final int keyNum) {
        byte[] data = null;
        // read from cache
        List<ECBlock> ecblocksCached = readCache(key);
        if (ecblocksCached.size() < LonghairLib.k) {
            Set<Integer> missingIds = getMissingIds(ecblocksCached);
            int numMissing = LonghairLib.k - ecblocksCached.size();
            final List<ECBlock> ecblocksBackend = readBackend(key, numMissing, missingIds);

            // cache in background
            if (ecblocksCached.size() == 0 && ecblocksBackend.size() > 0) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        int counter = 0;
                        for (int i = ecblocksBackend.size() - 1; i >= 0; i--) {
                            ECBlock ecblock = ecblocksBackend.get(i);
                            cacheBlock(key, ecblock);
                            counter++;
                            if (counter == blocksincache)
                                break;
                        }
                    }
                });
            }

            if (ecblocksCached.size() + ecblocksBackend.size() >= LonghairLib.k) {
                Set<byte[]> blockBytes = new HashSet<byte[]>();
                for (ECBlock ecblock : ecblocksCached)
                    blockBytes.add(ecblock.getBytes());
                for (ECBlock ecblock : ecblocksBackend)
                    blockBytes.add(ecblock.getBytes());
                data = LonghairLib.decode(blockBytes);
                if (ecblocksCached.size() > 0) {
                    logger.info("Read " + key + " " + data.length + " bytes Cache: " + ecblocksCached.size() + " Backend: " + ecblocksBackend.size());
                    cachePartialHits.incrementAndGet();
                } else {
                    logger.info("Read " + key + " " + data.length + " bytes Backend: " + ecblocksBackend.size());
                    cacheMisses.incrementAndGet();
                }
            } else
                logger.error("[Error] Read " + key);
        } else {
            Set<byte[]> blockBytes = new HashSet<byte[]>();
            for (ECBlock ecblock : ecblocksCached)
                blockBytes.add(ecblock.getBytes());
            data = LonghairLib.decode(blockBytes);
            logger.info("Read " + key + " " + data.length + " bytes Cache: " + ecblocksCached.size());
            cacheHits.incrementAndGet();
        }

        /*try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

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
