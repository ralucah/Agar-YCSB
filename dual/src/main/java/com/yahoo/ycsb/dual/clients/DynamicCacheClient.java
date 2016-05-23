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
import com.yahoo.ycsb.dual.utils.PropertyFactory;
import com.yahoo.ycsb.dual.utils.Storage;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/*
  IntelliJ
  Main: com.yahoo.ycsb.Client
  VM options: -Xmx3g
  Program arguments: -client com.yahoo.ycsb.dual.clients.GreedyCacheClient -p fieldlength=4194304 -P workloads/myworkload
  Working directory: /home/ubuntu/work/repos/YCSB
  Use classpath of module: root
  JRE: 1.8
*/

/*
   Command line:
   cd YCSB
   mvn clean package
   bin/ycsb proxy -p cachesize=40 -P workloads/myworkload
   bin/ycsb run greedy -threads 2 -p fieldlength=4194304 -P workloads/myworkload
*/

public class DynamicCacheClient extends ClientBlueprint {
    public static Logger logger = Logger.getLogger(DynamicCacheClient.class);

    public static AtomicInteger cacheHits;
    public static AtomicInteger cachePartialHits;
    public static AtomicInteger cacheMisses;

    public static PropertyFactory propertyFactory;

    private List<S3Connection> s3Connections;
    private MemcachedConnection memConnection;
    private ProxyConnection proxyConnection;
    private int blocksPerRegion;
    private ExecutorService executor;

    private void initS3() {
        List<String> regions = Arrays.asList(propertyFactory.propertiesMap.get(PropertyFactory.S3_REGIONS_PROPERTY).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(propertyFactory.propertiesMap.get(PropertyFactory.S3_ENDPOINTS_PROPERTY).split("\\s*,\\s*"));
        List<String> s3Buckets = Arrays.asList(propertyFactory.propertiesMap.get(PropertyFactory.S3_BUCKETS_PROPERTY).split("\\s*,\\s*"));
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
        if (LonghairLib.Longhair.INSTANCE._cauchy_256_init(2) != 0) {
            logger.error("Error initializing longhair");
        }
    }

    private void initCache() throws ClientException {
        proxyConnection = new ProxyConnection(getProperties());

        String memHost = propertyFactory.propertiesMap.get(PropertyFactory.MEMCACHED_SERVER_PROPERTY);
        memConnection = new MemcachedConnection(memHost);
        logger.debug("Memcached connection " + memHost);
    }

    public void init() throws ClientException {
        logger.debug("SmartCacheClient.init() start");

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
        blocksPerRegion = (LonghairLib.k + LonghairLib.m) / s3Connections.size();

        if (executor == null) {
            final int threadsNum = Integer.valueOf(propertyFactory.propertiesMap.get(PropertyFactory.EXECUTOR_THREADS_PROPERTY));
            logger.debug("threads num: " + threadsNum);
            executor = Executors.newFixedThreadPool(threadsNum);
        }
        logger.debug("SmartCacheClient.init() end");
    }


    @Override
    public void cleanup() throws ClientException {
        logger.error("Hits: " + cacheHits + " Misses: " + cacheMisses + " PartialHits: " + cachePartialHits);
        if (executor.isTerminated())
            executor.shutdown();
    }

    private List<Integer> getBlockIdsByRegion(String region) {
        List<Integer> blockIds = new ArrayList<Integer>();

        // identify connection id
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

        // compute block ids
        int s3ConnNum = s3Connections.size();
        int blocksPerRegion = (LonghairLib.k + LonghairLib.m) / s3ConnNum;
        for (int i = 0; i < s3ConnNum; i++) {
            int blockId = i * blocksPerRegion + s3connId;
            blockIds.add(blockId);
        }
        return blockIds;
    }

    private ECBlock readBackendBlock(String key, int blockId) {
        String blockKey = key + blockId;
        return new ECBlock(blockId, blockKey, new byte[699072], Storage.BACKEND);
        /*int s3ConnNum = blockId % s3Connections.size();
        S3Connection s3Connection = s3Connections.get(s3ConnNum);
        byte[] bytes = s3Connection.read(blockKey);

        ECBlock ecblock = null;
        if (bytes != null)
            ecblock = new ECBlock(blockId, blockKey, bytes, Storage.BACKEND);
        else
            logger.error("[Error] ReadBlockBackend " + key + " block" + blockId + " from bucket" + s3ConnNum);

        return ecblock;*/
    }

    public List<ECBlock> readBackend(final String key, List<String> backendRecipe) {
        List<ECBlock> ecblocks = new ArrayList<ECBlock>();

        int blocksNum = 0;
        CompletionService<ECBlock> completionService = new ExecutorCompletionService<ECBlock>(executor);

        // for each region
        for (final String region : backendRecipe) {
            List<Integer> blockIds = getBlockIdsByRegion(region);
            blocksNum += blockIds.size();

            // read each block
            for (Integer blockId : blockIds) {
                final int blockIdFin = blockId;
                completionService.submit(new Callable<ECBlock>() {
                    @Override
                    public ECBlock call() throws Exception {
                        return readBackendBlock(key, blockIdFin);
                    }
                });
            }
        }

        int success = 0;
        int errors = 0;
        while (success < blocksNum) {
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
            if (errors == blocksNum)
                break;
        }

        return ecblocks;
    }

    private void cacheBlock(String key, ECBlock ecblock) {
        Status status = memConnection.insert(ecblock.getKey(), ecblock.getBytes());
        if (status == Status.OK)
            logger.debug("Cache  " + key + " block " + ecblock.getId() + " at " + memConnection.getHost());
        else
            logger.debug("[Error] Cache  " + key + " block " + ecblock.getId() + " at " + memConnection.getHost());
    }

    private ECBlock readCacheBlock(final String key, int blockId) {
        String blockKey = key + blockId;
        byte[] bytes = memConnection.read(blockKey);

        ECBlock ecblock = null;
        if (bytes != null) {
            logger.debug("CacheHit " + key + " block " + blockId);
            ecblock = new ECBlock(blockId, blockKey, bytes, Storage.CACHE);
        } else {
            logger.debug("CacheMiss " + key + " block " + blockId);
            ecblock = readBackendBlock(key, blockId);
            if (ecblock != null) {
                final ECBlock ecblockFin = ecblock;
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        cacheBlock(key, ecblockFin);
                    }
                });
            }
        }
        return ecblock;
    }

    private List<ECBlock> readCache(final String key, List<String> cacheRecipe) {
        List<ECBlock> ecblocks = new ArrayList<ECBlock>();

        int blocksNum = 0;
        CompletionService<ECBlock> completionService = new ExecutorCompletionService<ECBlock>(executor);

        // for each region
        for (final String region : cacheRecipe) {
            List<Integer> blockIds = getBlockIdsByRegion(region);
            blocksNum += blockIds.size();

            // read each block
            for (Integer blockId : blockIds) {
                final int blockIdFin = blockId;
                completionService.submit(new Callable<ECBlock>() {
                    @Override
                    public ECBlock call() throws Exception {
                        return readCacheBlock(key, blockIdFin);
                    }
                });
            }
        }

        int success = 0;
        int errors = 0;
        while (success < blocksNum) {
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
            if (errors == blocksNum)
                break;
        }

        int fromCache = 0;
        for (ECBlock ecblock : ecblocks) {
            if (ecblock.getStorage() == Storage.CACHE)
                fromCache++;
        }

        if (blocksNum > 0 && fromCache == blocksNum)
            cacheHits.incrementAndGet();
        else if (fromCache > 0 && fromCache < blocksNum)
            cachePartialHits.incrementAndGet();
        else if (fromCache == 0)
            cacheMisses.incrementAndGet();

        return ecblocks;
    }

    @Override
    public byte[] read(final String key, final int keyNum) {
        final ProxyReply reply = proxyConnection.sendRequest(key);
        //logger.info(reply.prettyPrint());

        // read from cache and backend in parallel
        List<ECBlock> ecblocks = new ArrayList<ECBlock>();
        CompletionService<List<ECBlock>> completionService = new ExecutorCompletionService<List<ECBlock>>(executor);
        if (reply.getCacheRecipe().size() > 0) {
            completionService.submit(new Callable<List<ECBlock>>() {
                @Override
                public List<ECBlock> call() throws Exception {
                    return readCache(key, reply.getCacheRecipe());
                }
            });
        } else {
            cacheMisses.incrementAndGet();
        }
        if (reply.getS3Recipe().size() > 0) {
            completionService.submit(new Callable<List<ECBlock>>() {
                @Override
                public List<ECBlock> call() throws Exception {
                    return readBackend(key, reply.getS3Recipe());
                }
            });
        }

        // wait for at least k blocks in total
        int success = 0;
        int errors = 0;
        while (success < LonghairLib.k) {
            try {
                Future<List<ECBlock>> resultFuture = completionService.take();
                List<ECBlock> ecblocksRes = resultFuture.get();
                if (ecblocksRes != null) {
                    ecblocks.addAll(ecblocksRes);
                    success += ecblocksRes.size();
                } else
                    errors++;
            } catch (Exception e) {
                errors++;
                logger.debug("Exception reading block.");
            }
            if (errors > LonghairLib.m)
                break;
        }

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

        byte[] data = LonghairLib.decode(blockBytes);
        logger.info("Read " + key + " " + data.length + " bytes Cache: " + fromCache + " Backend: " + fromBackend);

        return data;

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
