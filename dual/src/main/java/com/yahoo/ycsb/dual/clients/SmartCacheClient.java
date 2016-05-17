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

import java.util.*;
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

    public List<ECBlock> readBackend(String key, List<String> backendRecipe) {
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

    private ECBlock readCacheBlock(String key, int blockId) {
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

    private List<ECBlock> readCache(String key, List<String> cacheRecipe) {
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
