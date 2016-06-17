package com.yahoo.ycsb.dual.clients;

/*
  IntelliJ
  Main: com.yahoo.ycsb.Client
  VM options: -Xmx3g
  Program arguments: -client com.yahoo.ycsb.dual.clients.BackendClient -p fieldlength=4194304 -P workloads/myworkload
  Working directory: /home/ubuntu/work/repos/YCSB
  Use classpath of module: root
  JRE: 1.8
*/

/*
   Command line:
   cd YCSB
   mvn clean package
   bin/ycsb load backend -s -threads 1 -p fieldlength=1048576 -P workloads/myworkload
   bin/ycsb run backend -threads 1 -p fieldlength=1048576 -P workloads/myworkload
*/

import com.yahoo.ycsb.ClientBlueprint;
import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.liberasure.LonghairLib;
import com.yahoo.ycsb.common.properties.PropertyFactory;
import com.yahoo.ycsb.common.s3.S3Connection;
import com.yahoo.ycsb.dual.utils.ClientUtils;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.*;

/*
 Assumptions:
 - number of Amazon regions = k + m
 - there is one S3 bucket per Amazon region
*/

public class BackendClient extends ClientBlueprint {
    public static Logger logger = Logger.getLogger(BackendClient.class);
    public static PropertyFactory propertyFactory;

    // S3 bucket names mapped to connections to AWS S3 buckets
    private List<S3Connection> s3Connections;
    private ExecutorService executor;
    private List<String> s3Buckets;

    private void initS3() {
        List<String> regions = Arrays.asList(propertyFactory.propertiesMap.get(PropertyFactory.S3_REGIONS_PROPERTY).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(propertyFactory.propertiesMap.get(PropertyFactory.S3_ENDPOINTS_PROPERTY).split("\\s*,\\s*"));
        s3Buckets = Arrays.asList(propertyFactory.propertiesMap.get(PropertyFactory.S3_BUCKETS_PROPERTY).split("\\s*,\\s*"));
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

    @Override
    public void init() throws ClientException {
        logger.debug("BackendClient.init() start");
        propertyFactory = new PropertyFactory(getProperties());

        initS3();
        initLonghair();

        // init executor service
        if (executor == null) {
            final int threadsNum = Integer.valueOf(propertyFactory.propertiesMap.get(PropertyFactory.EXECUTOR_THREADS_PROPERTY));
            logger.debug("threads num: " + threadsNum);
            executor = Executors.newFixedThreadPool(threadsNum);
        }
        logger.debug("BackendClient.init() end");
    }

    @Override
    public void cleanup() throws ClientException {
        logger.debug("Cleaning up.");
        if (executor.isTerminated())
            executor.shutdownNow();
    }


    private byte[] readBlock(String baseKey, int blockNum) throws InterruptedException {
        String blockKey = baseKey + blockNum;
        int s3ConnNum = blockNum % s3Connections.size();
        S3Connection s3Connection = s3Connections.get(s3ConnNum);
        byte[] block = s3Connection.read(blockKey);
        //logger.debug("ReadBlock " + blockNum + " " + blockKey + " " + ClientUtils.bytesToHash(block));
        logger.debug("ReadBlock " + baseKey + " " + blockNum + " " + s3Buckets.get(s3ConnNum));
        return block;
    }

    @Override
    public byte[] read(final String key, final int keyNum) {

        //long starttime = System.currentTimeMillis();
        List<Future> tasks = new ArrayList<Future>();

        // read blocks in parallel
        CompletionService<byte[]> completionService = new ExecutorCompletionService<byte[]>(executor);
        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            //try {
            final int blockNumFin = i;
            Future newTask = completionService.submit(new Callable<byte[]>() {
                @Override
                public byte[] call() throws Exception {
                    byte[] toRet = null;

                    try {
                        toRet = readBlock(key, blockNumFin);
                    } catch (InterruptedException e) {
                    }

                    return toRet;
                }
            });

            tasks.add(newTask);
            //} catch(RejectedExecutionException e) {}
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

        if (data != null)
            logger.info("Read " + key + " " + data.length + "B"); // + ClientUtils.bytesToHash(data));
        else
            logger.error("Error reading " + key);

        //long endtime = System.currentTimeMillis();
        //System.out.println("Read: " + key + " " + starttime + ":" + endtime + " " + (endtime - starttime));

        return data;
    }

    @Override
    public Status update(String key, byte[] value) {
        return null;
    }

    private Status insertBlock(String baseKey, int blockNum, byte[] block) {
        String blockKey = baseKey + blockNum;
        int s3ConnNum = blockNum % s3Connections.size();
        S3Connection s3Connection = s3Connections.get(s3ConnNum);
        Status status = s3Connection.insert(blockKey, block);
        logger.info("InsertBlock " + baseKey + " " + blockNum + " " + s3Buckets.get(s3ConnNum));
        return status;
    }

    /* insert data (encoded or full) in S3 buckets */
    @Override
    public Status insert(String key, byte[] value) {
        Status status = Status.OK;

        // encode data
        Set<byte[]> encodedBlocks = LonghairLib.encode(value);

        // insert encoded blocks in parallel
        final String keyFin = key;
        CompletionService<Status> completionService = new ExecutorCompletionService<Status>(executor);
        int counter = 0;
        for (final byte[] block : encodedBlocks) {
            final int blockNumFin = counter;
            counter++;
            completionService.submit(new Callable<Status>() {
                @Override
                public Status call() throws Exception {
                    return insertBlock(keyFin, blockNumFin, block);
                }
            });
        }

        int success = 0;
        int errors = 0;
        while (success < encodedBlocks.size()) {
            Future<Status> statusFuture = null;
            try {
                statusFuture = completionService.take();
            } catch (InterruptedException e) {
                logger.error("Exception completionService.take()");
                //e.printStackTrace();
            }
            if (statusFuture != null) {
                Status insertStatus = null;
                try {
                    insertStatus = statusFuture.get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    logger.error("Exception for block insert operation.");
                    errors++;
                    //e.printStackTrace();
                }
                if (insertStatus == Status.OK)
                    success++;
                else
                    errors++;
            }

            if (errors > LonghairLib.m)
                break;
        }

        // set status
        if (success < LonghairLib.k)
            status = Status.ERROR;

        logger.info("Insert " + key + " " + value.length + "B " + ClientUtils.bytesToHash(value));
        return status;
    }

    @Override
    public Status delete(String key) {
        return null;
    }
}
