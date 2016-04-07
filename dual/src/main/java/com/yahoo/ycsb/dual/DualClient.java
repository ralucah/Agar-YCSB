package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.dual.utils.ClientConstants;
import com.yahoo.ycsb.dual.utils.ClientUtils;
import com.yahoo.ycsb.dual.utils.LonghairLib;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

/*
 Assumptions:
 - number of Amazon regions = k + m
 - there is one S3 bucket per Amazon region
 */

public class DualClient extends DB {
    protected static Logger logger = Logger.getLogger(DualClient.class);

    private Properties properties;

    // S3 bucket names mapped to connections to AWS S3 buckets
    private List<S3Connection> s3Connections;

    // for concurrent processing
    private ExecutorService executor;

    // TODO Assumption: one bucket per region (num regions = num endpoints = num buckets)
    private void initS3() {
        // s3-related configuration
        int s3ZonesNum = Integer.valueOf(properties.getProperty(ClientConstants.S3_ZONES));
        List<String> regions = Arrays.asList(properties.getProperty(ClientConstants.S3_REGIONS).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(properties.getProperty(ClientConstants.S3_ENDPOINTS).split("\\s*,\\s*"));
        List<String> s3Buckets = Arrays.asList(properties.getProperty(ClientConstants.S3_BUCKETS).split("\\s*,\\s*"));
        if (s3Buckets.size() != s3ZonesNum || endpoints.size() != s3ZonesNum || regions.size() != s3ZonesNum)
            logger.error("Configuration error: #buckets = #regions = #endpoints = " + s3ZonesNum);

        // establish S3 connections
        s3Connections = new ArrayList<S3Connection>();
        for (int i = 0; i < s3Buckets.size(); i++) {
            String bucket = s3Buckets.get(i);
            String region = regions.get(i);
            String endpoint = endpoints.get(i);
            try {
                logger.debug("S3 connection " + i + " " + bucket + " " + region + " " + endpoint);
                S3Connection client = new S3Connection(s3Buckets.get(i), regions.get(i), endpoints.get(i));
                s3Connections.add(client);
            } catch (DBException e) {
                logger.error("Error connecting to " + s3Buckets.get(i));
            }
        }
    }

    private void initLonghair() {
        // erasure coding-related configuration
        LonghairLib.k = Integer.valueOf(properties.getProperty(ClientConstants.LONGHAIR_K, ClientConstants.LONGHAIR_K_DEFAULT));
        LonghairLib.m = Integer.valueOf(properties.getProperty(ClientConstants.LONGHAIR_M, ClientConstants.LONGHAIR_M_DEFAULT));
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
    public void init() throws DBException {
        logger.debug("DualClient.init() start");

        // load properties
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        properties = new Properties();
        InputStream in = loader.getResourceAsStream(ClientConstants.PROPERTIES_FILE);
        try {
            properties.load(in);
        } catch (IOException e) {
            logger.error("Error loading properties.");
        }

        initS3();
        initLonghair();

        // init executor service
        final int threadsNum = Integer.valueOf(properties.getProperty(ClientConstants.THREADS_NUM, ClientConstants.THREADS_NUM_DEFAULT));
        logger.debug("threads num: " + threadsNum);
        executor = Executors.newFixedThreadPool(threadsNum);

        logger.debug("DualClient.init() end");
    }

    @Override
    public void cleanup() throws DBException {
        logger.debug("Cleaning up.");
        executor.shutdown();
    }


    private byte[] readBlock(String baseKey, int blockNum) {
        String blockKey = baseKey + blockNum;
        S3Connection s3Connection = s3Connections.get(blockNum);
        byte[] block = s3Connection.read(blockKey);
        logger.debug("ReadBlock " + blockKey + ":" + blockNum + " " + ClientUtils.bytesToHash(block));
        return block;
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        Status status = Status.OK;

        // read blocks in parallel
        final String keyFin = key;
        CompletionService<byte[]> completionService = new ExecutorCompletionService<byte[]>(executor);
        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            final int blockNumFin = i;
            completionService.submit(new Callable<byte[]>() {
                @Override
                public byte[] call() throws Exception {
                    return readBlock(keyFin, blockNumFin);
                }
            });
        }

        int success = 0;
        int errors = 0;
        Set<byte[]> blocks = new HashSet<byte[]>();
        while (success + errors < LonghairLib.k + LonghairLib.m) {
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
            if (data == null)
                status = Status.ERROR;
        }
        else
            status = Status.ERROR;

        logger.info("Read " + key + " " + status.getName() + " " + ClientUtils.bytesToHash(data));

        // for debugging purposes
        /*try {
            Thread.sleep(600000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/

        return status;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return null;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        return null;
    }

    private Status insertBlock(String baseKey, int blockNum, byte[] block) {
        String blockKey = baseKey + blockNum;
        S3Connection s3Connection = s3Connections.get(blockNum);
        Status status = s3Connection.insert(blockKey, block);
        logger.debug("InsertBlock " + blockKey + ":" + blockNum + ":" + status.getName());
        return status;
    }

    /* insert data (encoded or full) in S3 buckets */
    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        Status status = Status.OK;

        // generate bytes array based on values
        byte[] data = ClientUtils.valuesToBytes(values);

        // encode data
        Set<byte[]> encodedBlocks = LonghairLib.encode(data);

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
        while (success + errors < encodedBlocks.size()) {
            Future<Status> statusFuture = null;
            try {
                statusFuture = completionService.take();
                Status insertStatus = statusFuture.get();
                if (insertStatus == Status.OK)
                    success++;
                else
                    errors++;
            } catch (Exception e) {
                errors++;
                logger.error("Exception for block insert operation.");
            }
            if (errors > LonghairLib.m)
                break;
        }

        // set status
        if (success < LonghairLib.k)
            status = Status.ERROR;

        logger.info("Insert " + key + " " + status.getName() + " " + ClientUtils.bytesToHash(data));
        return status;
    }

    @Override
    public Status delete(String table, String key) {
        return null;
    }
}
