package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.dual.policy.EncodedDataPolicy;
import com.yahoo.ycsb.dual.policy.FullDataPolicy;
import com.yahoo.ycsb.dual.policy.StoragePolicy;
import com.yahoo.ycsb.dual.utils.MemcachedRegion;
import com.yahoo.ycsb.dual.utils.ProximityClassifier;
import com.yahoo.ycsb.dual.utils.S3Region;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

public class DualClient extends DB {
    public static Logger logger = Logger.getLogger("com.yahoo.ycsb.dual");
    private static Properties props;

    // s3 / memcached / dual
    private Mode mode;

    // backend (s3)
    private List<S3Client> s3Clients;
    private List<String> s3Buckets;
    private StoragePolicy s3Policy;
    private boolean s3Encode = false;

    // cache (memcached)
    private List<MemcachedClient> memClients;
    private StoragePolicy memPolicy;
    private boolean memEncode = false;
    private boolean memFlush = false;

    private void initS3() throws DBException {
        // get properties
        s3Buckets = Arrays.asList(props.getProperty("s3.buckets").split("\\s*,\\s*"));
        List<String> regionNames = Arrays.asList(props.getProperty("s3.regions").split("\\s*,\\s*"));
        List<String> endPointNames = Arrays.asList(props.getProperty("s3.endPoints").split("\\s*,\\s*"));

        // compile array of s3 regions
        List<S3Region> s3Regions = new ArrayList<S3Region>();
        if (s3Buckets.size() != regionNames.size() ||
            s3Buckets.size() != endPointNames.size() ||
            regionNames.size() != endPointNames.size())
            logger.error("Configuration error: #buckets must match #regions and #endpoints");
        for (int i = 0; i < s3Buckets.size(); i++) {
            s3Regions.add(new S3Region(regionNames.get(i), endPointNames.get(i), s3Buckets.get(i)));
        }

        // create connections to each s3 region
        s3Clients = new ArrayList<S3Client>();
        ProximityClassifier.sortS3Regions(s3Regions);
        for (S3Region region : s3Regions) {
            region.print();
            s3Clients.add(new S3Client(region.getRegion(), region.getEndPoint()));
        }

        // whether to encode or not
        s3Encode = Boolean.valueOf(props.getProperty("s3.encode"));
        if (s3Encode == true) {
            if (LonghairLib.initialized == false)
                initLonghair();
            s3Policy = new EncodedDataPolicy(s3Clients.size(), LonghairLib.k + LonghairLib.m);
        } else
            s3Policy = new FullDataPolicy(s3Clients.size());
    }

    private void initMemcached() {
        List<String> memcachedHosts = Arrays.asList(props.getProperty("memcached.hosts").split("\\s*,\\s*"));
        List<MemcachedRegion> memcachedRegions = new ArrayList<MemcachedRegion>();
        for (String memcachedHost : memcachedHosts) {
            String[] tokens = memcachedHost.split(" ");
            memcachedRegions.add(new MemcachedRegion(tokens[0], tokens[1]));
        }

        memClients = new ArrayList<MemcachedClient>();
        for (MemcachedRegion region : memcachedRegions) {
            region.print();
            memClients.add(new MemcachedClient(region.getIp() + ":" + region.getPort()));
        }

        memEncode = Boolean.valueOf(props.getProperty("memcached.encode"));

        if (memEncode == true) {
            if (LonghairLib.initialized == false)
                initLonghair();
            memPolicy = new EncodedDataPolicy(memClients.size(), LonghairLib.k + LonghairLib.m);
        } else
            memPolicy = new FullDataPolicy(memClients.size());

        memFlush = Boolean.valueOf(props.getProperty("memcached.flush"));
    }

    private void initLonghair() {
        LonghairLib.k = Integer.valueOf(props.getProperty("longhair.k"));
        LonghairLib.m = Integer.valueOf(props.getProperty("longhair.m"));

        // check k >= 0 and k < 256
        if (LonghairLib.k < 0 || LonghairLib.k >= 256) {
            logger.error("Invalid longhair.k: k should be >= 0 and < 256.");
            System.exit(1);
        }

        // check m >=0 and m <= 256 - k
        if (LonghairLib.m < 0 || LonghairLib.m > 256 - LonghairLib.k) {
            logger.error("Invalid longhair.m: m should be >= 0 and <= 256 - k.");
            System.exit(1);
        }

        // init longhair
        if (LonghairLib.Longhair.INSTANCE._cauchy_256_init(2) != 0) {
            logger.error("Error initializing longhair");
            System.exit(1);
        }

        LonghairLib.initialized = true;
    }

    @Override
    public void init() throws DBException {
        // properties
        InputStream propFile = DualClient.class.getClassLoader().getResourceAsStream("dual.properties");
        props = new Properties();
        try {
            props.load(propFile);
        } catch (IOException e) {
            logger.error("Could not find dual.properties");
            System.exit(1);
        }

        // mode (s3 / memcached / dual) + liblonghair
        mode = Mode.valueOf(props.getProperty("mode").toUpperCase());
        switch (mode) {
            case S3:
                initS3();
                break;
            case MEMCACHED:
                initMemcached();
                break;
            case DUAL:
                initS3();
                initMemcached();
                break;
            default:
                logger.error("Invalid mode in  dual.properties. Mode should be: s3 / memcached / dual.");
                System.exit(-1);
                break;
        }
    }

    @Override
    public void cleanup() throws DBException {
        if (memClients != null) {
            for (MemcachedClient conn : memClients) {
                if (memFlush == true) {
                    conn.flush();
                }
                conn.cleanup();
            }
        }
    }

    public Result readOneBlock(String key, int blockId, Mode readMode) {
        Result result = new Result();

        int regionNum;
        final String bucket;

        switch (readMode) {
            case S3: {
                regionNum = s3Policy.assignToRegion(key, blockId, readMode);
                bucket = s3Buckets.get(regionNum);
                result = s3Clients.get(regionNum).read(bucket, key);
                break;
            }
            case MEMCACHED: {
                regionNum = memPolicy.assignToRegion(key, blockId, readMode);
                bucket = s3Buckets.get(regionNum);
                result = memClients.get(regionNum).read(bucket, key);
                break;
            }
            default: {
                logger.error("Invalid read mode!");
                break;
            }
        }
        //logger.debug("DualClient.readBlock_" + mode + "(" + key + " " +bucket + " " + bytesToHex(bytes) + ")");
        return result;
    }


    public List<Result> readKBlocks(String key, Mode readMode, List<Result> results) {
        List<Future> futures = new ArrayList<Future>();

        // TODO how many threads in the thread pool?
        Executor executor = Executors.newFixedThreadPool(LonghairLib.k + LonghairLib.m);
        CompletionService<Result> completionService = new ExecutorCompletionService<Result>(executor);

        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            final String keyFin = key + i;
            final int iFin = i;
            completionService.submit(new Callable<Result>() {
                @Override
                public Result call() throws Exception {
                    return readOneBlock(keyFin, iFin, readMode);
                }
            });
        }

        /* best effort read at least k valid blocks */
        int errors = 0;
        while (results.size() < LonghairLib.k) {
            Future<Result> resultFuture = null;
            try {
                resultFuture = completionService.take();
                Result res = resultFuture.get();
                if (res.getStatus() == Status.OK) {
                    if (!results.contains(res))
                        results.add(res);
                } else
                    errors++;
            } catch (Exception e) {
                errors++;
                logger.debug("Exception reading a block.");
            }
            if (errors == LonghairLib.k + LonghairLib.m)
                break;
        }

        return results;
    }


    // TODO possible optimization: batch requests to the same data center (S3 bucket or memcached server)
    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        Status status = null;
        byte[] bytes = null;

        switch (mode) {
            case S3:
            case MEMCACHED: {
                if (memEncode) {
                    // read k blocks
                    List<Result> results = new ArrayList<Result>();
                    readKBlocks(key, mode, results);
                    if (results == null || results.size() < LonghairLib.k)
                        status = Status.ERROR;
                    else {
                        List<byte[]> blocks = new ArrayList<byte[]>();
                        status = results.get(0).getStatus();
                        for (Result res : results) {
                            blocks.add(res.getBytes());
                            if (res.getStatus() != status)
                                status = res.getStatus();
                        }
                        bytes = LonghairLib.decode(blocks);
                    }
                } else {
                    // read full data
                    Result res = readOneBlock(key, -1, mode);
                    status = res.getStatus();
                    bytes = res.getBytes();
                }
                break;
            }
            case DUAL: {
                // result: full data or encoded chunks
                Result res = null;
                List<Result> results = new ArrayList<Result>();
                int numCachedChunks = 0;

                // read from cache
                if (memEncode == false) {
                    // full data
                    res = readOneBlock(key, -1, Mode.MEMCACHED);
                    status = res.getStatus();
                    bytes = res.getBytes();
                } else {
                    // encoded chunks
                    readKBlocks(key, Mode.MEMCACHED, results);
                    numCachedChunks = results.size();

                    // if at least k valid chunks were retrieved
                    if (results.size() >= LonghairLib.k) {
                        status = Status.OK;

                        // decode the blocks
                        List<byte[]> blocks = new ArrayList<byte[]>();
                        for (Result r : results)
                            blocks.add(r.getBytes());
                        bytes = LonghairLib.decode(blocks);
                    } else
                        status = Status.ERROR;
                }

                if (status == Status.OK && bytes != null) {
                    logger.debug("Cache hit!");
                    // stop here
                } else {
                    logger.debug("Cache (partial) miss");

                    // read from backend
                    if (s3Encode == false) {
                        // full data
                        res = readOneBlock(key, -1, Mode.S3);
                        status = res.getStatus();
                        bytes = res.getBytes();
                    } else {
                        // encoded chunks
                        readKBlocks(key, Mode.S3, results);

                        // if at least k valid chunks were retrieved
                        if (results.size() >= LonghairLib.k) {
                            status = Status.OK;

                            // decode the blocks
                            List<byte[]> blocks = new ArrayList<byte[]>();
                            for (Result r : results)
                                blocks.add(r.getBytes());
                            bytes = LonghairLib.decode(blocks);
                        } else
                            status = Status.ERROR;
                    }

                    // cache data!
                    if (status == Status.OK && bytes != null) {
                        Status statusCache = Status.ERROR;
                        if (memEncode == false) {
                            statusCache = insertOneBlock(key, -1, bytes, Mode.MEMCACHED);
                        } else {
                            // cache blocks
                            if (s3Encode == false) {
                                // divide data into blocks
                                List<byte[]> blocks = LonghairLib.encode(bytes);

                                if (results.size() > 0) {
                                    for (byte[] block : blocks) {
                                        if (Utils.containsBlock(results, block) == false) {
                                            statusCache = insertOneBlock(key, -1, block, Mode.MEMCACHED);
                                            if (statusCache != Status.OK)
                                                logger.warn("Error caching block!");
                                        }
                                    }
                                }
                            } else {
                                // store the blocks that are not already stored
                                for (int i = numCachedChunks; i < results.size(); i++) {
                                    Result r = results.get(i);
                                    if (r.getStatus() == Status.OK) {
                                        statusCache = insertOneBlock(key, i, r.getBytes(), Mode.MEMCACHED);
                                        if (statusCache != Status.OK)
                                            logger.warn("Error caching block!");
                                    } else
                                        logger.warn("Block retrieved from backedn not ok!");
                                }
                            }
                        }
                    } else {
                        logger.warn("Could not read data from S3!");
                    }
                }
                break;
            }
            default: {
                logger.error("Unknown mode");
                break;
            }
        }
        String msg = "Read_" + mode +
            " Key:" + key;
        //" EC:" + erasureCoding + " ";

        if (bytes != null) {
            status = Status.OK;
            // now transform bytes to result hashmap
            msg += Utils.bytesToHex(bytes) + " ";
            result.put(key, new ByteArrayByteIterator(bytes));
        } else {
            status = Status.ERROR;
            msg += "null";
        }
        logger.debug(msg + "Status: " + status.getName());

        return status;
    }


    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        logger.error("scan() not supported yet");
        System.exit(0);
        return null;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        logger.error("update() not supported yet");
        System.exit(0);
        return null;
    }

    /* helper function for inserting to s3 or memcached */
    private Status insertOneBlock(String key, int blockId, byte[] bytes, Mode insertMode) {
        Status status = null;

        // map to data center
        int connId;
        String bucket;

        //logger.debug("DualClient.insertBlock" + mode + "(" + blockKey + " " + bucket + " " + bytesToHex(block) + ")");

        switch (insertMode) {
            case S3: {
                connId = s3Policy.assignToRegion(key, blockId, insertMode);
                bucket = s3Buckets.get(connId);
                status = s3Clients.get(connId).insert(bucket, key, bytes);
                break;
            }
            case MEMCACHED: {
                connId = memPolicy.assignToRegion(key, blockId, insertMode);
                bucket = s3Buckets.get(connId);
                status = memClients.get(connId).insert(bucket, key, bytes);
                break;
            }
            default: {
                logger.error("Invalid mode!");
                break;
            }
        }
        return status;
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {

        Mode insertMode = mode;
        if (mode == Mode.DUAL)
            insertMode = Mode.S3;

        // generate bytes array based on values
        byte[] bytes = Utils.valuesToBytes(values);
        Status status = Status.ERROR;
        switch (insertMode) {
            case S3: {
                if (s3Encode == false) {
                    // full data
                    status = insertOneBlock(key, -1, bytes, Mode.S3);
                } else {
                    // encoded data
                    List<byte[]> blocks = LonghairLib.encode(bytes);

                    // store each block
                    int row = 0;
                    for (byte[] block : blocks) {
                        String blockKey = key + row;
                        row++;

                        status = insertOneBlock(blockKey, row, block, Mode.S3);
                        if (status != Status.OK)
                            break;
                    }
                }
                break;
            }
            case MEMCACHED: {
                if (memEncode == false) {
                    // full data
                    status = insertOneBlock(key, -1, bytes, Mode.MEMCACHED);
                } else {
                    // encoded data
                    List<byte[]> blocks = LonghairLib.encode(bytes);

                    // store each block
                    int row = 0;
                    for (byte[] block : blocks) {
                        String blockKey = key + row;
                        row++;

                        status = insertOneBlock(blockKey, row, block, Mode.MEMCACHED);
                        if (status != Status.OK)
                            break;
                    }
                }
                break;
            }
            default: {
                logger.error("Unknown insert case!");
                break;
            }
        }

        logger.debug("Insert_" + mode +
            " Key:" + key + " " + Utils.bytesToHex(bytes) +
            " Status:" + status.getName());
        return status;
    }

    @Override
    public Status delete(String table, String key) {
        logger.error("delete() not supported yet");
        System.exit(0);
        return null;
    }
}
