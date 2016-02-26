package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.dual.utils.*;
import com.yahoo.ycsb.dual.utils.Utils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

public class DualClient extends DB {
    public static Executor executor;
    private static Logger logger = Logger.getLogger(DualClient.class);
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

    /* init helper function: init S3 client connections */
    private void initS3() throws DBException {
        // get s3 properties
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
        //ProximityClassifier.sortS3Regions(s3Regions);
        for (S3Region region : s3Regions) {
            //region.print();
            logger.trace("S3Client: " + region.getRegion() + " " + region.getEndPoint());
            s3Clients.add(new S3Client(region.getRegion(), region.getEndPoint()));
        }

        // whether to encode data or not
        s3Encode = Boolean.valueOf(props.getProperty("s3.encode"));
        logger.trace("s3Encode: " + s3Encode);
        if (s3Encode == true) {
            initLonghair();
            s3Policy = new StoragePolicy(s3Clients.size(), LonghairLib.k + LonghairLib.m);
        } else
            s3Policy = new StoragePolicy(s3Clients.size());
    }

    /* init helper function: init memcached client connections */
    private void initMemcached() {
        // construct list of regions
        List<String> memcachedHosts = Arrays.asList(props.getProperty("memcached.hosts").split("\\s*,\\s*"));
        List<MemcachedRegion> memcachedRegions = new ArrayList<MemcachedRegion>();
        for (String memcachedHost : memcachedHosts) {
            String[] tokens = memcachedHost.split(":");
            memcachedRegions.add(new MemcachedRegion(tokens[0], tokens[1]));
        }
        //ProximityClassifier.sortMemcachedRegions(memcachedRegions);

        // init clients
        memClients = new ArrayList<MemcachedClient>();
        for (MemcachedRegion region : memcachedRegions) {
            //region.print();
            logger.trace("MemcachedClient: " + region.getIp() + ":" + region.getPort());
            memClients.add(new MemcachedClient(region.getIp() + ":" + region.getPort()));
        }

        // whether to encode data or not
        memEncode = Boolean.valueOf(props.getProperty("memcached.encode"));
        logger.trace("memEncode: " + memEncode);
        if (memEncode == true) {
            initLonghair();
            memPolicy = new StoragePolicy(memClients.size(), LonghairLib.k + LonghairLib.m);
        } else
            memPolicy = new StoragePolicy(memClients.size());

        // whether to flush cache or not
        memFlush = Boolean.valueOf(props.getProperty("memcached.flush"));
        logger.trace("memFlush: " + memFlush);
    }

    /* init helper function: init longhair lib for erasure coding */
    private void initLonghair() {
        if (LonghairLib.k == Integer.MIN_VALUE || LonghairLib.m == Integer.MIN_VALUE)
            return;

        // read k and m
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
        logger.trace("k: " + LonghairLib.k + " m: " + LonghairLib.m);
    }

    /**
     * initialize Dual Client: S3 client connections, Memcached client connections, Longhair lib
     *
     * @throws DBException
     */
    @Override
    public void init() throws DBException {
        logger.trace("init() begin");
        // properties
        InputStream propFile = DualClient.class.getClassLoader().getResourceAsStream("dual.properties");
        props = new Properties();
        try {
            props.load(propFile);
        } catch (IOException e) {
            logger.error("Could not find dual.properties");
            System.exit(1);
        }

        // used as table names
        s3Buckets = Arrays.asList(props.getProperty("s3.buckets").split("\\s*,\\s*"));

        // init thread pool
        int threadPoolSize = Integer.valueOf(props.getProperty("threadPoolSize"));
        if (threadPoolSize <= 0) {
            logger.warn("Invalid threadPoolSize! Default: 10");
            threadPoolSize = 10;
        }
        executor = Executors.newFixedThreadPool(threadPoolSize);

        // set mode (s3 / memcached / dual) + liblonghair
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
        logger.trace("init() end");
    }

    /**
     * Flushes memcached clients
     *
     * @throws DBException
     */
    //TODO clean S3 buckets too?
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

    /**
     * Read valid bytes of full data
     *
     * @param key      identifies data record
     * @param readMode S3 or MEMCACHED
     * @return valid bytes or null
     */
    private byte[] readFullData(String key, Mode readMode) {
        byte[] bytes = null;
        switch (readMode) {
            case S3: {
                int connId = s3Policy.assignFullDataToRegion(key, Mode.S3);
                String bucket = s3Buckets.get(connId);
                bytes = s3Clients.get(connId).read(bucket, key);
                break;
            }
            case MEMCACHED: {
                int connId = memPolicy.assignFullDataToRegion(key, Mode.MEMCACHED);
                String bucket = s3Buckets.get(connId);
                bytes = memClients.get(connId).read(bucket, key);
                break;
            }
            default: {
                logger.error("Invalid read mode!");
                break;
            }
        }
        if (bytes != null)
            logger.trace("readFullData_" + readMode + "(" + key + ") return: " + bytes.length + " bytes");
        else
            logger.trace("readFullData_" + readMode + "(" + key + ") return: null");
        return bytes;
    }

    /**
     * Read valid bytes of an encoded block
     *
     * @param blockKey identifies the block record
     * @param blockId  id within the encoded data
     * @param readMode S3 or MEMCACHED
     * @return valid block bytes or null
     */
    private BlockResult readBlock(String blockKey, int blockId, Mode readMode) {
        BlockResult block = null;
        switch (readMode) {
            case S3: {
                int connId = s3Policy.assignEncodedBlockToRegion(blockKey, blockId, Mode.S3);
                String bucket = s3Buckets.get(connId);
                byte[] bytes = s3Clients.get(connId).read(bucket, blockKey);
                block = new BlockResult(blockKey, blockId, bytes);
                break;
            }
            case MEMCACHED: {
                int connId = memPolicy.assignEncodedBlockToRegion(blockKey, blockId, Mode.MEMCACHED);
                String bucket = s3Buckets.get(connId);
                byte[] bytes = memClients.get(connId).read(bucket, blockKey);
                block = new BlockResult(blockKey, blockId, bytes);
                break;
            }
            default: {
                logger.error("Invalid read mode!");
                break;
            }
        }
        if (block != null)
            logger.trace("readBlock_" + readMode + "(" + blockKey + "," + blockId + ") return: " + block.getBytes().length + " bytes");
        else
            logger.trace("readBlock_" + readMode + "(" + blockKey + ":" + blockId + ") return: null");
        return block;
    }

    /**
     * Try to read at least k valid blocks
     *
     * @param key      of encoded record
     * @param readMode S3 or MEMCACHED
     * @param results  append blocks here
     */
    private void readEncodedBlocks(String key, Mode readMode, List<BlockResult> results) {
        List<Future> futures = new ArrayList<Future>();
        // one completion service per k blocks
        CompletionService<BlockResult> completionService = new ExecutorCompletionService<BlockResult>(executor);
        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            final String keyFin = key + i;
            final int iFin = i;
            completionService.submit(new Callable<BlockResult>() {
                @Override
                public BlockResult call() throws Exception {
                    return readBlock(keyFin, iFin, readMode);
                }
            });
        }

        // best effort read at least k valid blocks
        int errors = 0;
        while (results.size() < LonghairLib.k) {
            Future<BlockResult> resultFuture = null;
            try {
                resultFuture = completionService.take();
                BlockResult res = resultFuture.get();
                if (res != null) {
                    if (!results.contains(res))
                        results.add(res);
                } else
                    errors++;
            } catch (Exception e) {
                errors++;
                logger.trace("Exception reading a block.");
            }
            if (errors > LonghairLib.m)
                break;
        }
        logger.trace("readEncodedBlocks_" + readMode + "(" + key + ") return: " + results.size() + " results");
    }

    // TODO possible optimization: batch requests to the same data center (S3 bucket or memcached server)

    /**
     * Read record identified by key from bucket identified by table
     *
     * @param table  The name of the table / bucket
     * @param key    The record key of the record to read
     * @param fields The list of fields to read, or null for all of them / not used
     * @param result A HashMap of field/value pairs for the result
     * @return operation status
     */
    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        Status status = Status.ERROR;
        byte[] bytes = null;

        switch (mode) {
            case S3: {
                if (s3Encode == false) {
                    // read full data
                    bytes = readFullData(key, Mode.S3);
                } else {
                    // read encoded blocks and decode
                    List<BlockResult> results = new ArrayList<BlockResult>();
                    readEncodedBlocks(key, Mode.S3, results);
                    if (results != null && results.size() >= LonghairLib.k)
                        bytes = LonghairLib.decode(Utils.blocksToBytes(results));
                }
                break;
            }
            case MEMCACHED: {
                if (memEncode == false) {
                    // read full data
                    bytes = readFullData(key, Mode.MEMCACHED);
                } else {
                    // read encoded blocks and decode
                    List<BlockResult> results = new ArrayList<BlockResult>();
                    readEncodedBlocks(key, Mode.MEMCACHED, results);
                    if (results != null && results.size() >= LonghairLib.k)
                        bytes = LonghairLib.decode(Utils.blocksToBytes(results));
                }
                break;
            }
            case DUAL: {
                // remember the cached blocks, even if they are less than k
                List<BlockResult> readBlocks = new ArrayList<BlockResult>();
                int numCachedBlocks = 0;

                /* read from cache */
                if (memEncode == false) {
                    bytes = readFullData(key, Mode.MEMCACHED);
                } else {
                    readEncodedBlocks(key, Mode.MEMCACHED, readBlocks);
                    if (readBlocks.size() >= LonghairLib.k)
                        bytes = LonghairLib.decode(Utils.blocksToBytes(readBlocks));
                    else
                        numCachedBlocks = readBlocks.size();
                }

                /* if cache miss, read from backend */
                if (bytes == null) {
                    logger.debug("Cache miss!");
                    if (s3Encode == false) {
                        bytes = readFullData(key, Mode.S3);
                    } else {
                        readEncodedBlocks(key, Mode.S3, readBlocks);
                        if (readBlocks.size() >= LonghairLib.k)
                            bytes = LonghairLib.decode(Utils.blocksToBytes(readBlocks));
                    }

                    /* cache data */
                    if (bytes != null) {
                        if (memEncode == false) {
                            insertBytes(key, bytes, Mode.MEMCACHED); // TODO return sth?
                        } else {
                            if (s3Encode == false) {
                                // divide data into blocks and cache the diff with readBlocks
                                List<byte[]> blocks = LonghairLib.encode(bytes);
                                int id = 0;
                                for (byte[] blockBytes : blocks) {
                                    if (Utils.containsBlock(readBlocks, blockBytes) == false)
                                        insertBlock(new BlockResult(key + id, id, blockBytes), Mode.MEMCACHED);
                                    id++;
                                }
                            } else {
                                // store the blocks that are not already stored
                                for (int i = numCachedBlocks; i < readBlocks.size(); i++)
                                    insertBlock(readBlocks.get(i), Mode.MEMCACHED);
                            }
                        }
                    } else
                        logger.warn("Could not read data from S3!");
                } else {
                    logger.debug("Cache hit!");
                }
                break;
            }
            default: {
                logger.error("Unknown mode");
                break;
            }
        }
        String msg = "Read_" + mode + "(" + key + ") return: ";
        if (bytes != null) {
            // now transform bytes to result hashmap
            //msg += Utils.bytesToHex(bytes) + " ";
            result.put(key, new ByteArrayByteIterator(bytes));
            status = Status.OK;
            msg += bytes.length + " bytes";
        } else {
            msg += "null";
        }
        logger.debug(msg + " " + status);

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


    /**
     * Insert full data
     *
     * @param key        identifies the data record
     * @param bytes      actual content
     * @param insertMode S3 or MEMCACHED
     * @return Status.OK if success; Status.ERROR otherwise
     */
    private Status insertBytes(String key, byte[] bytes, Mode insertMode) {
        Status status = Status.ERROR;
        switch (insertMode) {
            case S3: {
                int connId = s3Policy.assignFullDataToRegion(key, insertMode);
                String bucket = s3Buckets.get(connId);
                status = s3Clients.get(connId).insert(bucket, key, bytes);
                break;
            }
            case MEMCACHED: {
                int connId = memPolicy.assignFullDataToRegion(key, insertMode);
                String bucket = s3Buckets.get(connId);
                status = memClients.get(connId).insert(bucket, key, bytes);
                break;
            }
            default: {
                logger.error("Invalid mode!");
                break;
            }
        }
        logger.trace("insertBytes_" + insertMode + "(" + key + "," + bytes.length + " bytes) " + status);
        //bytesToHex(block)
        return status;
    }

    /**
     * Insert block
     *
     * @param block
     * @param insertMode S3 or MEMCACHED
     * @return Status.OK if success; Status.ERROR otherwise
     */
    private Status insertBlock(BlockResult block, Mode insertMode) {
        Status status = Status.ERROR;
        switch (insertMode) {
            case S3: {
                int connId = s3Policy.assignEncodedBlockToRegion(block.getKey(), block.getId(), Mode.S3);
                String bucket = s3Buckets.get(connId);
                status = s3Clients.get(connId).insert(bucket, block.getKey(), block.getBytes());
                break;
            }
            case MEMCACHED: {
                int connId = memPolicy.assignEncodedBlockToRegion(block.getKey(), block.getId(), Mode.MEMCACHED);
                String bucket = s3Buckets.get(connId);
                status = memClients.get(connId).insert(bucket, block.getKey(), block.getBytes());
                break;
            }
            default: {
                logger.error("Invalid mode!");
                break;
            }
        }
        logger.trace("InsertBlock_" + insertMode + "(" + block.getKey() + "," + block.getId() + "," +
            block.getBytes().length + " bytes) " + status);
        return status;
    }

    /**
     * Insert encoded blocks
     *
     * @param key        identifies encoded data record
     * @param blocks     list of bytes
     * @param insertMode S3 or MEMCACHED
     * @return Status.OK if success; Status.ERROR otherwise
     */
    private Status insertEncodedBlocks(String key, List<byte[]> blocks, Mode insertMode) {
        Status status = Status.OK;

        int id = 0;
        for (byte[] blockBytes : blocks) {
            String blockKey = key + id;
            BlockResult block = new BlockResult(blockKey, id, blockBytes);
            Status blockStatus = insertBlock(block, insertMode);
            if (blockStatus != Status.OK) {
                logger.warn("Error inserting encoded block " + blockKey);
                status = Status.ERROR;
            }
            id++;
        }
        //logger.debug("insertEncodedBlocks_" + insertMode + "(" + key + "," + blocks.size() + "blocks) " + status);
        return status;
    }

    /**
     * Insert record in S3 bucket or Memcached
     * @param table The name of the table (bucket name in this case)
     * @param key The record key of the record to insert
     * @param values A HashMap of field/value pairs to insert in the record (processed to generate value bytes)
     * @return operation status (Status.OK or Status.ERROR)
     */
    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        // operation status
        Status status = Status.ERROR;

        // generate bytes array based on values
        byte[] bytes = Utils.valuesToBytes(values);

        // insert in S3 bucket or Memcached
        Mode insertMode = mode;
        if (mode == Mode.DUAL)
            insertMode = Mode.S3;

        switch (insertMode) {
            case S3: {
                if (s3Encode == false) {
                    // full data
                    status = insertBytes(key, bytes, Mode.S3);
                } else {
                    // encoded data
                    List<byte[]> blocks = LonghairLib.encode(bytes);
                    status = insertEncodedBlocks(key, blocks, Mode.S3);
                }
                break;
            }
            case MEMCACHED: {
                if (memEncode == false) {
                    // full data
                    status = insertBytes(key, bytes, Mode.MEMCACHED);
                } else {
                    // encoded data
                    List<byte[]> blocks = LonghairLib.encode(bytes);
                    status = insertEncodedBlocks(key, blocks, Mode.MEMCACHED);
                }
                break;
            }
            default: {
                logger.error("Unknown insert case!");
                break;
            }
        }

        logger.debug("Insert_" + mode + "(" + key + "," + bytes.length + " bytes) " + status);
        return status;
    }

    @Override
    public Status delete(String table, String key) {
        logger.error("delete() not supported yet");
        System.exit(0);
        return null;
    }
}
