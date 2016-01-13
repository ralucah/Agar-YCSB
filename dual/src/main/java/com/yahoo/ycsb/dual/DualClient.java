package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

public class DualClient extends DB {
    public static final String DUAL_PROPERTIES = "dual.properties";
    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    //public static final String MEMCACHED_HOSTS_PROPERTY = "memcached.hosts";
    protected static Logger logger = Logger.getLogger("com.yahoo.ycsb.dual");
    private static Mode mode;

    private static List<String> s3Buckets;
    private static List<String> s3Regions;
    private static List<String> s3EndPoints;

    private static int numConnections;

    private static List<S3Connection> s3Connections;
    private static List<MemcachedConnection> memcachedConnections;

    private static boolean memFlush = false;

    private static boolean erasureCoding = false;

    private static Properties props;

    /**
     * initialize connections to AWS S3
     */
    private void initS3() throws DBException {
        s3Connections = new ArrayList<S3Connection>();

        s3Regions = Arrays.asList(props.getProperty("s3.regions").split("\\s*,\\s*"));
        s3EndPoints = Arrays.asList(props.getProperty("s3.endPoints").split("\\s*,\\s*"));

        // check if settings are valid
        int s3BucketsSize = s3Buckets.size();
        int s3RegionsSize = s3Regions.size();
        int s3EndPointsSize = s3EndPoints.size();
        if (s3BucketsSize != s3RegionsSize || s3RegionsSize != s3EndPointsSize || s3BucketsSize != s3EndPointsSize) {
            logger.error("Num buckets should be equal to num regions and num endpoints. Check dual.properties");
            System.exit(1);
        }

        // set number of connections
        numConnections = s3Buckets.size();

        // establish S3 connections: one per data center
        for (int i = 0; i < s3BucketsSize; i++) {
            S3Connection s3Connection = new S3Connection(s3Regions.get(i), s3EndPoints.get(i));
            s3Connections.add(s3Connection);
        }
    }

    /**
     * initialize connections to Memcached
     */
    private void initMemcached() {
        memcachedConnections = new ArrayList<MemcachedConnection>();

        memFlush = Boolean.valueOf(props.getProperty("memcached.flush"));

        List<String> memcachedHosts = Arrays.asList(props.getProperty("memcached.hosts").split("\\s*,\\s*"));
        for (String memcachedHost : memcachedHosts) {
            MemcachedConnection conn = new MemcachedConnection(memcachedHost);
            memcachedConnections.add(conn);
        }

        numConnections = memcachedConnections.size();
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
    }

    /**
     * initialize dual client
     *
     * @throws DBException
     */
    @Override
    public void init() throws DBException {
        logger.debug("DualClient.init()");

        // get properties
        InputStream propFile = DualClient.class.getClassLoader().getResourceAsStream(DUAL_PROPERTIES);
        props = new Properties();
        try {
            props.load(propFile);
        } catch (IOException e) {
            logger.error("Could not find" + DUAL_PROPERTIES);
            System.exit(1);
        }

        // always use the S3 buckets as table names
        s3Buckets = Arrays.asList(props.getProperty("s3.buckets").split("\\s*,\\s*"));

        // get longhair attributes
        erasureCoding = Boolean.valueOf(props.getProperty("longhair.enable"));
        if (erasureCoding)
            initLonghair();

        // get mode
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
                // TODO how to exit properly?
                logger.error("Invalid mode in " + DUAL_PROPERTIES + ". Mode should be: s3 / memcached / dual.");
                System.exit(-1);
                break;
        }
    }

    @Override
    public void cleanup() throws DBException {
        if (memcachedConnections != null) {
            for (MemcachedConnection conn : memcachedConnections) {
                if (memFlush == true) {
                    conn.flush();
                }
                conn.cleanup();
            }
        }
    }

    public Result readOneBlock(String key, Mode readMode) {
        Result result = new Result();
        int connId = Mapper.mapKeyToDatacenter(key, numConnections);
        final String bucket = s3Buckets.get(connId);

        switch (readMode) {
            case S3: {
                result = s3Connections.get(connId).read(bucket, key);
                break;
            }
            case MEMCACHED: {
                result = memcachedConnections.get(connId).read(bucket, key);
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


    public List<Result> readKBlocks(String key, Mode readMode) {
        List<Result> results = new ArrayList<Result>();
        Map<Future, Boolean> futures = new HashMap<Future, Boolean>();

        // TODO how many threads in the thread pool?
        ExecutorService executor = Executors.newFixedThreadPool(LonghairLib.k + LonghairLib.m);

        int counter = 0;
        while (counter < LonghairLib.k + LonghairLib.m) {
            final int counterFin = counter;
            Future<Result> future = executor.submit(() -> {
                //logger.debug("Reading key " + (key+counterFin) + " returned " + toRet);
                return readOneBlock(key + counterFin, readMode);
            });
            futures.put(future, false);
            //logger.debug("Future <" + future + "> checks key " + (key+counterFin));
            counter++;
        }

        // TODO should give up in a while
        int receivedBlocks = 0;
        //logger.debug("k = " + LonghairLib.k + " " + futures.entrySet().size());
        int cancelled = 0;
        while (receivedBlocks < LonghairLib.k && cancelled < futures.entrySet().size()) {
            Iterator it = futures.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Future, Boolean> pair = (Map.Entry) it.next();
                Future future = pair.getKey();
                Boolean done = pair.getValue();
                if (done.equals(false) && future.isDone()) {
                    try {
                        Result res = (Result) future.get();

                        if (res != null) {
                            if (res.getStatus() == Status.OK) {
                                receivedBlocks++;
                                pair.setValue(true);
                                results.add(res);
                            } else {
                                future.cancel(true);
                                cancelled++;
                            }
                        }
                    } catch (Exception e) {
                        logger.error("Exception in future!");
                        future.cancel(true); // may interrupt if running
                    }
                }
            }
        }

        executor.shutdown();

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
                if (erasureCoding) {
                    // read k blocks
                    List<Result> results = readKBlocks(key, mode);
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
                    Result res = readOneBlock(key, mode);
                    status = res.getStatus();
                    bytes = res.getBytes();
                }
                break;
            }
            case DUAL: {
                if (erasureCoding) {
                    // try to read k blocks from memcached
                    List<Result> results = readKBlocks(key, Mode.MEMCACHED);
                    if (results != null && results.size() > 0) {
                        logger.debug("Cache hit");
                        status = Status.OK;
                        List<byte[]> blocks = new ArrayList<byte[]>();
                        int ok = 0;
                        for (Result res : results) {
                            blocks.add(res.getBytes());
                            if (res.getStatus() == Status.OK)
                                ok++;
                        }
                        if (ok >= LonghairLib.k) {
                            status = Status.OK;
                            bytes = LonghairLib.decode(blocks);
                        }
                    } else {
                        // if unsuccessful, try to read k blocks from s3
                        logger.debug("Cache miss");

                        results = readKBlocks(key, Mode.S3);

                        if (results == null || results.size() < LonghairLib.k)
                            status = Status.ERROR;
                        else {
                            List<byte[]> blocks = new ArrayList<byte[]>();
                            int ok = 0;
                            for (Result res : results) {
                                blocks.add(res.getBytes());
                                if (res.getStatus() == Status.OK)
                                    ok++;
                            }
                            if (ok >= LonghairLib.k) {
                                status = Status.OK;
                                bytes = LonghairLib.decode(blocks);
                            } else
                                status = Status.ERROR;

                            // then, if successful, store the blocks in memcached
                            int row = 0;
                            Status cacheStatus;
                            for (Result res : results) {
                                String blockKey = key + row;
                                row++;
                                cacheStatus = insertOneBlock(blockKey, res.getBytes(), Mode.MEMCACHED);
                                if (cacheStatus != Status.OK) {
                                    status = Status.ERROR;
                                    logger.error("Error caching block!");
                                    break;
                                }
                            }
                        }
                    }
                } else {
                    // try to read data from memcached
                    Result res = readOneBlock(key, Mode.MEMCACHED);
                    status = res.getStatus();
                    bytes = res.getBytes();

                    if (status == Status.ERROR) {
                        logger.debug("Cache miss");

                        // try to get from s3
                        res = readOneBlock(key, Mode.S3);
                        status = res.getStatus();
                        bytes = res.getBytes();

                        // cache it
                        if (status == Status.OK) {
                            status = insertOneBlock(key, bytes, Mode.MEMCACHED);
                        }
                    } else
                        logger.debug("Cache hit");
                }
                break;
            }
            default: {
                logger.error("Unknown mode");
                break;
            }
        }

        String msg = "Read_" + mode +
            " Key:" + key +
            " Status:" + status.getName() +
            " EC:" + erasureCoding + " ";

        if (bytes != null) {
            // now transform bytes to result hashmap
            msg += Utils.bytesToHex(bytes);
            result.put(key, new ByteArrayByteIterator(bytes));
        } else {
            msg += "null";
        }
        logger.debug(msg);

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

    private byte[] valuesToBytes(HashMap<String, ByteIterator> values) {
        // get the first value
        int fieldCount = values.size();
        Object keyToSearch = values.keySet().toArray()[0];
        byte[] sourceArray = values.get(keyToSearch).toArray();
        int sizeArray = sourceArray.length;

        // use it to generate new value
        int totalSize = sizeArray * fieldCount;
        byte[] bytes = new byte[totalSize];
        int offset = 0;
        for (int i = 0; i < fieldCount; i++) {
            System.arraycopy(sourceArray, 0, bytes, offset, sizeArray);
            offset += sizeArray;
        }
        return bytes;
    }

    /* helper function for inserting to s3 or memcached */
    private Status insertOneBlock(String key, byte[] bytes, Mode insertMode) {
        Status status = null;

        // map to data center
        int connId = Mapper.mapKeyToDatacenter(key, numConnections);
        String bucket = s3Buckets.get(connId);

        //logger.debug("DualClient.insertBlock" + mode + "(" + blockKey + " " + bucket + " " + bytesToHex(block) + ")");

        switch (insertMode) {
            case S3: {
                status = s3Connections.get(connId).insert(bucket, key, bytes);
                break;
            }
            case MEMCACHED: {
                status = memcachedConnections.get(connId).insert(bucket, key, bytes);
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
        Status status = null;

        Mode imode = mode;
        if (mode == Mode.DUAL)
            imode = Mode.S3;

        // generate bytes array based on values
        byte[] bytes = valuesToBytes(values);

        if (erasureCoding) {
            // encode data using Longhair
            List<byte[]> blocks = LonghairLib.encode(bytes);

            // store each block
            int row = 0;
            for (byte[] block : blocks) {
                String blockKey = key + row;
                row++;

                status = insertOneBlock(blockKey, block, imode);
                if (status != Status.OK)
                    break;
            }
        } else
            status = insertOneBlock(key, bytes, imode);

        logger.debug("Insert_" + mode +
            " Key:" + key + " " + Utils.bytesToHex(bytes) +
            " Status:" + status.getName() +
            " EC:" + erasureCoding);
        return status;
    }

    @Override
    public Status delete(String table, String key) {
        logger.error("delete() not supported yet");
        System.exit(0);
        return null;
    }
}
