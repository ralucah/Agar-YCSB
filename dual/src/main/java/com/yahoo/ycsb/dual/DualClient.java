package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DualClient extends DB {
    public static final String DUAL_PROPERTIES = "dual.properties";
    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    //public static final String MEMCACHED_HOSTS_PROPERTY = "memcached.hosts";
    protected static Logger logger = Logger.getLogger("com.yahoo.ycsb.dual");
    private static Mode mode;
    private static Mapper mapper;

    private static List<String> s3Buckets;
    private static List<String> s3Regions;
    private static List<String> s3EndPoints;

    private static int numConnections;

    private static List<S3Client> s3Clients;
    private static List<MemcachedClient> memcachedConnections;

    private static boolean memFlush = false;

    private static boolean erasureCoding = false;

    private static Properties props;

    /**
     * initialize connections to AWS S3
     */
    private void initS3() throws DBException {
        s3Clients = new ArrayList<S3Client>();

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
            S3Client s3Client = new S3Client(s3Regions.get(i), s3EndPoints.get(i));
            s3Clients.add(s3Client);
        }
    }

    /**
     * initialize connections to Memcached
     */
    private void initMemcached() {
        memcachedConnections = new ArrayList<MemcachedClient>();

        memFlush = Boolean.valueOf(props.getProperty("memcached.flush"));

        List<String> memcachedHosts = Arrays.asList(props.getProperty("memcached.hosts").split("\\s*,\\s*"));
        for (String memcachedHost : memcachedHosts) {
            MemcachedClient conn = new MemcachedClient(memcachedHost);
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

        // mapper
        String mapperName = props.getProperty("mapper");
        mapper = MapperFactory.newMapper(mapperName);
        mapper.setNumOfDataCenters(numConnections);
    }

    @Override
    public void cleanup() throws DBException {
        if (memcachedConnections != null) {
            for (MemcachedClient conn : memcachedConnections) {
                if (memFlush == true) {
                    conn.flush();
                }
                conn.cleanup();
            }
        }
    }

    public Result readOneBlock(String key, Mode readMode) {
        Result result = new Result();
        int connId = mapper.assignToDataCenter(key);
        final String bucket = s3Buckets.get(connId);

        switch (readMode) {
            case S3: {
                result = s3Clients.get(connId).read(bucket, key);
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


    public List<Result> readKBlocks(String key, Mode readMode, List<Result> results) {
        List<Future> futures = new ArrayList<Future>();

        // TODO how many threads in the thread pool?
        //ExecutorService executor = Executors.newFixedThreadPool(LonghairLib.k + LonghairLib.m);
        Executor executor = Executors.newFixedThreadPool(LonghairLib.k + LonghairLib.m);
        CompletionService<Result> completionService = new ExecutorCompletionService<Result>(executor);

        for (int i = 0; i < LonghairLib.k + LonghairLib.m; i++) {
            final String keyFin = key + i;
            completionService.submit(new Callable<Result>() {
                @Override
                public Result call() throws Exception {
                    return readOneBlock(keyFin, readMode);
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
                if (erasureCoding) {
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
                    Result res = readOneBlock(key, mode);
                    status = res.getStatus();
                    bytes = res.getBytes();
                }
                break;
            }
            case DUAL: {
                if (erasureCoding) {
                    // try to read at least k valid blocks from memcached
                    List<Result> results = new ArrayList<Result>();
                    readKBlocks(key, Mode.MEMCACHED, results);

                    // if at least k valid blocks were retrieved
                    if (results.size() >= LonghairLib.k) {
                        logger.debug("Cache hit " + results.size());
                        List<byte[]> blocks = new ArrayList<byte[]>();
                        for (Result res : results)
                            blocks.add(res.getBytes());
                        bytes = LonghairLib.decode(blocks);
                    } else {
                        if (results.size() > 0)
                            logger.debug("Cache partial hit " + results.size());
                        else
                            logger.debug("Cache miss " + results.size());

                        // try to retrieve missing blocks from the S3 backend
                        readKBlocks(key, Mode.S3, results);

                        // if at least k valid blocks are now available
                        if (results.size() >= LonghairLib.k) {
                            List<byte[]> blocks = new ArrayList<byte[]>();
                            for (Result res : results)
                                blocks.add(res.getBytes());
                            bytes = LonghairLib.decode(blocks);
                        } else {
                            // not enough blocks available in the system
                            logger.debug("Not enough blocks available in the system for " + key);
                        }

                        // cache the new blocks
                        // store the new blocks in memcached
                        int row = 0;
                        Status cacheStatus;
                        for (Result res : results) {
                            String blockKey = key + row;
                            row++;
                            cacheStatus = insertOneBlock(blockKey, res.getBytes(), Mode.MEMCACHED);
                            if (cacheStatus != Status.OK) {
                                logger.error("Error caching block " + blockKey);
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
            " EC:" + erasureCoding + " ";

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
    private Status insertOneBlock(String key, byte[] bytes, Mode insertMode) {
        Status status = null;

        // map to data center
        int connId = mapper.assignToDataCenter(key);
        String bucket = s3Buckets.get(connId);

        //logger.debug("DualClient.insertBlock" + mode + "(" + blockKey + " " + bucket + " " + bytesToHex(block) + ")");

        switch (insertMode) {
            case S3: {
                status = s3Clients.get(connId).insert(bucket, key, bytes);
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
        byte[] bytes = Utils.valuesToBytes(values);

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
