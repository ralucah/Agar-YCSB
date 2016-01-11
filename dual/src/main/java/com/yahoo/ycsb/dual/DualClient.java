package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
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

    public byte[] readBlock(String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        byte[] bytes = null;
        int connId = Mapper.mapKeyToDatacenter(key, numConnections);
        final String bucket = s3Buckets.get(connId);

        switch (mode) {
            case S3: {
                bytes = s3Connections.get(connId).read(bucket, key);
                break;
            }
            case MEMCACHED: {
                bytes = memcachedConnections.get(connId).read(bucket, key);
                break;
            }
            case DUAL: {
                // try to read from memcached
                final MemcachedConnection memConn = memcachedConnections.get(connId);
                bytes = memConn.read(bucket, key);

                // if cache miss, read from S3
                if (bytes == null) {
                    logger.debug("Cache miss!");

                    // get from s3
                    bytes = s3Connections.get(connId).read(bucket, key);

                    // store to memcached
                    //memConn.insert(bucket, key, bytes);

                    //store in memcached in a different thread, in the background
                    final String keyFinal = key;
                    final HashMap<String, ByteIterator> resultFinal = new HashMap<String, ByteIterator>(result);
                    final byte[] bytesFin = new byte[bytes.length];
                    System.arraycopy(bytes, 0, bytesFin,0, bytes.length);
                    new Thread() {
                        @Override
                        public void run() {
                            memConn.insert(bucket, keyFinal, bytesFin);
                        }
                    }.start();
                } else {
                    logger.debug("Cache hit!");
                }

                break;
            }
            default: {
                logger.error("Invalid mode!");
                break;
            }
        }

        //logger.debug("DualClient.readBlock_" + mode + "(" + key + " " +bucket + " " + bytesToHex(bytes) + ")");
        return bytes;
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        // TODO possible optimization: batch requests to the same data center (S3 bucket or memcached server)
        byte[] bytes;
        if (erasureCoding) {
            Map<Future, Boolean> futures = new HashMap<Future, Boolean>();
            List<byte[]> results = new ArrayList<byte[]>();

            ExecutorService executor = Executors.newFixedThreadPool(LonghairLib.k + LonghairLib.m); // how many threads in the thread pool?

            int counter = 0;
            while (counter < LonghairLib.k + LonghairLib.m) {
                final int counterFin = counter;
                Future<byte[]> future = executor.submit(() -> {
                    byte[] toRet = readBlock(key + counterFin, fields, result);
                    //logger.debug("Reading key " + (key+counterFin) + " returned " + toRet);
                    return toRet;
                });
                futures.put(future, false);
                //logger.debug("Future <" + future + "> checks key " + (key+counterFin));
                counter++;
            }

            // TODO should give up in a while
            int receivedBlocks = 0;
            //logger.debug("k = " + LonghairLib.k + " " + futures.entrySet().size());
            while (receivedBlocks < LonghairLib.k) {
                Iterator it = futures.entrySet().iterator();
                while (it.hasNext()) {
                    Map.Entry<Future, Boolean> pair = (Map.Entry) it.next();
                    Future future = pair.getKey();
                    Boolean done = pair.getValue();
                    if (done.equals(false) && future.isDone()) {
                        try {
                            byte[] res = (byte[])future.get();
                            if (res != null) {
                                receivedBlocks++;
                                pair.setValue(true);
                                results.add(res);
                            }
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        } catch (ExecutionException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }
            assert(receivedBlocks >= LonghairLib.k);

            // decode needs Map<Integer, byte[]> blocksBytes
            // so transform results into List<byte[]> blocksBytes

            bytes = LonghairLib.decode(results);
        } else {
            bytes = readBlock(key, fields, result);
        }
        Status status = Status.ERROR;
        if (bytes != null) {
            status = Status.OK;
            // now transform bytes to result hashmap
            logger.debug("DualClient.read_" + mode + "(" + key + " " + Utils.bytesToHex(bytes) + ")");
            result.put(key, new ByteArrayByteIterator(bytes));
        }

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

    /* helper function for insert */
    private Status insertBytes(String key, byte[] bytes) {
        Status status = null;

        // map to data center
        int connId = Mapper.mapKeyToDatacenter(key, numConnections);
        String bucket = s3Buckets.get(connId);

        //logger.debug("DualClient.insertBlock" + mode + "(" + blockKey + " " + bucket + " " + bytesToHex(block) + ")");

        switch (mode) {
            case S3: {
                status = s3Connections.get(connId).insert(bucket, key, bytes);
                break;
            }
            case MEMCACHED: {
                status = memcachedConnections.get(connId).insert(bucket, key, bytes);
                break;
            }
            case DUAL: {
                // insert in S3
                status = s3Connections.get(connId).insert(bucket, key, bytes);
                // TODO to cache or not to cache on insert?
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

        // generate bytes array based on values
        byte[] bytes = valuesToBytes(values);
        logger.debug("DualClient.insert_" + mode + "(" + key + " " + Utils.bytesToHex(bytes) + ") EC:" + erasureCoding);

        if (erasureCoding) {
            // encode data using Longhair
            List<byte[]> blocks = LonghairLib.encode(bytes);

            // store each block
            int row = 0;
            for (byte[] block : blocks) {
                String blockKey = key + row;
                row++;
                status = insertBytes(blockKey, block);
                if (status != Status.OK)
                    break;
            }
        } else
            status = insertBytes(key, bytes);

        return status;
    }

    @Override
    public Status delete(String table, String key) {
        logger.error("delete() not supported yet");
        System.exit(0);
        return null;
    }
}
