package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.db.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load

import com.sun.jna.Memory;
import com.sun.jna.Pointer;
import com.yahoo.ycsb.*;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class DualClient extends DB {
    //public static final String MEMCACHED_HOSTS_PROPERTY = "memcached.hosts";
    public static final String DUAL_PROPERTIES = "dual.properties";

    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

    private static Mode mode;

    private static List<String> s3Buckets;
    private static List<String> s3Regions;
    private static List<String> s3EndPoints;

    private static int numConnections;

    private static List<S3Connection> s3Connections;
    private static List<MemcachedConnection> memcachedConnections;

    private static boolean memFlush = false;

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
            System.err.println("Num buckets should be equal to num regions and num endpoints. Check dual.properties");
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
            System.out.println(memcachedHost);
            MemcachedConnection conn = new MemcachedConnection(memcachedHost);
            memcachedConnections.add(conn);
        }

        numConnections = memcachedConnections.size();
    }

    /**
     * initialize dual client
     *
     * @throws DBException
     */
    @Override
    public void init() throws DBException {
        System.out.println("DualClient.init()");

        // get properties
        InputStream propFile = DualClient.class.getClassLoader().getResourceAsStream(DUAL_PROPERTIES);
        props = new Properties();
        try {
            props.load(propFile);
        } catch (IOException e) {
            System.err.println("Could not find" + DUAL_PROPERTIES);
            System.exit(1);
        }

        // always use the S3 buckets as table names
        s3Buckets = Arrays.asList(props.getProperty("s3.buckets").split("\\s*,\\s*"));

        // get longhair attributes
        LonghairLib.k = Integer.valueOf(props.getProperty("longhair.k"));
        LonghairLib.m = Integer.valueOf(props.getProperty("longhair.m"));
        assert(LonghairLib.k >= 0 && LonghairLib.k < 256);
        assert(LonghairLib.m >= 0 && LonghairLib.m <= 256 - LonghairLib.k);

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
                System.err.println("Invalid mode in " + DUAL_PROPERTIES + ". Mode should be: s3 / memcached / dual.");
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

    public byte[] readBlock(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        byte[] bytes = null;
        int connId = Mapper.mapKeyToDatacenter(key, numConnections);
        final String bucket = s3Buckets.get(connId);

        //System.out.println("DualClient.read_" + mode + "(" + bucket + ", " + key + ")");

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
                    System.out.println("Cache miss!");

                    // get from s3
                    bytes = s3Connections.get(connId).read(bucket, key);

                    //store in memcached in a different thread, in the background
                    final String keyFinal = key;
                    final HashMap<String, ByteIterator> resultFinal = new HashMap<String, ByteIterator>(result);
                    new Thread() {
                        @Override
                        public void run() {
                            memConn.insert(bucket, keyFinal, resultFinal.get(keyFinal).toArray());
                        }
                    }.start();
                } else {
                    System.out.println("Cache hit!");
                }

                break;
            }
            default: {
                System.err.println("Invalid mode!");
                break;
            }
        }

        return bytes;
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        // TODO possible optimization: batch requests to the same data center (S3 bucket or memcached server)

        Map<Future, Boolean> futures = new HashMap<Future, Boolean>();
        List<byte[]> results = new ArrayList<byte[]>();
        ExecutorService executor = Executors.newFixedThreadPool(LonghairLib.k); // how many threads in the thread pool?

        int counter = 0;
        while (counter < LonghairLib.k + LonghairLib.m) {
            final int counterFin = counter;
            Future<byte[]> future = executor.submit(() -> {
                return readBlock(table, key + counterFin, fields, result);
            });
            futures.put(future, false);
            counter++;
        }

        // TODO should give up in a while
        int receivedBlocks = 0;
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

        byte[] bytes = LonghairLib.decode(results);

        // now transform bytes to result hashmap
        System.out.println("DualClient.read_" + mode + "(" + key + " " + bytesToHex(bytes) + ")");
        result.put(key, new ByteArrayByteIterator(bytes));

        return Status.OK;

    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        System.err.println("scan() not supported yet");
        System.exit(0);
        return null;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        System.err.println("update() not supported yet");
        System.exit(0);
        return null;
        /*Status status = null;

        int connId = Mapper.mapKeyToDatacenter(key, numConnections);
        String bucket = s3Buckets.get(connId);

        System.out.println("DualClient.update_" + mode + "(" + bucket + ", " + key + ")");

        switch (mode) {
            case S3: {
                status = s3Connections.get(connId).update(bucket, key, values);
                break;
            }
            case MEMCACHED: {
                status = memcachedConnections.get(connId).update(bucket, key, values);
                break;
            }
            case DUAL: {
                // TODO handle this in parallel
                // update in cache or delete from cache?
                // does the status matter?
                memcachedConnections.get(connId).update(bucket, key, values);
                // update in S3
                status = s3Connections.get(connId).update(bucket, key, values);
                break;
            }
            default: {
                System.err.println("Invalid mode!");
                break;
            }
        }

        return status;*/
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

    /*private byte[] deconstructBlocks(List<byte[]> blocks) {
        Map<Integer, byte[]> dataBlocks = new HashMap<Integer, byte[]>();

        for (byte[] block : blocks){
            int dataBlockSize = block.length - (LonghairLib.reservedBytes * 2);

            // divide full value into original length, row number, value
            byte[] lengthBytes = new byte[LonghairLib.reservedBytes];
            byte[] rowBytes = new byte[LonghairLib.reservedBytes];
            byte[] actualData = new byte[dataBlockSize];

            int offset = 0;
            System.arraycopy(block, offset, lengthBytes, 0, LonghairLib.reservedBytes);
            offset += LonghairLib.reservedBytes;
            System.arraycopy(block, offset, rowBytes, 0, LonghairLib.reservedBytes);
            offset += LonghairLib.reservedBytes;
            System.arraycopy(block, offset, actualData, 0, dataBlockSize);

            // obtain int
            int originalLength = ByteBuffer.wrap(lengthBytes).getInt();
            int row = ByteBuffer.wrap(rowBytes).getInt();

            // add row and value to block
            dataBlocks.put(row, actualData);
        }
        return null;
    }*/

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        Status status = null;

        // generate bytes array based on values
        byte[] bytes = valuesToBytes(values);
        System.out.println("DualClient.insert_" + mode + "(" + key + " " + bytesToHex(bytes) + ")");

        int bytesLen = bytes.length;
        byte[] lengthBytes = ByteBuffer.allocate(LonghairLib.reservedBytes).putInt(bytesLen).array();

        // encode data using Longhair
        List<byte[]> blocks = LonghairLib.encode(bytes);

        // store each block
        int row = 0;
        for (byte[] block : blocks) {
            String blockKey = key + row;

            // map to data center
            int connId = Mapper.mapKeyToDatacenter(key, numConnections);
            String bucket = s3Buckets.get(connId);

            //System.out.println("DualClient.insert_" + mode + "(" + bucket + ", " + blockKey + " " + bytesToHex(newBlock) + ")");

            switch (mode) {
                case S3: {
                    status = s3Connections.get(connId).insert(bucket, blockKey, block);
                    break;
                }
                case MEMCACHED: {
                    status = memcachedConnections.get(connId).insert(bucket, blockKey, block);
                    break;
                }
                case DUAL: {
                    // insert in S3
                    status = s3Connections.get(connId).insert(bucket, blockKey, block);
                    // TODO to cache or not to cache on insert?
                    break;
                }
                default: {
                    System.err.println("Invalid mode!");
                    break;
                }
            }

            if (status != Status.OK)
                break;
        }

        return status;
    }

    @Override
    public Status delete(String table, String key) {
        System.err.println("delete() not supported yet");
        System.exit(0);
        return null;

        /*Status status = null;

        int connId = Mapper.mapKeyToDatacenter(key, numConnections);
        String bucket = s3Buckets.get(connId);

        System.out.println("DualClient.delete_" + mode + "(" + bucket + ", " + key + ")");

        switch (mode) {
            case S3: {
                status = s3Connections.get(connId).delete(bucket, key);
                break;
            }
            case MEMCACHED: {
                status = memcachedConnections.get(connId).delete(bucket, key);
                break;
            }
            case DUAL: {
                // delete from S3
                status = s3Connections.get(connId).delete(bucket, key);
                // TODO delete or invalidate cache?
                memcachedConnections.get(connId).delete(bucket, key);
                break;
            }
            default: {
                System.err.println("Invalid mode!");
                break;
            }
        }

        return status;*/
    }
}
