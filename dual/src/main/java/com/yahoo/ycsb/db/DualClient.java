package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
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

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        Status status = null;

        int connId = Mapper.mapKeyToDatacenter(key, numConnections);
        final String bucket = s3Buckets.get(connId);

        System.out.println("DualClient.read_" + mode + "(" + bucket + ", " + key + ")");

        switch (mode) {
            case S3: {
                status = s3Connections.get(connId).read(bucket, key, fields, result);
                break;
            }
            case MEMCACHED: {
                status = memcachedConnections.get(connId).read(bucket, key, fields, result);
                break;
            }
            case DUAL: {
                // try to read from memcached
                final MemcachedConnection memConn = memcachedConnections.get(connId);
                status = memConn.read(bucket, key, fields, result);

                // if cache miss, read from S3
                if (status == Status.NOT_FOUND) {
                    System.out.println("Cache miss!");

                    // get from s3
                    status = s3Connections.get(connId).read(bucket, key, fields, result);

                    //store in memcached in a different thread, in the background
                    final String keyFinal = key;
                    final HashMap<String, ByteIterator> resultFinal = new HashMap<String, ByteIterator>(result);
                    new Thread() {
                        @Override
                        public void run() {
                            memConn.insert(bucket, keyFinal, resultFinal);
                        }
                    }.start();
                } else if (status == Status.OK) {
                    System.out.println("Cache hit!");
                }

                break;
            }
            default: {
                System.err.println("Invalid mode!");
                break;
            }
        }

        return status;
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

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        Status status = null;

        int connId = Mapper.mapKeyToDatacenter(key, numConnections);
        String bucket = s3Buckets.get(connId);

        System.out.println("DualClient.insert_" + mode + "(" + bucket + ", " + key + ")");

        switch (mode) {
            case S3: {
                status = s3Connections.get(connId).insert(bucket, key, values);
                break;
            }
            case MEMCACHED: {
                status = memcachedConnections.get(connId).insert(bucket, key, values);
                break;
            }
            case DUAL: {
                // insert in S3
                status = s3Connections.get(connId).insert(bucket, key, values);
                // TODO to cache or not to cache on insert?
                break;
            }
            default: {
                System.err.println("Invalid mode!");
                break;
            }
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
