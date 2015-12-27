package com.yahoo.ycsb.db;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.SSECustomerKey;
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

    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    private static List<String> s3Buckets;
    private static List<String> s3Regions;
    private static List<S3Connection> s3Connections;
    private static List<MemcachedConnection> memcachedConnections;
    private static String mode;
    private static int numDatacenters;

    @Override
    public void init() throws DBException {
        System.out.println("DualClient.init()");

        /* S3 */
        s3Connections = new ArrayList<S3Connection>();

        InputStream propFile = DualClient.class.getClassLoader()
            .getResourceAsStream("dual.properties");
        Properties props = new Properties();
        try {
            props.load(propFile);
        } catch (IOException e) {
            System.err.println("IOException");
            //e.printStackTrace();
        }

        mode = props.getProperty("mode").toLowerCase();
        if (!mode.equals("s3") && !mode.equals("memcached") && !mode.equals("dual")) {
            System.err.println("mode should be: s3/memcached/dual; check dual.properties");
            System.exit(-1);
        }

        s3Buckets = Arrays.asList(props.getProperty("s3.buckets").split("\\s*,\\s*"));
        s3Regions = Arrays.asList(props.getProperty("s3.regions").split("\\s*,\\s*"));
        List<String> s3EndPoints = Arrays.asList(props.getProperty("s3.endPoints").split("\\s*,\\s*"));

        int s3BucketsSize = s3Buckets.size();
        int s3RegionsSize = s3Regions.size();
        int s3EndPointsSize = s3EndPoints.size();
        if (s3BucketsSize != s3RegionsSize || s3RegionsSize != s3EndPointsSize || s3BucketsSize != s3EndPointsSize) {
            System.err.println("Num buckets should be equal to num regions and num endpoints. Check dual.properties");
            System.exit(1);
        }

        numDatacenters = s3Buckets.size();

        for (int i = 0; i < s3BucketsSize; i++) {
            S3Connection s3Connection = new S3Connection(s3Regions.get(i), s3EndPoints.get(i));
            s3Connections.add(s3Connection);
        }

        /* Memcached */
        memcachedConnections = new ArrayList<MemcachedConnection>();
        List<String> memcachedHosts = Arrays.asList(props.getProperty("memcached.hosts").split("\\s*,\\s*"));
        for (String memcachedHost : memcachedHosts) {
            System.out.println(memcachedHost);
            MemcachedConnection conn = new MemcachedConnection(memcachedHost);
            memcachedConnections.add(conn);
        }

        System.out.println("Quitting nicely...");
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        int datacenterId = Mapper.mapKeyToDatacenter(key, numDatacenters);
        String bucket = s3Buckets.get(datacenterId);
        if (mode.equals("s3")) {
            System.out.println("DualClient.read_s3(" + bucket + ", " + key + ")");
            return s3Connections.get(datacenterId).read(bucket, key, fields, result);
        } else if (mode.equals("memcached")) {
            MemcachedConnection memServer = memcachedConnections.get(datacenterId);
            System.out.println("DualClient.read_memcached(" + bucket + ", " + key + ")");
            return memServer.read(bucket,key, fields,result);
        }
        return null;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        int datacenterId = Mapper.mapKeyToDatacenter(startkey, s3Buckets.size());
        String bucket = s3Buckets.get(datacenterId);
        if (mode.equals("s3")) {
            System.out.println("DualClient.scan_s3(" + bucket + ", " + startkey + ")");
            return s3Connections.get(datacenterId).scan(bucket, startkey, recordcount, fields, result);
        } else if (mode.equals("memcached")) {
            MemcachedConnection memServer = memcachedConnections.get(datacenterId);
            System.out.println("DualClient.scan_memcached(" + bucket + ", " + startkey + ")");
            return memServer.scan(bucket, startkey, recordcount, fields, result);
        }
        return null;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        int datacenterId = Mapper.mapKeyToDatacenter(key, s3Buckets.size());
        String bucket = s3Buckets.get(datacenterId);
        if (mode.equals("s3")) {
            System.out.println("DualClient.update_s3(" + bucket + ", " + key + ")");
            return s3Connections.get(datacenterId).update(bucket, key, values);
        } else if (mode.equals("memcached")) {
            MemcachedConnection memServer = memcachedConnections.get(datacenterId);
            System.out.println("DualClient.update_memcached(" + bucket + ", " + key + ")");
            return memServer.update(bucket, key, values);
        }
        return null;
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        int datacenterId = Mapper.mapKeyToDatacenter(key, s3Buckets.size());
        String bucket = s3Buckets.get(datacenterId);
        if (mode.equals("s3")) {
            System.out.println("DualClient.insert_s3(" + bucket + ", " + key + ")");
            return s3Connections.get(datacenterId).insert(s3Buckets.get(datacenterId), key, values);
        } else if (mode.equals("memcached")) {
            MemcachedConnection memServer = memcachedConnections.get(datacenterId);
            System.out.println("DualClient.insert_memcached(" + bucket + ", " + key + ")");
            return memServer.insert(bucket, key, values);
        }
        return null;
    }

    @Override
    public Status delete(String table, String key) {
        int datacenterId = Mapper.mapKeyToDatacenter(key, s3Buckets.size());
        String bucket = s3Buckets.get(datacenterId);
        if (mode.equals("s3")) {
            System.out.println("DualClient.delete_s3(" + bucket + ", " + key + ")");
            return s3Connections.get(datacenterId).delete(bucket, key);
        } else if (mode.equals("memcached")) {
            MemcachedConnection memServer = memcachedConnections.get(datacenterId);
            System.out.println("DualClient.delete_memcached(" + bucket + ", " + key + ")");
            return memServer.delete(bucket, key);
        }
        return null;
    }
}
