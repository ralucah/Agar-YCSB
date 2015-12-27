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
    private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);
    private static List<String> s3Buckets;
    private static List<S3Connection> s3Connections;
    //private static List<MemcachedConnection> memConnections;

    @Override
    public void init() throws DBException {
        System.out.println("DualClient.init()");

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

        s3Buckets = Arrays.asList(props.getProperty("s3.buckets").split("\\s*,\\s*"));
        List<String> s3Regions = Arrays.asList(props.getProperty("s3.regions").split("\\s*,\\s*"));
        List<String> s3EndPoints = Arrays.asList(props.getProperty("s3.endPoints").split("\\s*,\\s*"));

        int s3BucketsSize = s3Buckets.size();
        int s3RegionsSize = s3Regions.size();
        int s3EndPointsSize = s3EndPoints.size();
        if (s3BucketsSize != s3RegionsSize || s3RegionsSize != s3EndPointsSize || s3BucketsSize != s3EndPointsSize) {
            System.err.println("Num buckets should be equal to num regions and num endpoints. Check dual.properties");
            System.exit(1);
        }

        for (int i = 0; i < s3BucketsSize; i++) {
            S3Connection s3Connection = new S3Connection(s3Regions.get(i), s3EndPoints.get(i));
            s3Connections.add(s3Connection);
        }

        System.out.println("Quitting nicely...");
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        int bucketId = Mapper.mapKeyToBucket(key, s3Buckets.size());
        String bucket = s3Buckets.get(bucketId);
        System.out.println("DualClient.read(" + bucket + ", " + key + ")");
        return s3Connections.get(bucketId).read(bucket, key, fields, result);
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        int bucketId = Mapper.mapKeyToBucket(startkey, s3Buckets.size());
        String bucket = s3Buckets.get(bucketId);
        System.out.println("DualClient.scan(" + bucket + ", " + startkey + ")");
        return s3Connections.get(bucketId).scan(bucket, startkey, recordcount, fields, result);
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        int bucketId = Mapper.mapKeyToBucket(key, s3Buckets.size());
        String bucket = s3Buckets.get(bucketId);
        System.out.println("DualClient.update(" + bucket + ", " + key + ")");
        return s3Connections.get(bucketId).update(bucket, key, values);
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        int bucketId = Mapper.mapKeyToBucket(key, s3Buckets.size());
        String bucket = s3Buckets.get(bucketId);
        System.out.println("DualClient.insert(" + bucket + ", " + key + ")");
        return s3Connections.get(bucketId).insert(s3Buckets.get(bucketId), key, values);
    }

    @Override
    public Status delete(String table, String key) {
        int bucketId = Mapper.mapKeyToBucket(key, s3Buckets.size());
        String bucket = s3Buckets.get(bucketId);
        System.out.println("DualClient.delete(" + bucket + ", " + key + ")");
        return s3Connections.get(bucketId).delete(bucket, key);
    }
}
