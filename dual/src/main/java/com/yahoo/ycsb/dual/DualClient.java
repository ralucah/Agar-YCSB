package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.dual.utils.LonghairLib;
import com.yahoo.ycsb.dual.utils.Proxy;
import com.yahoo.ycsb.dual.utils.StoragePolicy;
import com.yahoo.ycsb.dual.utils.Utils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.*;

public class DualClient extends DB {
    public static Logger logger = Logger.getLogger(DualClient.class);

    public static String PROPERTIES_FILE = "client.properties";
    public static String PROXY_ADDRESS = "proxy.address";
    public static String S3_REGIONS = "s3.regions";
    public static String S3_ENDPOINTS = "s3.endpoints";
    public static String S3_BUCKETS = "s3.buckets";
    public static String S3_ENCODE = "s3.encode";
    public static String LONGHAIR_K = "longhair.k";
    public static String LONGHAIR_M = "longhair.m";
    public static String PACKET_SIZE = "packet.size";

    private Properties properties;

    /* connection to proxy */
    private DatagramSocket socket;
    private Proxy proxy;
    private int packetSize;

    /* connections to AWS S3 buckets */
    private List<S3Client> s3Connections;
    private List<String> s3Buckets;
    private boolean s3Encode = false;

    /* storage policy valid for backend */
    private StoragePolicy storagePolicy;

    private void initProxy() {
        String[] pair = properties.getProperty(PROXY_ADDRESS).split(":");
        proxy = new Proxy(pair[0], Integer.parseInt(pair[1]));
        logger.trace("Proxy " + proxy.getIp() + " " + proxy.getPort());

        /* datagram socket */
        try {
            socket = new DatagramSocket();
        } catch (SocketException e) {
            logger.error("Error creating datagram socket.");
        }

        /* packet size */
        packetSize = Integer.valueOf(properties.getProperty(PACKET_SIZE));
        logger.trace("packet size: " + packetSize);
    }

    private void initS3() throws DBException {
        List<String> regions = Arrays.asList(properties.getProperty(S3_REGIONS).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(properties.getProperty(S3_ENDPOINTS).split("\\s*,\\s*"));
        s3Buckets = Arrays.asList(properties.getProperty(S3_BUCKETS).split("\\s*,\\s*"));
        if (s3Buckets.size() != regions.size() || s3Buckets.size() != endpoints.size() || regions.size() != endpoints.size())
            logger.error("Configuration error: #buckets must match #regions and #endpoints");

        s3Connections = new ArrayList<S3Client>();
        for (int i = 0; i < s3Buckets.size(); i++) {
            logger.trace("Client" + i + " " + regions.get(i) + " " + endpoints.get(i));
            S3Client client = new S3Client(regions.get(i), endpoints.get(i));
            s3Connections.add(client);
        }

        s3Encode = Boolean.valueOf(properties.getProperty(S3_ENCODE));
        logger.trace("s3Encode: " + s3Encode);
    }

    private void initLonghair() {
        LonghairLib.k = Integer.valueOf(properties.getProperty(LONGHAIR_K));
        LonghairLib.m = Integer.valueOf(properties.getProperty(LONGHAIR_M));
        logger.trace("k: " + LonghairLib.k + " m: " + LonghairLib.m);
        /* check k >= 0 and k < 256 */
        if (LonghairLib.k < 0 || LonghairLib.k >= 256) {
            logger.error("Invalid longhair.k: k should be >= 0 and < 256.");
        }
        /* check m >=0 and m <= 256 - k */
        if (LonghairLib.m < 0 || LonghairLib.m > 256 - LonghairLib.k) {
            logger.error("Invalid longhair.m: m should be >= 0 and <= 256 - k.");
        }
        /* init longhair */
        if (LonghairLib.Longhair.INSTANCE._cauchy_256_init(2) != 0) {
            logger.error("Error initializing longhair");
        }
    }

    @Override
    public void init() throws DBException {
        logger.trace("DualClient.init() start");

        /* properties */
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        properties = new Properties();
        InputStream in = loader.getResourceAsStream(PROPERTIES_FILE);
        try {
            properties.load(in);
        } catch (IOException e) {
            logger.error("Error loading properties from " + PROPERTIES_FILE);
        }

        /* set up connections to proxies */
        initProxy();

        /* set up connections to AWS S3 */
        initS3();

        /* set up Longhair erasure coding library and storage policy */
        if (s3Encode == true) {
            initLonghair();
            storagePolicy = new StoragePolicy(s3Connections.size(), LonghairLib.k + LonghairLib.m);
        } else {
            storagePolicy = new StoragePolicy(s3Connections.size());
        }

        logger.trace("Dualclient.init() end");
        //System.exit(1);
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        // contact proxy to ask about cache status
        if (s3Encode == true) {
            // compute block keys
            List<String> blockKeys = Utils.computeBlockKeys(key, LonghairLib.k + LonghairLib.m);

            // transform block keys list to byte array
            byte[] sendData = Utils.listToBytes(blockKeys);

            // ask proxy about the status of the blocks
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, proxy.getIp(), proxy.getPort());
            try {
                socket.send(sendPacket);
            } catch (IOException e) {
                logger.error("Error sending packet to Proxy.");
            }

            // get answer from proxy
            byte[] receiveData = new byte[packetSize];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, packetSize);
            try {
                socket.receive(receivePacket);
            } catch (IOException e) {
                logger.error("Error receiving packet from Proxy.");
            }
            List<String> receivedList = Utils.bytesToList(receivePacket.getData());
            for (String recv : receivedList) {
                logger.trace(recv);
            }
        }
        return null;
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        return null;
    }

    @Override
    public Status update(String table, String key, HashMap<String, ByteIterator> values) {
        return null;
    }

    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        // operation status
        Status status = Status.OK;
        // generate bytes array based on values
        byte[] bytes = Utils.valuesToBytes(values);

        if (s3Encode == true) {
            // encode data
            List<byte[]> encodedBlocks = LonghairLib.encode(bytes);
            // insert encoded blocks
            int id = 0;
            for (byte[] blockBytes : encodedBlocks) {
                String blockKey = key + id;
                int connId = storagePolicy.assignEncodedBlockToRegion(blockKey, id);
                String bucket = s3Buckets.get(connId);
                Status blockStatus = s3Connections.get(connId).insert(bucket, blockKey, blockBytes);
                logger.trace("Block " + blockKey + " " + status.getName());
                if (blockStatus != Status.OK) {
                    //logger.warn("Error inserting encoded block " + blockKey);
                    status = Status.ERROR;
                }
                id++;
            }
        } else {
            // insert full data
            int connId = storagePolicy.assignFullDataToRegion(key);
            String bucket = s3Buckets.get(connId);
            status = s3Connections.get(connId).insert(bucket, key, bytes);
        }

        logger.debug("Item " + key + " " + status.getName());
        return status;
    }

    @Override
    public Status delete(String table, String key) {
        return null;
    }
}
