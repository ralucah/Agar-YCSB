package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.CacheInfo;
import com.yahoo.ycsb.common.CommonUtils;
import com.yahoo.ycsb.common.ProxyGet;
import com.yahoo.ycsb.common.ProxyGetResponse;
import com.yahoo.ycsb.dual.utils.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.*;
import java.util.concurrent.*;

public class DualClient extends DB {
    //TODO for init and cleanup
    //private static final AtomicInteger INIT_COUNT = new AtomicInteger(0);

    public static Logger logger = Logger.getLogger(DualClient.class);

    public static String PROPERTIES_FILE = "client.properties";
    public static String PROXY_HOST = "proxy.host";
    public static String S3_REGIONS = "s3.regions";
    public static String S3_ENDPOINTS = "s3.endpoints";
    public static String S3_BUCKETS = "s3.buckets";
    public static String S3_ENCODE = "s3.encode";
    public static String MEMCACHED_HOSTS = "memcached.hosts";
    public static String LONGHAIR_K = "longhair.k";
    public static String LONGHAIR_M = "longhair.m";
    public static String PACKET_SIZE = "packet.size";
    public static String THREADS_NUM = "executor.threads";

    private Properties properties;

    /* connection to proxy */
    private DatagramSocket socket;
    private Proxy proxy;
    private int packetSize;

    /* connections to AWS S3 buckets */
    // TODO there should be a mapping between s3buckets and s3Connections
    private List<S3Client> s3Clients;
    private boolean s3Encode = false;

    /* connections to memcached servers */
    private List<MemClient> memClients;

    /* storage policy valid for backend */
    private BlockStoragePolicy storagePolicy;

    /* concurrent processing */
    private ExecutorService executor;

    private void initProxy() {
        String[] pair = properties.getProperty(PROXY_HOST).split(":");
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
        List<String> s3Buckets = Arrays.asList(properties.getProperty(S3_BUCKETS).split("\\s*,\\s*"));
        if (s3Buckets.size() != regions.size() || s3Buckets.size() != endpoints.size() || regions.size() != endpoints.size())
            logger.error("Configuration error: #buckets must match #regions and #endpoints");

        s3Clients = new ArrayList<S3Client>();
        for (int i = 0; i < s3Buckets.size(); i++) {
            logger.trace("Client" + i + " " + s3Buckets.get(i) + " " + regions.get(i) + " " + endpoints.get(i));
            S3Client client = new S3Client(s3Buckets.get(i), regions.get(i), endpoints.get(i));
            s3Clients.add(client);
        }

        s3Encode = Boolean.valueOf(properties.getProperty(S3_ENCODE));
        logger.trace("s3Encode: " + s3Encode);
    }

    private void initMemcached() throws DBException {
        List<String> memcachedHosts = Arrays.asList(properties.getProperty(MEMCACHED_HOSTS).split("\\s*,\\s*"));
        logger.trace("Memcached hosts: " + memcachedHosts);
        memClients = new ArrayList<MemClient>();
        for (String memcachedHost : memcachedHosts) {
            logger.trace("New MemcachedClient " + memcachedHost);
            MemClient client = new MemClient(memcachedHost);
            memClients.add(client);
        }
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
            storagePolicy = new BlockStoragePolicy(s3Clients.size(), LonghairLib.k + LonghairLib.m);
        }

        /* set up connections to Memcached servers */
        initMemcached();

        /* init executor service */
        final int threadsNum = Integer.valueOf(properties.getProperty(THREADS_NUM));
        logger.trace("threads num: " + threadsNum);
        executor = Executors.newFixedThreadPool(threadsNum);

        logger.trace("Dualclient.init() end");
        //System.exit(1);
    }

    private MemClient getMemClientByHost(String host) {
        for (MemClient client : memClients) {
            if (client.getHost().equals(host))
                return client;
        }
        return null;
    }

    private EncodedBlock readBlock(String table, String key, CacheInfo cacheInfo) {
        EncodedBlock encodedBlock = new EncodedBlock(key);

        // if block is in cache, contact memcached host and download it
        if (cacheInfo.isCached()) {
            // get client by host str
            MemClient memClient = getMemClientByHost(cacheInfo.getCacheServer());
            byte[] bytes = memClient.read(key);
            encodedBlock.setBytes(bytes);
        }
        // otherwise, get the block from the backend
        // TODO cache it + inform proxy
        else {
            // get block from S3
            int clientId = storagePolicy.assignBlockToS3Client(key, encodedBlock.getId());
            S3Client s3Client = s3Clients.get(clientId);
            byte[] bytes = s3Client.read(key);
            encodedBlock.setBytes(bytes);
        }

        return encodedBlock;
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        if (s3Encode == true) {
            /* contact proxy to ask about cache status */
            // proxy msg
            List<String> blockKeys = Utils.computeBlockKeys(key, LonghairLib.k + LonghairLib.m);
            ProxyGet query = new ProxyGet(blockKeys);

            // transform query into byte array
            byte[] sendData = CommonUtils.serializeProxyMsg(query);

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
            ProxyGetResponse response = (ProxyGetResponse) CommonUtils.deserializeProxyMsg(receivePacket.getData());
            logger.debug(CommonUtils.mapToStr(response.getKeyToCacheInfoPairs()));
            /* do stuff with the info from proxy
            * -
            * -
            * */

            List<Future> futures = new ArrayList<Future>();
            CompletionService<EncodedBlock> completionService = new ExecutorCompletionService<EncodedBlock>(executor);
            Iterator it = response.getKeyToCacheInfoPairs().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                String blockKey = (String) pair.getKey();
                CacheInfo cacheInfo = (CacheInfo) pair.getValue();
                completionService.submit(new Callable<EncodedBlock>() {
                    @Override
                    public EncodedBlock call() throws Exception {
                        return readBlock(table, blockKey, cacheInfo);
                    }
                });
            }

            // only k blocks are needed to reconstruct the data, but take care of all k+m blocks in the background
            List<EncodedBlock> results = new ArrayList<EncodedBlock>();
            int errors = 0;
            while (results.size() < LonghairLib.k) {
                Future<EncodedBlock> resultFuture = null;
                try {
                    resultFuture = completionService.take();
                    EncodedBlock res = resultFuture.get();
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
        }
        System.exit(1);
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
                int connId = storagePolicy.assignBlockToS3Client(key, id);
                Status blockStatus = s3Clients.get(connId).insert(blockKey, blockBytes);
                logger.trace("Block " + blockKey + " " + status.getName());
                if (blockStatus != Status.OK) {
                    //logger.warn("Error inserting encoded block " + blockKey);
                    status = Status.ERROR;
                }
                id++;
            }
        }

        logger.debug("Item " + key + " " + status.getName());
        return status;
    }

    @Override
    public Status delete(String table, String key) {
        return null;
    }
}
