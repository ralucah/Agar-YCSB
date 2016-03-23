package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.*;
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

    /* property names */
    public static String PROPERTIES_FILE = "client.properties";
    public static String PROXY_HOST = "proxy.host";
    public static String S3_REGIONS = "s3.regions";
    public static String S3_ENDPOINTS = "s3.endpoints";
    public static String S3_BUCKETS = "s3.buckets";
    public static String S3_ENCODE = "s3.encode";
    public static String MEMCACHED_HOSTS = "memcached.hosts";
    public static String LONGHAIR_K = "longhair.k";
    public static String LONGHAIR_M = "longhair.m";
    public static String THREADS_NUM = "executor.threads";
    public static String PACKET_SIZE = "socket.packet_size";
    public static String SOCKET_TIMEOUT = "socket.timeout"; // in ms
    public static String SOCKET_RETRIES = "socket.retries";

    public static String S3_ENCODE_DEFAULT = "false";
    public static String LONGHAIR_K_DEFAULT = "3";
    public static String LONGHAIR_M_DEFAULT = "2";
    public static String THREADS_NUM_DEFAULT = "5";
    public static String PACKET_SIZE_DEFAULT = "1024";
    public static String SOCKET_TIMEOUT_DEFAULT = "1000";
    public static String SOCKET_RETRIES_DEFAULT = "3";

    private Properties properties;

    /* connection to proxy */
    private DatagramSocket socket;
    private Proxy proxy;
    private int packetSize;
    private int socketRetries;

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

        final int socketTimeout = Integer.valueOf(properties.getProperty(SOCKET_TIMEOUT, SOCKET_TIMEOUT_DEFAULT));
        socketRetries = Integer.valueOf(properties.getProperty(SOCKET_RETRIES, SOCKET_RETRIES_DEFAULT));
        /* datagram socket */
        try {
            socket = new DatagramSocket();
            socket.setSoTimeout(socketTimeout);
        } catch (SocketException e) {
            logger.error("Error creating datagram socket.");
        }

        /* packet size */
        packetSize = Integer.valueOf(properties.getProperty(PACKET_SIZE, PACKET_SIZE_DEFAULT));
        logger.trace("packet size: " + packetSize);
    }

    private void initS3() {
        List<String> regions = Arrays.asList(properties.getProperty(S3_REGIONS).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(properties.getProperty(S3_ENDPOINTS).split("\\s*,\\s*"));
        List<String> s3Buckets = Arrays.asList(properties.getProperty(S3_BUCKETS).split("\\s*,\\s*"));
        if (s3Buckets.size() != regions.size() ||
            s3Buckets.size() != endpoints.size() ||
            regions.size() != endpoints.size())
            logger.error("Configuration error: #buckets must match #regions and #endpoints");

        s3Clients = new ArrayList<S3Client>();
        for (int i = 0; i < s3Buckets.size(); i++) {
            S3Client client = null;
            try {
                client = new S3Client(s3Buckets.get(i), regions.get(i), endpoints.get(i));
            } catch (DBException e) {
                logger.error("Error connecting to " + s3Buckets.get(i));
            }
            s3Clients.add(client);
            logger.trace("Client" + i + " " + s3Buckets.get(i) + " " + regions.get(i) + " " + endpoints.get(i));
        }

        s3Encode = Boolean.valueOf(properties.getProperty(S3_ENCODE, S3_ENCODE_DEFAULT));
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
        LonghairLib.k = Integer.valueOf(properties.getProperty(LONGHAIR_K, LONGHAIR_K_DEFAULT));
        LonghairLib.m = Integer.valueOf(properties.getProperty(LONGHAIR_M, LONGHAIR_M_DEFAULT));
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
        final int threadsNum = Integer.valueOf(properties.getProperty(THREADS_NUM, THREADS_NUM_DEFAULT));
        logger.trace("threads num: " + threadsNum);
        executor = Executors.newFixedThreadPool(threadsNum);

        logger.trace("Dualclient.init() end");
        //System.exit(1);
    }

    @Override
    public void cleanup() throws DBException {
        logger.trace("Cleaning up.");
        executor.shutdown();
    }

    private MemClient getMemClientByHost(String host) {
        for (MemClient client : memClients) {
            if (client.getHost().equals(host))
                return client;
        }
        return null;
    }

    // when this method returns a valid block, the block is assumed to have been cached
    private EncodedBlock readBlock(String key, CacheInfo cacheInfo) {
        EncodedBlock encodedBlock = new EncodedBlock(key);
        MemClient memClient = getMemClientByHost(cacheInfo.getCacheServer());

        // if block is in cache, contact memcached host and download it
        if (cacheInfo.isCached()) {
            // get client by host str
            byte[] bytes = memClient.read(key);
            encodedBlock.setBytes(bytes);
        }
        // otherwise, get the block from the backend and cache it
        else {
            // get block from s3
            int clientId = storagePolicy.assignBlockToS3Client(key, encodedBlock.getId());
            S3Client s3Client = s3Clients.get(clientId);
            byte[] bytes = s3Client.read(key);
            encodedBlock.setBytes(bytes);

            // cache block
            Status cacheStatus = memClient.insert(key, bytes);
            if (cacheStatus != Status.OK)
                logger.trace("Error caching block " + key);
        }
        return encodedBlock;
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        Status status = Status.ERROR;
        if (s3Encode == true) {
            /* contact proxy to ask about cache status */
            List<String> blockKeys = Utils.computeBlockKeys(key, LonghairLib.k + LonghairLib.m);
            ProxyGet proxyGet = new ProxyGet(blockKeys);
            logger.debug(proxyGet.print());
            byte[] sendData = CommonUtils.serializeProxyMsg(proxyGet);
            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, proxy.getIp(), proxy.getPort());

            /* send GET with retries */
            // continue trying to send get?
            boolean retryGet = true;
            // attempt number
            int retryCounter = 0;
            // data to be received from proxy
            byte[] receiveData = new byte[packetSize];
            DatagramPacket receivePacket = new DatagramPacket(receiveData, packetSize);
            while (retryGet == true && retryCounter < socketRetries) {
                retryCounter++;
                try {
                    socket.send(sendPacket);
                } catch (IOException e) {
                    logger.error("Error sending packet to Proxy. Attempt #" + retryCounter);
                }
                try {
                    socket.receive(receivePacket);
                    retryGet = false;
                } catch (IOException e) {
                    logger.error("Error receiving packet from Proxy. Attempt #" + retryCounter);
                }
            }

            if (retryGet == true) {
                logger.error("Read failed for " + key);
                return Status.ERROR;
            }
            ;

            final ProxyGetResponse getResponse = (ProxyGetResponse) CommonUtils.deserializeProxyMsg(receivePacket.getData());
            logger.debug(getResponse.print());

            List<Future> futures = new ArrayList<Future>();
            CompletionService<EncodedBlock> completionService = new ExecutorCompletionService<EncodedBlock>(executor);
            Iterator it = getResponse.getKeyToCacheInfoPairs().entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry pair = (Map.Entry) it.next();
                final String blockKey = (String) pair.getKey();
                final CacheInfo cacheInfo = (CacheInfo) pair.getValue();
                completionService.submit(new Callable<EncodedBlock>() {
                    @Override
                    public EncodedBlock call() throws Exception {
                        return readBlock(blockKey, cacheInfo);
                    }
                });
            }

            // wait for first k blocks, cancel the others
            List<EncodedBlock> results = new ArrayList<EncodedBlock>();
            int errors = 0;
            while (results.size() < LonghairLib.k) {
                Future<EncodedBlock> resultFuture = null;
                try {
                    resultFuture = completionService.take();
                    EncodedBlock res = resultFuture.get();
                    if (res.getBytes() != null) {
                        if (!results.contains(res))
                            results.add(res);
                    } else
                        errors++;
                } catch (Exception e) {
                    errors++;
                    logger.error("Error reading a block.");
                }
                if (errors > LonghairLib.m)
                    break;
            }
            logger.debug("Retrieved " + results.size() + " blocks for " + key);
            // shut down all execution threads
            //executor.shutdownNow();

            if (results.size() > 0) {
                // compute put msg to inform proxy about caching
                final List<EncodedBlock> resultsFin = results;
                executor.execute(new Runnable() {
                    public void run() {
                        // compute put proxyGet
                        // TODO should I do a diff? or is it ok to send everything to the proxy to update
                        ProxyPut proxyPut = new ProxyPut();
                        Map<String, CacheInfo> keyToCacheInfo = getResponse.getKeyToCacheInfoPairs();
                        for (EncodedBlock block : resultsFin) {
                            String blockKey = block.getKey();
                            //logger.trace(blockKey);
                            CacheInfo keyInfo = keyToCacheInfo.get(blockKey);
                            if (keyInfo.isCached() == false) {
                                proxyPut.addKeyToHostPair(blockKey, keyInfo.getCacheServer());
                            }
                        }

                        // send it to proxy
                        if (proxyPut.getKeyToHostPairs().size() > 0) {
                            logger.debug(proxyPut.print());
                            byte[] sendData = CommonUtils.serializeProxyMsg(proxyPut);
                            DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, proxy.getIp(), proxy.getPort());
                            try {
                                socket.send(sendPacket);
                            } catch (IOException e) {
                                logger.error("Error sending packet to Proxy.");
                            }
                        } //else
                        //logger.debug("Nothing to update in the proxy DB");
                    }
                });

                // decode data
                byte[] bytes = LonghairLib.decode(Utils.blocksToBytes(results));
                if (bytes != null)
                    status = Status.OK;
            }
        }

        /*try {
            System.out.println(executor.isTerminated());
            executor.awaitTermination(10, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }*/
        return status;
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
                logger.trace("Block " + blockKey + " " + blockStatus.getName());
                if (blockStatus != Status.OK) {
                    //logger.warn("Error inserting encoded block " + blockKey);
                    status = Status.ERROR;
                    break;
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
