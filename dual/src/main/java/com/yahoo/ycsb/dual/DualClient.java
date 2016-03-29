package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.CacheInfo;
import com.yahoo.ycsb.dual.utils.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class DualClient extends DB {
    /* logger */
    protected static Logger logger = Logger.getLogger(DualClient.class);

    /* config */
    private Properties properties;

    /* connection to proxy */
    private ProxyClient proxyConn;

    /* connections to AWS S3 buckets */
    private List<S3Client> s3Conns;

    /* connections to memcached servers */
    private List<MemClient> memClients;

    /* storage policy for the client */
    private StoragePolicy storagePolicy;

    /* concurrent processing */
    private ExecutorService executor;

    /**********
     * init functions
     **********/
    private void initProxyClient() {
        /* host + port */
        String[] pair = properties.getProperty(Constants.PROXY_HOST).split(":");
        String host = pair[0];
        int port = Integer.parseInt(pair[1]);

        /* new proxy connection */
        proxyConn = new ProxyClient(host, port);

        /* set socket timeout */
        int socketTimeout = Integer.valueOf(properties.getProperty(Constants.SOCKET_TIMEOUT, Constants.SOCKET_TIMEOUT_DEFAULT));
        proxyConn.setSocketTimeout(socketTimeout);

        /* set number of retries */
        int socketRetries = Integer.valueOf(properties.getProperty(Constants.SOCKET_RETRIES, Constants.SOCKET_RETRIES_DEFAULT));
        proxyConn.setSocketRetries(socketRetries);

        /* set packet size */
        int packetSize = Integer.valueOf(properties.getProperty(Constants.PACKET_SIZE, Constants.PACKET_SIZE_DEFAULT));
        proxyConn.setPacketSize(packetSize);
    }

    private void initS3() {
        /* TODO assumption: one bucket per region (num regions = num endpoints = num buckets) */
        List<String> regions = Arrays.asList(properties.getProperty(Constants.S3_REGIONS).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(properties.getProperty(Constants.S3_ENDPOINTS).split("\\s*,\\s*"));
        List<String> s3Buckets = Arrays.asList(properties.getProperty(Constants.S3_BUCKETS).split("\\s*,\\s*"));
        if (s3Buckets.size() != regions.size() ||
            s3Buckets.size() != endpoints.size() ||
            regions.size() != endpoints.size())
            logger.error("Configuration error: #buckets must match #regions and #endpoints");

        /* establish S3 connections */
        s3Conns = new ArrayList<S3Client>();
        for (int i = 0; i < s3Buckets.size(); i++) {
            logger.debug("S3 connection " + i + " " + s3Buckets.get(i) + " " + regions.get(i) + " " + endpoints.get(i));
            S3Client client = null;
            try {
                client = new S3Client(s3Buckets.get(i), regions.get(i), endpoints.get(i));
            } catch (DBException e) {
                logger.error("Error connecting to " + s3Buckets.get(i));
            }
            s3Conns.add(client);
        }
    }

    // TODO assumption: Memcached connections are established in advance, in order to avoid this cost of doing it on-the-fly (i.e., when proxy sends memcached host)
    private void initMemcached() throws DBException {
        List<String> memcachedHosts = Arrays.asList(properties.getProperty(Constants.MEMCACHED_HOSTS).split("\\s*,\\s*"));
        memClients = new ArrayList<MemClient>();
        for (String memcachedHost : memcachedHosts) {
            logger.debug("Memcached connection " + memcachedHost);
            MemClient client = new MemClient(memcachedHost);
            memClients.add(client);
        }
    }

    private void initLonghair() {
        LonghairLib.k = Integer.valueOf(properties.getProperty(Constants.LONGHAIR_K, Constants.LONGHAIR_K_DEFAULT));
        LonghairLib.m = Integer.valueOf(properties.getProperty(Constants.LONGHAIR_M, Constants.LONGHAIR_M_DEFAULT));
        logger.debug("k: " + LonghairLib.k + " m: " + LonghairLib.m);

        /* check k >= 0 and k < 256 */
        if (LonghairLib.k < 0 || LonghairLib.k >= 256) {
            logger.error("Invalid Longhair.k: k should be >= 0 and < 256.");
        }
        /* check m >=0 and m <= 256 - k */
        if (LonghairLib.m < 0 || LonghairLib.m > 256 - LonghairLib.k) {
            logger.error("Invalid Longhair.m: m should be >= 0 and <= 256 - k.");
        }
        /* init longhair */
        if (LonghairLib.Longhair.INSTANCE._cauchy_256_init(2) != 0) {
            logger.error("Error initializing longhair");
        }
    }

    @Override
    public void init() throws DBException {
        logger.debug("DualClient.init() start");

        /* properties */
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        properties = new Properties();
        InputStream in = loader.getResourceAsStream(Constants.PROPERTIES_FILE);
        try {
            properties.load(in);
        } catch (IOException e) {
            logger.error("Error loading properties.");
        }

        /* set up connection to nearby proxy */
        initProxyClient();

        /* set up connections to AWS S3 */
        initS3();

        /* set up connections to Memcached servers */
        initMemcached();

        /* set up storage policy */
        boolean s3Encode = Boolean.valueOf(properties.getProperty(Constants.S3_ENCODE, Constants.S3_ENCODE_DEFAULT));
        logger.debug("s3Encode: " + s3Encode);
        storagePolicy = new StoragePolicy(s3Conns, memClients, s3Encode);

        /* set up Longhair erasure coding library and storage policy */
        if (s3Encode == true) {
            initLonghair();
        }

        /* init executor service */
        final int threadsNum = Integer.valueOf(properties.getProperty(Constants.THREADS_NUM, Constants.THREADS_NUM_DEFAULT));
        logger.debug("threads num: " + threadsNum);
        executor = Executors.newFixedThreadPool(threadsNum);

        logger.debug("DualClient.init() end");
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
    /*private EncodedBlock readBlock(String key, CacheInfo cacheInfo) {
        EncodedBlock encodedBlock = new EncodedBlock(key);
        MemClient memClient = getMemClientByHost(cacheInfo.getCacheAddress());

        // if block is in cache, contact memcached host and download it
        if (cacheInfo.isCached()) {
            // get client by host str
            byte[] bytes = memClient.read(key);
            encodedBlock.setBytes(bytes);
        }
        // otherwise, get the block from the backend and cache it
        else {
            // get block from s3
            int clientId = storagePolicy.assignToS3(key, encodedBlock.getId());
            S3Client s3Client = s3Conns.get(clientId);
            byte[] bytes = s3Client.read(key);
            encodedBlock.setBytes(bytes);

            // cache block
            Status cacheStatus = memClient.insert(key, bytes);
            if (cacheStatus != Status.OK)
                logger.trace("Error caching block " + key);
        }
        return encodedBlock;
    }*/

    /*private void processGetResponse(ProxyGetResponse getResponseMsg) {
        logger.debug(getResponseMsg.prettyPrint());

        if (getResponseMsg.getKeyToCacheInfoPairs().size() == 1) {
            // the cache stores full data
        } else {
            // there are blocks stored in the cache
            // but are there enough?
            // if enough blocks in the cache => try to fetch
            // if not enough blocks in the cache => fetch remaining from the backend
            // if not enough blocks in the cache and full data stored in the backend, then fetch full data and cache it!
        }
    }*/

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        /* operation status to be returned */
        Status status = Status.UNEXPECTED_STATE;

        /* send GET to proxy and wait for GET_RESPONSE */
        Map<String, CacheInfo> keyToMem = proxyConn.sendGET(key);

        /* ask storage policy where to get the data from */
        //storagePolicy.assign(keyToMemHosts);

        /* 2. use GET_RESPONSE to fetch data from cache / backend and compute PUT msg */

        /* 3. send PUT to update proxy data base and decode data if necessary */

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

    // TODO inserting encoded blocks could be done in parallel
    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        // operation status
        Status status = Status.OK;

        // generate bytes array based on values
        byte[] bytes = ClientUtils.valuesToBytes(values);

        List<S3Client> assignedS3Conns = storagePolicy.assignToS3(key);

        /* store full data item */
        if (assignedS3Conns.size() == 1) {
            status = assignedS3Conns.get(0).insert(key, bytes);
        }
        /* need to encode, then store blocks */
        else {
            List<byte[]> encodedBlocks = LonghairLib.encode(bytes);
            for (int i = 0; i < assignedS3Conns.size(); i++) {
                String blockKey = key + i;
                S3Client s3Conn = assignedS3Conns.get(i);
                byte[] block = encodedBlocks.get(i);
                Status blockStatus = s3Conn.insert(blockKey, block);
                logger.debug("Block " + blockKey + " " + blockStatus.getName() + " " + blockStatus);
                if (blockStatus != Status.OK) {
                    logger.warn("Error inserting encoded block " + blockKey);
                    status = Status.ERROR;
                    break;
                }
            }
        }

        logger.debug("Data Item " + key + " " + status.getName());
        return status;
    }

    @Override
    public Status delete(String table, String key) {
        return null;
    }
}
