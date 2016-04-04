package com.yahoo.ycsb.dual;

// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload -load
// -db com.yahoo.ycsb.dual.DualClient -p fieldlength=10 -p fieldcount=20 -s -P workloads/myworkload

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.dual.policy.*;
import com.yahoo.ycsb.dual.utils.ClientConstants;
import com.yahoo.ycsb.dual.utils.ClientUtils;
import com.yahoo.ycsb.dual.utils.LonghairLib;
import com.yahoo.ycsb.dual.utils.UDPClient;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;

public class DualClient extends DB {
    protected static Logger logger = Logger.getLogger(DualClient.class);

    private Properties properties;

    /* connection to proxy */
    private UDPClient proxyConnection;

    /* S3 bucket names mapped to connections to AWS S3 buckets */
    private Map<String, S3Connection> s3Connections;

    /* mapping memcached hosts to connections to memcached servers */
    private Map<String, MemcachedConnection> memConnections;

    /* backend storage policy */
    private StorageOracle storageOracle;

    /* concurrent processing */
    private ExecutorService executor;

    private void initProxyConnection() {
        /* host address + port */
        String[] pair = properties.getProperty(ClientConstants.PROXY_HOST).split(":");
        String host = pair[0];
        int port = Integer.parseInt(pair[1]);

        /* new proxy connection */
        proxyConnection = new UDPClient(host, port);

        /* set socket timeout */
        int socketTimeout = Integer.valueOf(properties.getProperty(ClientConstants.SOCKET_TIMEOUT, ClientConstants.SOCKET_TIMEOUT_DEFAULT));
        proxyConnection.setSocketTimeout(socketTimeout);

        /* set number of retries */
        int socketRetries = Integer.valueOf(properties.getProperty(ClientConstants.SOCKET_RETRIES, ClientConstants.SOCKET_RETRIES_DEFAULT));
        proxyConnection.setSocketRetries(socketRetries);

        /* set packet size */
        int packetSize = Integer.valueOf(properties.getProperty(ClientConstants.PACKET_SIZE, ClientConstants.PACKET_SIZE_DEFAULT));
        proxyConnection.setPacketSize(packetSize);
    }

    /* returns list of s3 buckets, to pass them to storage oracle */
    /* TODO assumption: one bucket per region (num regions = num endpoints = num buckets) */
    private List<String> initS3() {
        List<String> regions = Arrays.asList(properties.getProperty(ClientConstants.S3_REGIONS).split("\\s*,\\s*"));
        List<String> endpoints = Arrays.asList(properties.getProperty(ClientConstants.S3_ENDPOINTS).split("\\s*,\\s*"));
        List<String> s3Buckets = Arrays.asList(properties.getProperty(ClientConstants.S3_BUCKETS).split("\\s*,\\s*"));
        if (s3Buckets.size() != regions.size() ||
            s3Buckets.size() != endpoints.size() ||
            regions.size() != endpoints.size())
            logger.error("Configuration error: #buckets must match #regions and #endpoints");

        /* establish S3 connections */
        s3Connections = new HashMap<String, S3Connection>();
        for (int i = 0; i < s3Buckets.size(); i++) {
            logger.debug("S3 connection " + i + " " + s3Buckets.get(i) + " " + regions.get(i) + " " + endpoints.get(i));
            S3Connection client = null;
            try {
                client = new S3Connection(s3Buckets.get(i), regions.get(i), endpoints.get(i));
            } catch (DBException e) {
                logger.error("Error connecting to " + s3Buckets.get(i));
            }
            s3Connections.put(s3Buckets.get(i), client);
        }
        return s3Buckets;
    }

    /* returns list of memcached hosts, to pass them to the storage oracle */
    // TODO assumption: Memcached connections are established in advance, in order to avoid this cost of doing it on-the-fly (i.e., when proxy sends memcached host)
    private List<String> initMemcached() throws DBException {
        List<String> memcachedHosts = Arrays.asList(properties.getProperty(ClientConstants.MEMCACHED_HOSTS).split("\\s*,\\s*"));
        memConnections = new HashMap<String, MemcachedConnection>();
        for (String memcachedHost : memcachedHosts) {
            logger.debug("Memcached connection " + memcachedHost);
            MemcachedConnection client = new MemcachedConnection(memcachedHost);
            memConnections.put(memcachedHost, client);
        }
        return memcachedHosts;
    }

    private void initLonghair() {
        LonghairLib.k = Integer.valueOf(properties.getProperty(ClientConstants.LONGHAIR_K, ClientConstants.LONGHAIR_K_DEFAULT));
        LonghairLib.m = Integer.valueOf(properties.getProperty(ClientConstants.LONGHAIR_M, ClientConstants.LONGHAIR_M_DEFAULT));
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
        InputStream in = loader.getResourceAsStream(ClientConstants.PROPERTIES_FILE);
        try {
            properties.load(in);
        } catch (IOException e) {
            logger.error("Error loading properties.");
        }

        /* set up connection to nearby proxy */
        initProxyConnection();

        /* set up connections to AWS S3 */
        List<String> s3Buckets = initS3();

        /* set up connections to Memcached servers */
        List<String> memcachedHosts = initMemcached();

        /* set up storage policy */
        boolean s3Encode = Boolean.valueOf(properties.getProperty(ClientConstants.S3_ENCODE, ClientConstants.S3_ENCODE_DEFAULT));
        logger.debug("s3Encode: " + s3Encode);
        storageOracle = new StorageOracle(s3Buckets, memcachedHosts, s3Encode);

        /* set up Longhair erasure coding library and storage policy */
        if (s3Encode == true) {
            initLonghair();
        }

        /* init executor service */
        final int threadsNum = Integer.valueOf(properties.getProperty(ClientConstants.THREADS_NUM, ClientConstants.THREADS_NUM_DEFAULT));
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

    /* read a block of bytes (used for full data too) */
    private byte[] readBytes(StorageSubitem subItem) {
        String key = subItem.getKey();
        String host = subItem.getHost();
        StorageLayer storageLayer = subItem.getLayer();

        byte[] data = null;
        switch (storageLayer) {
            case CACHE:
                MemcachedConnection memConn = memConnections.get(host);
                data = memConn.read(key);
                break;
            case BACKEND:
                S3Connection s3Conn = s3Connections.get(host);
                data = s3Conn.read(key);
                break;
            default:
                logger.error("Unknown read storage layer!");
                break;
        }
        return data;
    }

    /* read an encoded block of data */
    private ReadResult readBlock(StorageSubitem subItem) {
        ReadResult result = new ReadResult(subItem.getKey());
        byte[] data = readBytes(subItem);
        result.setBytes(data);
        return result;
    }

    /* read blocks in parallel */
    private void readBlocks(Set<StorageSubitem> strategy, List<ReadResult> readResults) {
        List<Future> futures = new ArrayList<Future>();
        CompletionService<ReadResult> completionService = new ExecutorCompletionService<ReadResult>(executor);

        for (final StorageSubitem subItem : strategy) {
            completionService.submit(new Callable<ReadResult>() {
                @Override
                public ReadResult call() throws Exception {
                    return readBlock(subItem);
                }
            });
        }

        //int errors = 0;
        int resultsNum = 0;
        while (readResults.size() < LonghairLib.k) {
            Future<ReadResult> resultFuture = null;
            try {
                resultFuture = completionService.take();
                ReadResult res = resultFuture.get();
                resultsNum++;
                /*if (res.getBytes() == null)
                    errors++;*/
                if (res.getBytes() != null &&
                    ClientUtils.readResultsContains(readResults, res.getKey()) == false)
                        readResults.add(res);
            } catch (Exception e) {
                //errors++;
                resultsNum++;
                logger.trace("Exception reading a block.");
            }
            //if (errors > LonghairLib.m)
            //    break;
            if (resultsNum == strategy.size())
                break;
        }
    }

    private void cacheBlocks(Map<String, String> toCache, List<ReadResult> readResults) {
        logger.debug("toCache: " + toCache.keySet());
        logger.debug("readResults: " + readResults.size());
        for (ReadResult result : readResults) {
            String key = result.getKey();
            if (toCache.containsKey(key) == true)
                cacheData(key, result.getBytes(), toCache.get(key));
        }
    }

    private byte[] readByStrategy(StorageItem storageItem) {
        boolean success = true;
        byte[] data = null;

        // read according to strategy
        List<ReadResult> readResults = Collections.synchronizedList(new ArrayList<>());

        //new ArrayList<ReadResult>();
        Set<StorageSubitem> strategy = storageItem.getStrategy();
        Map<String, String> toCache = new HashMap<String, String>();
        // read full data
        if (storageItem.isEncodedInStrategy() == false) {
            StorageSubitem first = strategy.iterator().next();
            data = readBytes(first);
            if (data == null) {
                success = false;
                if (first.getLayer() == StorageLayer.CACHE)
                    toCache.put(first.getKey(), first.getHost());
            }
        }
        // read encoded blocks
        else {
            readBlocks(strategy, readResults);
            if (readResults.size() >= LonghairLib.k) {
                data = LonghairLib.decode(ClientUtils.resultsToBytes(readResults));
                if (data == null)
                    success = false;
            } else
                success = false;
            logger.debug("success: " + success);
            // check for cache misses
            logger.debug("readResults: " + readResults.size());
            // TODO observation: sometimes readResults is filled in after readBlcoks returns
            // TODO and after toCache is computed
            for (StorageSubitem subitem : strategy) {
                String key = subitem.getKey();
                if (subitem.getLayer() == StorageLayer.CACHE &&
                    ClientUtils.readResultsContains(readResults, key) == false) {
                    logger.debug("toCache.put " + key);
                    toCache.put(key, subitem.getHost());
                }
            }
        }

        // if read (partially) failed, fallback to backend
        if (success == false) {
            Set<StorageSubitem> backend = storageItem.getBackendSet();
            // if strategy = full data
            if (storageItem.isEncodedInStrategy() == false) {
                // read from backend
                if (storageItem.isEncodedInBackend() == false) {
                    // read full data from backend
                    StorageSubitem first = backend.iterator().next();
                    data = readBytes(first);
                    if (data == null)
                        success = false;
                } else {
                    // read blocks from backend, in parallel
                    readBlocks(backend, readResults);
                    if (readResults.size() >= LonghairLib.k)
                        data = LonghairLib.decode(ClientUtils.resultsToBytes(readResults));
                    else
                        success = false;
                }
            } else {
                // read from backend - only a few blocks or everything?
                if (storageItem.isEncodedInBackend() == false) {
                    // read full data from backend
                    StorageSubitem first = backend.iterator().next();
                    data = readBytes(first);
                    if (data == null)
                        success = false;
                } else {
                    // read missing blocks from backend
                    //List<ReadResult> readResultsBackend = new ArrayList<ReadResult>();
                    Set<StorageSubitem> strategyDiffBackend = new HashSet<StorageSubitem>();
                    for (StorageSubitem subitem : backend) {
                        if (ClientUtils.readResultsContains(readResults, subitem.getKey()) == false) {
                            strategyDiffBackend.add(subitem);
                        }
                    }
                    readBlocks(strategyDiffBackend, readResults);
                    if (readResults.size() >= LonghairLib.k)
                        data = LonghairLib.decode(ClientUtils.resultsToBytes(readResults));
                    else
                        success = false;
                }
            }
        }

        // cache in the background
        if (toCache.size() > 0 && data != null) {
            final byte[] dataFin = data;
            final List<ReadResult> readResultsFin = readResults;
            final Map<String, String> toCacheFin = toCache;
            executor.submit(new Runnable() {
                @Override
                public void run() {
                    if (storageItem.isEncodedInCache() == false) {
                        Map.Entry<String, String> first = toCacheFin.entrySet().iterator().next();
                        cacheData(first.getKey(), dataFin, first.getValue());
                    } else {
                        if (readResultsFin.size() == 0) {
                            List<byte[]> encodedBlocks = LonghairLib.encode(dataFin);
                            String firstKey = toCacheFin.entrySet().iterator().next().getKey();
                            cacheBlocks(toCacheFin, ClientUtils.blocksToReadResults(ClientUtils.getBaseKey(firstKey), encodedBlocks));
                        } else if (readResultsFin.size() >= LonghairLib.k)
                            cacheBlocks(toCacheFin, readResultsFin);
                    }
                }
            });
        }

        return data;
    }

    @Override
    public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        /* operation status to be returned */
        Status status = Status.UNEXPECTED_STATE;

        /* obtain cache info from proxy */
        Map<String, String> keyToCacheHost = proxyConnection.sendGET(key);

        /* ask storage oracle to compile storage info */
        StorageItem storageItem = storageOracle.compileStorageInfo(key, keyToCacheHost);

        /* try to read data according to strategy */
        byte[] data = readByStrategy(storageItem);
        if (data == null)
            status = Status.ERROR;
        else
            status = Status.OK;

        logger.debug("Read " + key + " " + status.getName() + " " + ClientUtils.bytesToHash(data));
        /*try {
            Thread.sleep(600000);
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

    private Status cacheData(String key, byte[] data, String host) {
        MemcachedConnection memConn = memConnections.get(host);
        Status status = memConn.insert(key, data);
        logger.debug("Cached " + key + " " + host + " " + status.getName());
        return status;
    }

    private Status insertData(String key, byte[] data) {
        String bucket = storageOracle.assignS3Bucket(key);
        S3Connection s3Conn = s3Connections.get(bucket);
        Status status = s3Conn.insert(key, data);
        logger.debug("Item " + key + " " + status.getName());
        return status;
    }

    /* insert data (encoded or full) in S3 buckets */
    @Override
    public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
        // operation status
        Status status = Status.UNEXPECTED_STATE;

        // generate bytes array based on values
        byte[] bytes = ClientUtils.valuesToBytes(values);

        /* encode data? */
        if (storageOracle.getS3Encode() == true) {
            /* encode data */
            List<byte[]> encodedBlocks = LonghairLib.encode(bytes);

            /* insert encoded blocks in parallel */
            List<Status> statuses = new ArrayList<Status>();

            CompletionService<Status> completionService = new ExecutorCompletionService<Status>(executor);
            for (int i = 0; i < encodedBlocks.size(); i++) {
                final String blockKey = key + i;
                final byte[] block = encodedBlocks.get(i);
                completionService.submit(new Callable<Status>() {
                    @Override
                    public Status call() throws Exception {
                        return insertData(blockKey, block);
                    }
                });
            }

            int errors = 0;
            while (statuses.size() < encodedBlocks.size()) {
                Future<Status> statusFuture = null;
                try {
                    statusFuture = completionService.take();
                    Status blockInsertStatus = statusFuture.get();
                    if (blockInsertStatus != null)
                        statuses.add(blockInsertStatus);
                    else
                        errors++;
                } catch (Exception e) {
                    logger.error("Exception fetching status for block insert operation.");
                    errors++;
                }
                if (errors > 0)
                    break;
            }

            /* set final status */
            if (statuses.size() != encodedBlocks.size())
                status = Status.ERROR;
            else {
                status = Status.OK;
                for (Status st : statuses) {
                    if (!st.equals(Status.OK))
                        status = Status.ERROR;
                }
            }

        } else {
            /* insert the full data item */
            status = insertData(key, bytes);
        }

        logger.debug("Insert " + key + " " + status.getName());
        return status;
    }

    @Override
    public Status delete(String table, String key) {
        return null;
    }
}
