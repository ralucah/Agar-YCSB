package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.common.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Raluca on 11.03.16.
 */
public class UDPServer implements Runnable {
    protected static Logger logger = Logger.getLogger(UDPServer.class);

    /* get from bootstrapper */
    private List<ProxyHost> proxyHosts;
    private CacheStoragePolicy cacheStoragePolicy;

    /* to create */
    private DatagramSocket socket;
    private volatile CacheAddressManager cacheAddressManager; /* shared among executor threads */

    /* server is multithreaded */
    private ExecutorService executor;

    /* set defaults */
    private int packetSize = 1024;
    private int threadsNum = 10;

    public UDPServer(List<ProxyHost> proxyHosts, CacheStoragePolicy cacheStoragePolicy) {
        this.proxyHosts = proxyHosts;
        this.cacheStoragePolicy = cacheStoragePolicy;

        /* create datagram socket */
        ProxyHost me = proxyHosts.get(ProxyConstants.WHO_AM_I);
        try {
            socket = new DatagramSocket(me.getPort(), me.getAddress());
        } catch (SocketException e) {
            logger.error("Error creating socket.");
        }

        /* init executor service */
        executor = Executors.newFixedThreadPool(threadsNum);

        /* create cache address manager */
        cacheAddressManager = new CacheAddressManager();
    }

    public void setPacketSize(int packetSize) {
        this.packetSize = packetSize;
    }

    public void setThreadsNum(int threadsNum) {
        this.threadsNum = threadsNum;
    }

    @Override
    public void run() {
        /* listen from client requests */
        byte[] receiveData = new byte[packetSize];
        while (true) {
            DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
            try {
                socket.receive(receivePacket);
            } catch (IOException e) {
                logger.error("Error receiving packet from client.");
            }
            handleAsync(receivePacket);
        }
    }

    protected void handleAsync(final DatagramPacket packet) {
        executor.execute(new Runnable() {
            public void run() {
                handle(packet);
            }
        });
    }

    /* process get request, computes get response, and sends it back to the client */
    protected void handleGet(ProxyGet msg, InetAddress senderIp, int senderPort) {
        /* compute actual keys based on storage policy */
        List<String> keys = cacheStoragePolicy.computeCacheKeys(msg.getKey());

        /* compute info for get response */
        String address;
        boolean isCached;
        Map<String, CacheInfo> keyToCache = new HashMap<String, CacheInfo>();
        for (String key : keys) {
            isCached = true;
            address = cacheAddressManager.getCacheAddress(key);
            if (address == null) {
                isCached = false;
                address = cacheStoragePolicy.assignCacheAddress(key);
            }
            if (address != null)
                keyToCache.put(key, new CacheInfo(address, isCached));
            else
                logger.warn(key + " cache address is null");
        }

        /* new get response msg */
        ProxyGetResponse getResponseMsg = new ProxyGetResponse(keyToCache);
        logger.debug(getResponseMsg.prettyPrint());

        /* send get response to client */
        byte[] sendData = new byte[packetSize];
        sendData = Serializer.serializeProxyMsg(getResponseMsg);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, senderIp, senderPort);
        try {
            socket.send(sendPacket);
        } catch (IOException e) {
            logger.error("Exception sending packet.");
        }
    }

    /* broadcast to other proxies info about data cached in this data center */
    private void broadcast(final DatagramPacket packet) {
        logger.debug("Broadcast!");
        for (int i = 0; i < proxyHosts.size(); i++) {
            // skip iteration for self
            if (i == ProxyConstants.WHO_AM_I) {
                logger.debug("Skipping iteration for self!");
                continue;
            }

            // for others
            ProxyHost proxy = proxyHosts.get(i);
            packet.setAddress(proxy.getAddress());
            packet.setPort(proxy.getPort());
            logger.debug("Send packet to " + proxy.getAddress() + ":" + proxy.getPort());
            try {
                socket.send(packet);
            } catch (IOException e) {
                logger.error("Exception sending packet to " + proxy.getAddress() + ":" + proxy.getPort());
            }
        }
    }

    protected void handlePut(ProxyPut msg, InetAddress senderIp, int senderPort) {
        Map<String, String> keyToHost = msg.getKeyToHostPairs();
        cacheAddressManager.updateRegistry(keyToHost);
    }

    private boolean isSenderClient(InetAddress senderAddress, int senderPort) {
        for (ProxyHost proxy : proxyHosts) {
            if (proxy.equals(senderAddress, senderPort) == true)
                return false;
        }
        return true;
    }

    protected void handle(final DatagramPacket packet) {
        /* get msg from client */
        ProxyMessage query = (ProxyMessage) Serializer.deserializeProxyMsg(packet.getData());
        InetAddress senderAddress = packet.getAddress();
        int senderPort = packet.getPort();

        if (query != null) {
            logger.debug(query.prettyPrint());
            switch (query.getType()) {
                case GET:
                    handleGet((ProxyGet) query, senderAddress, senderPort);
                    break;
                case PUT:
                    handlePut((ProxyPut) query, senderAddress, senderPort);
                    // if the sender was a client
                    if (isSenderClient(senderAddress, senderPort) == true) {
                        broadcast(packet);
                    }
                    break;
                default:
                    logger.error("Unknown query type!");
                    break;
            }
            logger.debug("Done with " + query.getType());
        } else
            logger.warn("Null query!");
    }
}
