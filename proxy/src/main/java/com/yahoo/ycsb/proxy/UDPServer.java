package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.common.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Raluca on 11.03.16.
 */
public class UDPServer implements Runnable {
    protected static Logger logger = Logger.getLogger(UDPServer.class);

    private DatagramSocket socket;
    private ExecutorService executor;
    private volatile CacheAddressManager cacheAddressManager; /* shared among executor threads */

    /* set defaults */
    private int packetSize = 1024;
    private int threadsNum = 10;

    public UDPServer(InetAddress address, int port, CacheAddressManager cacheAddressManager) {
        /* create datagram socket */
        try {
            socket = new DatagramSocket(port, address);
            logger.trace("UDP server on " + address + ":" + port);
        } catch (SocketException e) {
            logger.error("Error creating socket.");
        }
        /* init executor service */
        executor = Executors.newFixedThreadPool(threadsNum);
        /* set cache addr manager */
        this.cacheAddressManager = cacheAddressManager;
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

    private void handleAsync(final DatagramPacket packet) {
        executor.execute(new Runnable() {
            public void run() {
                handle(packet);
            }
        });
    }

    protected void handleGet(final DatagramPacket packet, ProxyGet msg) {
        /* access the cache address manager and build a reply */
        // TODO HASH MAP CANNOT CONTAIN DUPLICATES!
        List<String> keys = msg.getKeys();
        ProxyGetResponse getResponse = new ProxyGetResponse();
        for (String key : keys) {
            boolean isCached = true;
            String serverAddress = cacheAddressManager.getCacheServer(key);
            if (serverAddress == null) {
                isCached = false;
                serverAddress = cacheAddressManager.assignToCacheServer(key);
            }
            getResponse.addKeyToCacheInfoPair(key, serverAddress, isCached);
        }
        logger.debug(getResponse.print());

        /* send back to client */
        InetAddress clientIp = packet.getAddress();
        int clientPort = packet.getPort();
        byte[] sendData = new byte[packetSize];
        sendData = CommonUtils.serializeProxyMsg(getResponse);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, clientIp, clientPort);
        try {
            socket.send(sendPacket);
        } catch (IOException e) {
            logger.error("Exception sending packet.");
        }
    }

    protected void handlePut(ProxyPut msg) {
        Map<String, String> keyToHost = msg.getKeyToHostPairs();
        cacheAddressManager.update(keyToHost);
    }

    private void handle(final DatagramPacket packet) {
        /* get msg from client */
        ProxyMessage query = (ProxyMessage) CommonUtils.deserializeProxyMsg(packet.getData());
        if (query != null) {
            logger.debug(query.print());
            switch (query.getType()) {
                case GET:
                    handleGet(packet, (ProxyGet) query);
                    break;
                case PUT:
                    handlePut((ProxyPut) query);
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
