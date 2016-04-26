package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.common.communication.ProxyRequest;
import com.yahoo.ycsb.common.communication.Serializer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.*;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class UDPServer implements Runnable {
    public static String PROXY_PROPERTIES = "proxy.properties";
    public static String PROXY = "proxy";
    public static String THREADS_NUM = "threads";
    public static String THREADS_NUM_DEFAULT = "5";
    public static String PACKET_SIZE = "packetsize";
    public static String PACKET_SIZE_DEFAULT = "1024";
    public static String CACHE_SIZE = "cachesize";
    public static String CACHE_SIZE_DEFAULT = "64"; // in mb
    public static String FIELD_LENGTH = "fieldlength";
    public static String FIELD_LENGTH_DEFAULT = "4194304"; // in bytes
    public static String MEMCACHED = "memcached";
    public static String LONGHAIR_K = "longhair.k";
    public static String LONGHAIR_M = "longhair.m";
    protected static Logger logger = Logger.getLogger(UDPServer.class);
    protected static ExecutorService executor;
    protected static int packetSize;
    private DatagramSocket socket;
    private CacheAdmin cacheAdmin;

    public UDPServer() {
        // proxy properties
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties properties = new Properties();
        try {
            try (InputStream resourceStream = loader.getResourceAsStream(PROXY_PROPERTIES)) {
                properties.load(resourceStream);
            }
        } catch (IOException e) {
            logger.error("Error reading properties.");
        }

        // threads
        int threadsNum = Integer.valueOf(properties.getProperty(THREADS_NUM, THREADS_NUM_DEFAULT));
        logger.debug("Threads number: " + threadsNum);

        // packet size for UDP communication
        packetSize = Integer.valueOf(properties.getProperty(PACKET_SIZE, PACKET_SIZE_DEFAULT));
        logger.trace("Packet size: " + packetSize);

        // data size
        long fieldlength = Long.valueOf(properties.getProperty(FIELD_LENGTH, FIELD_LENGTH_DEFAULT));
        logger.debug("fieldlength: " + fieldlength);

        // max cache size
        long cachesize = Long.valueOf(properties.getProperty(CACHE_SIZE, CACHE_SIZE_DEFAULT)) * 1024 * 1024;
        logger.debug("cachesize: " + cachesize);

        // memcached servers
        String memHost = properties.getProperty(MEMCACHED);
        //cacheAdmin = new CacheAdmin(memHosts, cachesize, fieldlength);

        // address of current server
        String proxyHost = properties.getProperty(PROXY);
        logger.debug("Proxy: " + proxyHost);

        // datagram socket
        String[] tokens = proxyHost.split(":");
        InetAddress address = null;
        try {
            address = InetAddress.getByName(tokens[0]);
        } catch (UnknownHostException e) {
            logger.error("Error getting inetaddress.");
        }
        int port = Integer.parseInt(tokens[1]);
        try {
            socket = new DatagramSocket(port, address);
        } catch (SocketException e) {
            logger.error("Error creating socket.");
        }

        // executor service
        executor = Executors.newFixedThreadPool(threadsNum);
    }

    public static void main(String args[]) throws UnknownHostException {
        UDPServer server = new UDPServer();
        server.run();
    }

    protected void handle(final DatagramPacket packet) {
        // get request from client
        ProxyRequest request = Serializer.deserializeRequest(packet.getData());
        InetAddress clientAddress = packet.getAddress();
        int clientPort = packet.getPort();
        logger.info(request.prettyPrint() + " from " + clientAddress + ":" + clientPort);

        // compute reply
        //ProxyReply reply = cacheAdmin.computeReply(request.getKey());
        //logger.info(reply.prettyPrint() + " to " + clientAddress + ":" + clientPort);
        //logger.debug(cacheAdmin.printCacheRegistry());

        // send reply to client
        //byte[] sendData = Serializer.serializeReply(reply);
        //DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, clientAddress, clientPort);
        /*try {
            socket.send(sendPacket);
        } catch (IOException e) {
            logger.error("Exception sending packet to " + clientAddress + ":" + clientPort);
        }*/
    }

    protected void handleAsync(final DatagramPacket packet) {
        executor.execute(new Runnable() {
            public void run() {
                handle(packet);
            }
        });
    }

    @Override
    public void run() {
        // listen for client requests
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
}
