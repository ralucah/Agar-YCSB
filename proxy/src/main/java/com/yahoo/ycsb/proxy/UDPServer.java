package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Raluca on 04.03.16.
 */
public class UDPServer {
    public static Logger logger = Logger.getLogger(UDPServer.class);

    public static String PROPERTIES_FILE = "proxy.properties";
    public static String PROXIES = "proxy.hosts";
    public static String THREADS_NUM = "threads";
    public static String PACKET_SIZE = "packet.size";
    public static String MEMCACHED_SERVERS = "memcached.hosts";

    protected static DatagramSocket socket;
    protected static int packetSize;

    protected static void handle(DatagramPacket packet) {
        /* get list of blocks */
        List<String> blockKeys = Utils.bytesToList(packet.getData());

        /* process! */
        List<String> processed = new ArrayList<String>();
        for (String blockKey : blockKeys) {
            logger.trace("Received: " + blockKey);
            // assign to server and append ip
            processed.add(blockKey + ":processed");
        }

        /* send back to client */
        InetAddress clientIp = packet.getAddress();
        int clientPort = packet.getPort();
        byte[] sendData = new byte[packetSize];
        sendData = Utils.listToBytes(processed);

        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, clientIp, clientPort);
        try {
            socket.send(sendPacket);
        } catch (IOException e) {
            logger.error("Exception sending packet.");
        }

        /*String sentence = new String(packet.getData());
        logger.debug("Received: " + sentence);

        InetAddress clientIp = packet.getAddress();
        int clientPort = packet.getPort();

        String capitalizedSentence = sentence.toUpperCase();
        byte[] sendData = new byte[packet.getLength()];
        sendData = capitalizedSentence.getBytes();
        logger.debug("Sent: " + capitalizedSentence);

        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, clientIp, clientPort);
        try {
            socket.send(sendPacket);
        } catch(IOException e) {
            logger.error("Exception sending packet.");
        }*/
    }

    protected static void handleAsync(ExecutorService executor, final DatagramPacket packet) {
        executor.execute(new Runnable() {
            public void run() {
                handle(packet);
            }
        });
    }


    public static void main(String args[]) throws Exception {
        /* properties */
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties properties = new Properties();
        try (InputStream resourceStream = loader.getResourceAsStream(PROPERTIES_FILE)) {
            properties.load(resourceStream);
        }

        /* memcached servers from this data center */
        List<String> memcachedHosts = Arrays.asList(properties.getProperty(MEMCACHED_SERVERS).split("\\s*,\\s*"));

        /* proxies */
        List<String> proxies = Arrays.asList(properties.getProperty(MEMCACHED_SERVERS).split("\\s*,\\s*"));

        /* this proxy */
        String[] pair = proxies.get(0).split(":");
        String host = properties.getProperty(pair[0]);
        InetAddress address = InetAddress.getByName(host);
        int port = Integer.parseInt(pair[1]);
        logger.trace("UDP server running on " + host + ":" + port);

        /* other proxies */
        List<String> otherProxies = new ArrayList<String>();
        for (int i = 1; i < proxies.size(); i++)
            otherProxies.add(proxies.get(i));


        /* number of threads to handle client requests */
        final int threadsNum = Integer.valueOf(properties.getProperty(THREADS_NUM));
        logger.trace("num threads: " + threadsNum);
        ExecutorService executor = Executors.newFixedThreadPool(threadsNum);

        /* packet length */
        packetSize = Integer.valueOf(properties.getProperty(PACKET_SIZE));
        logger.trace("packet size: " + packetSize);

        socket = null;
        try {
            socket = new DatagramSocket(port, address);
            byte[] receiveData = new byte[packetSize];

            while (true) {
                DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                socket.receive(receivePacket);
                handle(receivePacket);
            }
        } catch (IOException e) {
            logger.error("IOException " + e.getMessage());
        } finally {
            logger.trace("Closing server socket.");
            //if (socket!= null)
            //    socket.close();
        }
    }
}
