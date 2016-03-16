package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * Created by Raluca on 11.03.16.
 */
public class Proxy {
    public static Logger logger = Logger.getLogger(Proxy.class);

    public static String PROPERTIES_FILE = "proxy.properties";
    public static String PROXIES = "proxy.hosts";
    public static String THREADS_NUM = "executor.threads";
    public static String PACKET_SIZE = "packet.size";
    public static String MEMCACHED_HOSTS = "memcached.hosts";

    public static void main(String args[]) throws UnknownHostException {
        /* properties */
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties properties = new Properties();
        try {
            try (InputStream resourceStream = loader.getResourceAsStream(PROPERTIES_FILE)) {
                properties.load(resourceStream);
            }
        } catch (IOException e) {
            logger.error("Error reading properties.");
        }

        /* memcached servers */
        List<String> memcachedServers = Arrays.asList(properties.getProperty(MEMCACHED_HOSTS).split("\\s*,\\s*"));
        logger.trace("Memcached servers: " + memcachedServers);

        /* proxies */
        List<String> proxies = Arrays.asList(properties.getProperty(PROXIES).split("\\s*,\\s*"));
        logger.trace("Proxies: " + proxies);

        /* this proxy */
        String[] pair = proxies.get(0).split(":");
        InetAddress address = InetAddress.getByName(pair[0]);
        int port = Integer.parseInt(pair[1]);

        /* number of threads to handle client requests */
        final int threadsNum = Integer.valueOf(properties.getProperty(THREADS_NUM));
        logger.trace("threads num: " + threadsNum);

        /* packet size */
        final int packetSize = Integer.valueOf(properties.getProperty(PACKET_SIZE));
        logger.trace("packet size: " + packetSize);

        /* init cache address manager */
        CacheAddressManager cacheAddressManager = new CacheAddressManager(memcachedServers, proxies);

        /* start udp server */
        UDPServer udpServer = new UDPServer(address, port, cacheAddressManager);
        udpServer.setPacketSize(packetSize);
        udpServer.setThreadsNum(threadsNum);
        udpServer.run();
    }
}
