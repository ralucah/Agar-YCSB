package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Bootstrapper {
    public static Logger logger = Logger.getLogger(Bootstrapper.class);

    public static void main(String args[]) throws UnknownHostException {
        /* properties */
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        Properties properties = new Properties();
        try {
            try (InputStream resourceStream = loader.getResourceAsStream(ProxyConstants.PROXY_PROPERTIES)) {
                properties.load(resourceStream);
            }
        } catch (IOException e) {
            logger.error("Error reading properties.");
        }

        /* cache storage policy */
        List<String> memcachedServers = Arrays.asList(properties.getProperty(ProxyConstants.MEMCACHED_HOSTS).split("\\s*,\\s*"));
        logger.debug("Memcached servers: " + memcachedServers);

        boolean memEncode = Boolean.valueOf(properties.getProperty(ProxyConstants.MEMCACHED_ENCODE, "false"));
        logger.debug("memEncode: " + memEncode);

        CacheOracle cacheOracle = new CacheOracle(memcachedServers, memEncode);

        if (memEncode == true) {
            final int numBlocks = Integer.valueOf(properties.getProperty(ProxyConstants.MEMCACHED_NUM_BLOCKS));
            logger.trace("num blocks: " + numBlocks);
            cacheOracle.setNumBlocks(numBlocks);
        }

        /* udp server */
        ProxyConstants.WHO_AM_I = Integer.valueOf(properties.getProperty(ProxyConstants.PROXY_ID));

        List<String> proxies = Arrays.asList(properties.getProperty(ProxyConstants.PROXIES).split("\\s*,\\s*"));
        logger.debug("Proxies: " + proxies);
        List<ProxyHost> proxyHosts = new ArrayList<ProxyHost>();
        for (int i = 0; i < proxies.size(); i++) {
            proxyHosts.add(new ProxyHost(proxies.get(i)));
        }

        final int threadsNum = Integer.valueOf(properties.getProperty(ProxyConstants.THREADS_NUM));
        logger.debug("threads num: " + threadsNum);

        final int packetSize = Integer.valueOf(properties.getProperty(ProxyConstants.PACKET_SIZE));
        logger.trace("packet size: " + packetSize);

        UDPServer udpServer = new UDPServer(proxyHosts, cacheOracle);
        udpServer.setPacketSize(packetSize);
        udpServer.setThreadsNum(threadsNum);
        udpServer.run();
    }
}
