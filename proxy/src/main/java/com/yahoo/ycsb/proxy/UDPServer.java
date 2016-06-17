package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.common.communication.ProxyReply;
import com.yahoo.ycsb.common.communication.ProxyRequest;
import com.yahoo.ycsb.common.communication.Serializer;
import com.yahoo.ycsb.common.properties.PropertyFactory;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.*;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// http://stackoverflow.com/questions/28051060/java-properties-best-practices

//./bin/ycsb proxy -p fieldlength=4194304 -P workloads/myworkload


public class UDPServer implements Runnable {
    protected static Logger logger = Logger.getLogger(UDPServer.class);

    protected ExecutorService executor;
    protected int packetSize;
    private DynamicCacheManager cacheManager;
    private DatagramSocket socket;

    public UDPServer() {
        // threads
        int executorThreads = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.EXECUTOR_THREADS_PROPERTY));
        logger.debug("Executor threads: " + executorThreads);

        // packet size for UDP communication
        packetSize = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.PACKET_SIZE_PROPERTY));
        logger.debug("Packet size: " + packetSize);

        // address of current server
        String proxyHost = PropertyFactory.propertiesMap.get(PropertyFactory.PROXY_PROPERTY);

        // datagram socket
        String[] tokens = proxyHost.split(":");
        InetAddress address = null;
        try {
            address = InetAddress.getByName("0.0.0.0"); // tokens[0]
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
        executor = Executors.newFixedThreadPool(executorThreads);

        // dynamic cache manager
        cacheManager = new DynamicCacheManager();

        logger.info("Proxy server running on " + proxyHost);
    }

    public static void usageMessage() {
        logger.warn("Usage: java com.yahoo.ycsb.proxy.UDPServer [options]");
        logger.warn("Options:");
        logger.warn("-threads n: execute using n threads (default: 1) - \"-p threadcount\"");
        logger.warn("-P propertyfile: load properties from the given file");
    }

    public static void main(String args[]) {
        Properties props = new Properties();
        Properties fileprops = new Properties();

        /*int i = 0;
        while (i < args.length) {
            System.out.println(i + " " + args[i]);
            i++;
        }*/

        //parse arguments
        int argindex = 0;

        if (args.length <= 1) {
            usageMessage();
            System.exit(0);
        }

        while (args[argindex].startsWith("-")) {
            if (args[argindex].compareTo("-P") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                String propfile = args[argindex];
                argindex++;

                //Properties myfileprops = new Properties();
                try {
                    fileprops.load(new FileInputStream(propfile));
                } catch (IOException e) {
                    logger.error(e.getMessage());
                    System.exit(0);
                }

                for (Enumeration e = fileprops.propertyNames(); e.hasMoreElements(); ) {
                    String prop = (String) e.nextElement();

                    fileprops.setProperty(prop, fileprops.getProperty(prop));
                }

            } else if (args[argindex].compareTo("-p") == 0) {
                argindex++;
                if (argindex >= args.length) {
                    usageMessage();
                    System.exit(0);
                }
                int eq = args[argindex].indexOf('=');
                if (eq < 0) {
                    usageMessage();
                    System.exit(0);
                }

                String name = args[argindex].substring(0, eq);
                String value = args[argindex].substring(eq + 1);
                props.put(name, value);
                //System.out.println("["+name+"]=["+value+"]");
                argindex++;
            } else {
                logger.warn("Unknown option " + args[argindex]);
                usageMessage();
                System.exit(0);
            }
            if (argindex >= args.length) {
                break;
            }
        }

        for (Enumeration e = props.propertyNames(); e.hasMoreElements(); ) {
            String prop = (String) e.nextElement();

            fileprops.setProperty(prop, props.getProperty(prop));
        }

        props = fileprops;
        PropertyFactory propertyFactory = new PropertyFactory(props);

        UDPServer server = new UDPServer();
        server.run();
    }

    /**
     * Handle client request
     *
     * @param packet received from client
     */
    protected void handle(final DatagramPacket packet) {
        // get request from client
        ProxyRequest request = Serializer.deserializeRequest(packet.getData());
        InetAddress clientAddress = packet.getAddress();
        int clientPort = packet.getPort();
        logger.debug(request.prettyPrint()); // + " from " + clientAddress + ":" + clientPort);

        // compute reply
        ProxyReply reply = cacheManager.buildReply(request.getKey());
        logger.debug(reply.prettyPrint());// + " to " + clientAddress + ":" + clientPort);

        // send reply to client
        byte[] sendData = Serializer.serializeReply(reply);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, clientAddress, clientPort);
        try {
            socket.send(sendPacket);
        } catch (IOException e) {
            logger.error("Exception sending packet to " + clientAddress + ":" + clientPort);
        }
    }

    /**
     * Handle each client request in a new thread from the executor thread pool
     * @param packet received from client
     */
    protected void handleAsync(final DatagramPacket packet) {
        executor.execute(new Runnable() {
            public void run() {
                handle(packet);
            }
        });
    }

    @Override
    public void run() {
        // listen for requests from clients, and handle them asynchronously - in a new thread from the executor's thread pool
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
