package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.common.communication.ProxyReply;
import com.yahoo.ycsb.common.communication.ProxyRequest;
import com.yahoo.ycsb.common.communication.Serializer;
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.*;
import java.util.Enumeration;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// http://stackoverflow.com/questions/28051060/java-properties-best-practices

public class UDPServer implements Runnable {
    protected static Logger logger = Logger.getLogger(UDPServer.class);

    protected static ExecutorService executor;
    protected static int packetSize;
    private static GreedyCacheManager greedy;
    private DatagramSocket socket;
    private int k = 6;

    public UDPServer() {

        // threads
        int executorThreads = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.EXECUTOR_THREADS_PROPERTY));
        logger.debug("Executor threads: " + executorThreads);

        // packet size for UDP communication
        packetSize = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.PACKET_SIZE_PROPERTY));
        logger.trace("Packet size: " + packetSize);

        // address of current server
        String proxyHost = PropertyFactory.propertiesMap.get(PropertyFactory.PROXY_PROPERTY);
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
        executor = Executors.newFixedThreadPool(executorThreads);

        // greedy cache manager
        greedy = new GreedyCacheManager();

    }

    public static void usageMessage() {
        System.out.println("Usage: java com.yahoo.ycsb.proxy.UDPServer [options]");
        System.out.println("Options:");
        System.out.println("-threads n: execute using n threads (default: 1) - \"-p threadcount\"");
        System.out.println("-P propertyfile: load properties from the given file");
    }

    public static void main(String args[]) {
        Properties props = new Properties();
        Properties fileprops = new Properties();

        int i = 0;
        while (i < args.length) {
            System.out.println(i + " " + args[i]);
            i++;
        }

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
                    System.out.println(e.getMessage());
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
                System.out.println("Unknown option " + args[argindex]);
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

    protected void handle(final DatagramPacket packet) {
        // get request from client
        ProxyRequest request = Serializer.deserializeRequest(packet.getData());
        InetAddress clientAddress = packet.getAddress();
        int clientPort = packet.getPort();
        logger.info(request.prettyPrint()); // + " from " + clientAddress + ":" + clientPort);

        // compute reply
        int blocks = greedy.getCachedBlocks(request.getKey());
        ProxyReply reply = new ProxyReply(blocks);
        logger.info(reply.prettyPrint());// + " to " + clientAddress + ":" + clientPort);

        // send reply to client
        byte[] sendData = Serializer.serializeReply(reply);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, clientAddress, clientPort);
        try {
            socket.send(sendPacket);
        } catch (IOException e) {
            logger.error("Exception sending packet to " + clientAddress + ":" + clientPort);
        }
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
