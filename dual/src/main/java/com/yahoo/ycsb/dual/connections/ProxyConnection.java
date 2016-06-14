package com.yahoo.ycsb.dual.connections;

import com.yahoo.ycsb.common.communication.ProxyReply;
import com.yahoo.ycsb.common.communication.ProxyRequest;
import com.yahoo.ycsb.common.communication.Serializer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.util.Properties;

// Connection established between client and proxy; one connection per client thread
public class ProxyConnection {
    public static String PROXY_PROPERTY = "proxy"; // ip:port
    public static String RETRIES = "proxy.retries"; // number of send retries
    public static String TIMEOUT = "proxy.timeout"; // in ms
    public static String PACKETSIZE = "proxy.packetSize"; // in bytes

    protected static Logger logger = Logger.getLogger(ProxyConnection.class);

    private final int socketRetries;
    private final int socketTimeout;
    private final int packetSize;
    private InetAddress udpServerAddress;
    private int udpServerPort;
    private DatagramSocket socket;

    public ProxyConnection(Properties props) {
        // get proxy host property
        String proxyHost = props.getProperty(PROXY_PROPERTY);
        logger.debug("Proxy connection: " + proxyHost);

        // split proxy host into ip address and port
        String[] tokens = proxyHost.split(":");
        String host = tokens[0];
        int port = Integer.parseInt(tokens[1]);
        try {
            udpServerAddress = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            logger.error("Error getting InetAddress from host.");
        }
        this.udpServerPort = port;

        // read socket properties
        socketRetries = Integer.parseInt(props.getProperty(RETRIES));
        socketTimeout = Integer.parseInt(props.getProperty(TIMEOUT));

        // max size of the packet exchanged by client and proxy
        packetSize = Integer.parseInt(props.getProperty(PACKETSIZE));

        // create UDP connection to proxy and set timeout
        try {
            socket = new DatagramSocket();
            socket.setSoTimeout(socketTimeout);
        } catch (SocketException e) {
            logger.error("Error creating datagram socket.");
        }
    }

    /**
     * Ask proxy where to get blocks corresponding to a data item from
     *
     * @param key of data item
     * @return a reply which contains the cache recipe and the backend recipe
     */
    public ProxyReply requestRecipe(String key) {
        // create new proxy request, serialize it, and use it to create new UDP packet
        ProxyRequest request = new ProxyRequest(key);
        logger.debug(request.prettyPrint());
        byte[] sendData = Serializer.serializeRequest(request);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, udpServerAddress, udpServerPort);

        // try to send the UDP packet to the proxy and wait for a reply (with num of retries set in config)
        boolean retry = true;
        int retries = 0;
        byte[] receiveData = new byte[packetSize];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, packetSize);
        while (retry == true && retries < socketRetries) {
            retries++;
            try {
                socket.send(sendPacket);
            } catch (IOException e) {
                logger.error("Error sending packet to Smart Cache Proxy. Attempt #" + retries);
            }
            try {
                socket.receive(receivePacket);
                retry = false;
            } catch (IOException e) {
                logger.error("Error receiving packet from Smart Cache Proxy. Attempt #" + retries);
            }
        }

        // if a reply from proxy was received, deserialize it and return it
        ProxyReply reply = null;
        if (retry == false)
            reply = Serializer.deserializeReply(receiveData);
        else
            logger.error("Read failed for " + key);

        return reply;
    }
}
