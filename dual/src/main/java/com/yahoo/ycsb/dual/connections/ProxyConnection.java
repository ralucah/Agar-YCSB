package com.yahoo.ycsb.dual.connections;

import com.yahoo.ycsb.common.communication.ProxyReply;
import com.yahoo.ycsb.common.communication.ProxyRequest;
import com.yahoo.ycsb.common.communication.Serializer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.util.Properties;

/* Establish connection to proxy */
public class ProxyConnection {
    public static String PROXY_PROPERTY = "proxy.server";
    public static String RETRIES = "proxy.retries";
    public static String RETRIES_DEFAULT = "3";
    public static String TIMEOUT = "proxy.timeout";
    public static String TIMEOUT_DEFAULT = "10000"; // ms
    public static String PACKETSIZE = "proxy.packetsize";
    public static String PACKETSIZE_DEFAULT = "1024"; // bytes
    protected static Logger logger = Logger.getLogger(ProxyConnection.class);
    private final int socketRetries;
    private final int socketTimeout;
    private final int packetsize;
    private InetAddress udpServerAddress;
    private int udpServerPort;
    private DatagramSocket socket;

    public ProxyConnection(Properties props) {
        String proxyHost = props.getProperty(PROXY_PROPERTY);
        logger.debug("Proxy connection: " + proxyHost);

        String[] tokens = proxyHost.split(":");
        String host = tokens[0];
        int port = Integer.parseInt(tokens[1]);
        try {
            udpServerAddress = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            logger.error("Error getting InetAddress from host.");
        }
        this.udpServerPort = port;

        socketRetries = Integer.parseInt(props.getProperty(RETRIES, RETRIES_DEFAULT));
        socketTimeout = Integer.parseInt(props.getProperty(TIMEOUT, TIMEOUT_DEFAULT));
        packetsize = Integer.parseInt(props.getProperty(PACKETSIZE, PACKETSIZE_DEFAULT));

        try {
            socket = new DatagramSocket();
            socket.setSoTimeout(socketTimeout);
        } catch (SocketException e) {
            logger.error("Error creating datagram socket.");
        }
    }

    // request info about key from proxy
    public ProxyReply sendRequest(String key) {
        ProxyRequest request = new ProxyRequest(key);
        byte[] sendData = Serializer.serializeRequest(request);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, udpServerAddress, udpServerPort);

        boolean retry = true;
        int retries = 0;
        byte[] receiveData = new byte[packetsize];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, packetsize);
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

        if (retry == true)
            logger.error("Read failed for " + key);

        ProxyReply reply = Serializer.deserializeReply(receiveData);

        return reply;
    }

    /* send GET msg with retries */
    /*public Map<String, String> sendGET(String key) {
        ProxyGet getMsg = new ProxyGet(key);
        logger.debug(getMsg.prettyPrint());

        byte[] sendData = Serializer.serializeProxyMsg(getMsg);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, udpServerAddress, udpServerPort);

        boolean retryGet = true; // send get with retries
        int retryCounter = 0; // attempt number
        byte[] receiveData = new byte[packetsize];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, packetsize);
        while (retryGet == true && retryCounter < socketRetries) {
            retryCounter++;
            try {
                socket.send(sendPacket);
            } catch (IOException e) {
                logger.error("Error sending packet to Proxy. Attempt #" + retryCounter);
            }
            try {
                socket.receive(receivePacket);
                retryGet = false;
            } catch (IOException e) {
                logger.error("Error receiving packet from Proxy. Attempt #" + retryCounter);
            }
        }

        // msg if get wasn't sent or get response wasn't received
        if (retryGet == true)
            logger.error("Read failed for " + key);

        ProxyGetResponse proxyGetResp = (ProxyGetResponse) Serializer.deserializeProxyMsg(receivePacket.getData());
        return proxyGetResp.getKeyToCacheHost();
    }*/

    /* send PUT msg */
    /*public void sendPUT() {
        // TODO
    }*/
}
