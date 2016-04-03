package com.yahoo.ycsb.dual.utils;

import com.yahoo.ycsb.common.ProxyGet;
import com.yahoo.ycsb.common.ProxyGetResponse;
import com.yahoo.ycsb.common.Serializer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.util.Map;

/* Establish connection to proxy */
public class UDPClient {
    protected static Logger logger = Logger.getLogger(UDPClient.class);

    /* proxy udp server running on.. */
    private InetAddress udpServerAddress;
    private int udpServerPort;

    /* socket + socket configs */
    private DatagramSocket socket;
    private int socketRetries;
    private int packetSize;

    public UDPClient(String host, int port) {
        logger.debug("Proxy connection on " + host + ":" + port);

        /* set udpServerAddress + udpServerPort */
        try {
            udpServerAddress = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            logger.error("Error getting InetAddress from host.");
        }
        this.udpServerPort = port;

        /* establish connection */
        try {
            socket = new DatagramSocket();
        } catch (SocketException e) {
            logger.error("Error creating datagram socket.");
        }

        /* set default socket config */
        socketRetries = Integer.parseInt(ClientConstants.SOCKET_RETRIES_DEFAULT);
        packetSize = Integer.parseInt(ClientConstants.PACKET_SIZE_DEFAULT);
        // no timeout by default, so the client might stall
    }

    /* set socket timeout in ms */
    public void setSocketTimeout(int socketTimeout) {
        try {
            socket.setSoTimeout(socketTimeout);
        } catch (SocketException e) {
            logger.error("Error setting socket timeout.");
        }
        logger.debug("Set socket timeout to " + socketTimeout + " ms");
    }

    /* set the number of retires */
    public void setSocketRetries(int socketRetries) {
        logger.debug("Set socket retries number to " + socketRetries);
        this.socketRetries = socketRetries;
    }

    /* set the packet size */
    public void setPacketSize(int packetSize) {
        logger.debug("Set packet size to " + packetSize);
        this.packetSize = packetSize;
    }

    /* send GET msg with retries */
    public Map<String, String> sendGET(String key) {
        ProxyGet getMsg = new ProxyGet(key);
        logger.debug(getMsg.prettyPrint());

        byte[] sendData = Serializer.serializeProxyMsg(getMsg);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, udpServerAddress, udpServerPort);

        boolean retryGet = true; // send get with retries
        int retryCounter = 0; // attempt number
        byte[] receiveData = new byte[packetSize];
        DatagramPacket receivePacket = new DatagramPacket(receiveData, packetSize);
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
    }

    /* send PUT msg */
    public void sendPUT() {
        // TODO
    }
}
