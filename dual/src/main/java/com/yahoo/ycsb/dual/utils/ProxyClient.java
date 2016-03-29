package com.yahoo.ycsb.dual.utils;

import com.yahoo.ycsb.common.CacheInfo;
import com.yahoo.ycsb.common.ProxyGet;
import com.yahoo.ycsb.common.ProxyGetResponse;
import com.yahoo.ycsb.common.Serializer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.*;
import java.util.Map;

/**
 * Created by Raluca on 25.03.16.
 */
public class ProxyClient {
    /* logger */
    protected static Logger logger = Logger.getLogger(ProxyClient.class);

    /* address + port */
    private InetAddress address;
    private int port;

    /* socket + socket configs */
    private DatagramSocket socket;
    private int socketRetries;
    private int packetSize;

    public ProxyClient(String host, int port) {
        logger.debug("Proxy connection on " + host + ":" + port);

        /* set address + port */
        try {
            address = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            logger.error("Error getting InetAddress from host.");
        }
        this.port = port;

        /* establish connection */
        try {
            socket = new DatagramSocket();
        } catch (SocketException e) {
            logger.error("Error creating datagram socket.");
        }

        /* set default socket config */
        socketRetries = Integer.parseInt(Constants.SOCKET_RETRIES_DEFAULT);
        packetSize = Integer.parseInt(Constants.PACKET_SIZE_DEFAULT);
        // no timeout by default
    }

    /* set socket timeout in ms */
    public void setSocketTimeout(int socketTimeout) {
        logger.debug("Set socket timeout to " + socketTimeout + " ms");
        try {
            socket.setSoTimeout(socketTimeout);
        } catch (SocketException e) {
            logger.error("Error setting socket timeout.");
        }
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

    /* send GET msg */
    public Map<String, CacheInfo> sendGET(String key) {
        ProxyGet getMsg = new ProxyGet(key);
        getMsg.setNumBlocks(LonghairLib.k + LonghairLib.m);
        logger.debug(getMsg.prettyPrint());

        byte[] sendData = Serializer.serializeProxyMsg(getMsg);
        DatagramPacket sendPacket = new DatagramPacket(sendData, sendData.length, address, port);

        // send get with retries
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
        return proxyGetResp.getKeyToCacheInfoPairs();
    }

    /* send PUT msg */
    public void sendPUT() {

    }
}
