/**
 * Copyright 2016 [Agar]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.ycsb.client.utils;

import com.yahoo.ycsb.utils.communication.ProxyReply;
import com.yahoo.ycsb.utils.communication.ProxyRequest;
import com.yahoo.ycsb.utils.communication.Serializer;
import com.yahoo.ycsb.utils.properties.PropertyFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.net.*;

// Connection established between client and proxy; one connection per client thread
public class AgarProxyConnection {

    protected static Logger logger = Logger.getLogger(AgarProxyConnection.class);

    private final int socketRetries;
    private final int socketTimeout;
    private final int packetSize;
    private InetAddress udpServerAddress;
    private int udpServerPort;
    private DatagramSocket socket;

    public AgarProxyConnection() {
        if (PropertyFactory.propertiesMap == null || PropertyFactory.propertiesMap.size() == 0)
            logger.warn("Property factory not initialized");
        // get proxy host property
        String proxyHost = PropertyFactory.propertiesMap.get(PropertyFactory.PROXY_PROPERTY);
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
        socketRetries = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.REQUEST_RETRIES));
        socketTimeout = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.REQUEST_TIMEOUT));

        // max size of the packet exchanged by client and proxy
        packetSize = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.REQUEST_TIMEOUT));

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
