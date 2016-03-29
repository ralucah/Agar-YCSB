package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Raluca on 29.03.16.
 */
public class ProxyHost {
    public static Logger logger = Logger.getLogger(ProxyHost.class);

    private InetAddress address;
    private int port;

    public ProxyHost(String host) {
        String[] pair = host.split(":");
        try {
            address = InetAddress.getByName(pair[0]);
        } catch (UnknownHostException e) {
            logger.error("Unknown host for proxy " + host);
        }
        port = Integer.parseInt(pair[1]);
    }

    public int getPort() {
        return port;
    }

    public InetAddress getAddress() {
        return address;
    }

    @Override
    public boolean equals(Object other) {
        ProxyHost otherProxy = (ProxyHost) other;
        if (otherProxy.getAddress().equals(address) && otherProxy.getPort() == this.port)
            return true;
        return false;
    }

    public boolean equals(InetAddress otherAddress, int otherPort) {
        if (otherAddress.equals(address) && otherPort == port)
            return true;
        return false;
    }
}
