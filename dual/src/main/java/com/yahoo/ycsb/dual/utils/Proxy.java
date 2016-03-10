package com.yahoo.ycsb.dual.utils;

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Created by Raluca on 04.03.16.
 */
public class Proxy {
    private InetAddress ip;
    private int port;

    public Proxy(String host, int port) {
        try {
            ip = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        this.port = port;
    }

    public InetAddress getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }
}
