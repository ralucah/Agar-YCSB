package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import org.apache.log4j.Logger;

public class Cache {
    protected static Logger logger = Logger.getLogger(CacheAdmin.class);

    private String name;
    private volatile long size;
    private MemcachedConnection connection;

    public Cache(String name) {
        // ip:port
        this.name = name;

        // initially 0
        size = 0;

        // connection to memcached server
        try {
            connection = new MemcachedConnection(name);
        } catch (ClientException e) {
            logger.error("Error establishing MemcachedConnection to " + name);
        }
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String getName() {
        return name;
    }
}
