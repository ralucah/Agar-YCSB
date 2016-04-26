package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import org.apache.log4j.Logger;

public class Cache {
    protected static Logger logger = Logger.getLogger(CacheAdmin.class);

    private String name;
    private long fieldlength;
    private long maxSize;

    private volatile long size;
    private MemcachedConnection connection;

    public Cache(String name, long fieldlength, long maxSize) {
        this.name = name; // ip:port
        this.fieldlength = fieldlength;
        this.maxSize = maxSize;
        size = 0;

        // connection to memcached server
        try {
            connection = new MemcachedConnection(name);
        } catch (ClientException e) {
            logger.error("Error establishing MemcachedConnection to " + name);
        }
    }

    public boolean isFull() {
        if (size + fieldlength <= maxSize)
            return false;
        return true;
    }

    public long getSize() {
        return size;
    }

    public void increment() {
        size += fieldlength;
    }

    public String getName() {
        return name;
    }
}
