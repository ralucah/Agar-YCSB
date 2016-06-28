package com.yahoo.ycsb.proxy;

/**
 * Created by Raluca on 25.06.16.
 */
public class CacheManagerException extends Exception {
    /**
     *
     */
    private static final long serialVersionUID = 6646883591588721476L;

    public CacheManagerException(String message) {
        super(message);
    }

    public CacheManagerException() {
        super();
    }

    public CacheManagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public CacheManagerException(Throwable cause) {
        super(cause);
    }
}
