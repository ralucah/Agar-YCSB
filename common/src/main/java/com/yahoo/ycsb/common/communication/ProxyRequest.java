package com.yahoo.ycsb.common.communication;

import java.io.Serializable;

/**
 * Created by Raluca on 24.04.16.
 */
public class ProxyRequest implements Serializable {
    private static final long serialVersionUID = 7526472295622776147L;

    private String key;

    public ProxyRequest(String key) {
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    public String prettyPrint() {
        return "ProxyRequest: " + key;
    }
}
