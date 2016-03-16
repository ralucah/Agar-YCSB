package com.yahoo.ycsb.common;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Raluca on 11.03.16.
 */
public final class ProxyGet extends ProxyMessage {
    private static final long serialVersionUID = 7526472295622776147L;

    private List<String> keys; // list of block keys

    public ProxyGet() {
        type = ProxyMessageType.GET;
        keys = new ArrayList<String>();
    }

    public ProxyGet(List<String> keys) {
        type = ProxyMessageType.GET;
        this.keys = keys;
    }

    public void addKey(String key) {
        keys.add(key);
    }

    public List<String> getKeys() {
        return keys;
    }
}
