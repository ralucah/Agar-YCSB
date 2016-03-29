package com.yahoo.ycsb.common;

/**
 * Created by Raluca on 11.03.16.
 */
public final class ProxyGet extends ProxyMessage {
    private static final long serialVersionUID = 7526472295622776147L;

    private String key;

    public ProxyGet(String key) {
        type = ProxyMessageType.GET;
        this.key = key;
    }

    public String getKey() {
        return key;
    }

    @Override
    public String prettyPrint() {
        String str = getType().name() + " " + key;
        return str;
    }
}
