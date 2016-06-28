package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.common.communication.ProxyReply;

public abstract class CacheManagerBlueprint {
    public abstract ProxyReply buildReply(String key);
}
