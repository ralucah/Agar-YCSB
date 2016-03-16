package com.yahoo.ycsb.common;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Raluca on 14.03.16.
 */
public class ProxyGetResponse extends ProxyMessage {
    private static final long serialVersionUID = 7526472295622776148L;

    private Map<String, CacheInfo> keyToCacheInfo;

    public ProxyGetResponse() {
        type = ProxyMessageType.GET_RESPONSE;
        keyToCacheInfo = new HashMap<String, CacheInfo>();
    }

    public Map<String, CacheInfo> getKeyToCacheInfoPairs() {
        return keyToCacheInfo;
    }

    public void addKeyToCacheInfoPair(String key, String serverAddress, boolean isCached) {
        keyToCacheInfo.put(key, new CacheInfo(serverAddress, isCached));
    }
}
