package com.yahoo.ycsb.common;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by Raluca on 14.03.16.
 */
public class ProxyGetResponse extends ProxyMessage {
    private static final long serialVersionUID = 7526472295622776148L;

    private Map<String, CacheInfo> keyToCacheInfo;

    public ProxyGetResponse(Map<String, CacheInfo> keyToCacheInfo) {
        type = ProxyMessageType.GET_RESPONSE;
        this.keyToCacheInfo = keyToCacheInfo;
    }

    public Map<String, CacheInfo> getKeyToCacheInfoPairs() {
        return keyToCacheInfo;
    }

    /*public void addKeyToCacheInfoPair(String key, String serverAddress, boolean isCached) {
        keyToCacheInfo.put(key, new CacheInfo(serverAddress, isCached));
    }*/

    @Override
    public String prettyPrint() {
        String str = getType().name();
        Iterator it = keyToCacheInfo.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            CacheInfo value = (CacheInfo) pair.getValue();
            str += " (" + pair.getKey() + ", " + value.getCacheAddress() + ", " + value.isCached() + ")";
        }
        return str;
    }
}
