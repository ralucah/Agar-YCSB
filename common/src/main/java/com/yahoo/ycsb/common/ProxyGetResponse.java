package com.yahoo.ycsb.common;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by Raluca on 14.03.16.
 */
public class ProxyGetResponse extends ProxyMessage {
    private static final long serialVersionUID = 7526472295622776148L;

    private Map<String, String> keyToCacheHost;

    public ProxyGetResponse(Map<String, String> keyToCacheHost) {
        type = ProxyMessageType.GET_RESPONSE;
        this.keyToCacheHost = keyToCacheHost;
    }

    public Map<String, String> getKeyToCacheHost() {
        return keyToCacheHost;
    }

    @Override
    public String prettyPrint() {
        String str = getType().name();
        Iterator it = keyToCacheHost.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            str += " (" + pair.getKey() + ", " + pair.getValue() + ")";
        }
        return str;
    }
}
