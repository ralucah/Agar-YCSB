package com.yahoo.ycsb.common;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by Raluca on 15.03.16.
 */
public class ProxyPut extends ProxyMessage {
    private static final long serialVersionUID = 7526472295622776149L;

    private Map<String, String> keyToHost;

    public ProxyPut() {
        type = ProxyMessageType.PUT;
        keyToHost = new HashMap<String, String>();
    }

    public void addKeyToHostPair(String key, String host) {
        keyToHost.put(key, host);
    }

    public Map<String, String> getKeyToHostPairs() {
        return keyToHost;
    }

    @Override
    public String prettyPrint() {
        String str = getType().name();
        Iterator it = keyToHost.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            str += " (" + pair.getKey() + ", " + pair.getValue() + ")";
        }
        return str;
    }
}
