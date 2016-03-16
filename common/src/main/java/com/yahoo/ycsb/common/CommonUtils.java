package com.yahoo.ycsb.common;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by Raluca on 10.03.16.
 */
public abstract class CommonUtils {
    private static Logger logger = Logger.getLogger(CommonUtils.class);

    public static byte[] serializeProxyMsg(ProxyMessage msg) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream outStream = null;
        try {
            outStream = new ObjectOutputStream(out);
            outStream.writeObject(msg);
        } catch (IOException e) {
            logger.error("Error creating object output stream.");
        } finally {
            if (outStream != null)
                try {
                    outStream.close();
                } catch (IOException e) {
                    logger.error("Error closing object output stream.");
                }
        }
        return out.toByteArray();
    }

    public static ProxyMessage deserializeProxyMsg(byte[] bytes) {
        ProxyMessage msg = null;
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        ObjectInputStream inStream = null;
        try {
            inStream = new ObjectInputStream(in);
            msg = (ProxyMessage) inStream.readObject();
        } catch (IOException e) {
            logger.error("Error creating object input stream.");
        } catch (ClassNotFoundException e) {
            logger.error("Error reading from input stream.");
        } finally {
            if (inStream != null)
                try {
                    inStream.close();
                } catch (IOException e) {
                    logger.error("Error closing object input stream.");
                }
        }
        return msg;
    }

    public static String listToStr(List<String> list) {
        String str = "";
        for (String s : list)
            str += s + " ";
        return str;
    }

    //TODO what is this?? sun.security.util.Cache<K,V>

    public static String mapToStr(Map<String, CacheInfo> map) {
        String str = "";
        Iterator it = map.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            CacheInfo value = (CacheInfo) pair.getValue();
            str += "(" + pair.getKey() + ", " + value.getCacheServer() + ", " + value.isCached() + ") ";
        }
        return str;
    }
}
