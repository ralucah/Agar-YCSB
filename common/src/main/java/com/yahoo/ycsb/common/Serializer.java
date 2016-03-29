package com.yahoo.ycsb.common;

import org.apache.log4j.Logger;

import java.io.*;

/**
 * Created by Raluca on 10.03.16.
 */
public abstract class Serializer {
    private static Logger logger = Logger.getLogger(Serializer.class);

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
        if (bytes != null) {
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
        }
        return msg;
    }
}
