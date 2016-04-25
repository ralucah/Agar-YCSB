package com.yahoo.ycsb.common.communication;

import org.apache.log4j.Logger;

import java.io.*;

/**
 * Created by Raluca on 10.03.16.
 */
public abstract class Serializer {
    private static Logger logger = Logger.getLogger(Serializer.class);

    public static byte[] serializeRequest(ProxyRequest request) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream outStream = null;
        try {
            outStream = new ObjectOutputStream(out);
            outStream.writeObject(request);
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

    public static byte[] serializeReply(ProxyReply reply) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream outStream = null;
        try {
            outStream = new ObjectOutputStream(out);
            outStream.writeObject(reply);
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

    public static ProxyRequest deserializeRequest(byte[] bytes) {
        ProxyRequest request = null;
        if (bytes != null) {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ObjectInputStream inStream = null;
            try {
                inStream = new ObjectInputStream(in);
                request = (ProxyRequest) inStream.readObject();
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
        return request;
    }

    public static ProxyReply deserializeReply(byte[] bytes) {
        ProxyReply reply = null;
        if (bytes != null) {
            ByteArrayInputStream in = new ByteArrayInputStream(bytes);
            ObjectInputStream inStream = null;
            try {
                inStream = new ObjectInputStream(in);
                reply = (ProxyReply) inStream.readObject();
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
        return reply;
    }
}
