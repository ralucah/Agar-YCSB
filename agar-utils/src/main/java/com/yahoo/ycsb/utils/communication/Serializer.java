/**
 * Copyright 2016 [Agar]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.ycsb.utils.communication;

import org.apache.log4j.Logger;

import java.io.*;

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
