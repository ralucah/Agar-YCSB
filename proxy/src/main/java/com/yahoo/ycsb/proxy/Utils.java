package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Raluca on 10.03.16.
 */
public abstract class Utils {
    private static Logger logger = Logger.getLogger(Utils.class);

    public static byte[] listToBytes(List<String> list) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ObjectOutputStream outStream = null;
        try {
            outStream = new ObjectOutputStream(out);
            outStream.writeObject(list);
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

    public static List<String> bytesToList(byte[] bytes) {
        List<String> list = new ArrayList<String>();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
        ObjectInputStream inStream = null;
        try {
            inStream = new ObjectInputStream(in);
            list = (List<String>) inStream.readObject();
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
        return list;
    }
}
