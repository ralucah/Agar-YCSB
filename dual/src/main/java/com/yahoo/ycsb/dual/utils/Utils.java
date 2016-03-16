package com.yahoo.ycsb.dual.utils;

import com.yahoo.ycsb.ByteIterator;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by ubuntu on 10.01.16.
 */
public abstract class Utils {
    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    private static Logger logger = Logger.getLogger(Utils.class);

    public static byte[] valuesToBytes(HashMap<String, ByteIterator> values) {
        // get the first value
        int fieldCount = values.size();
        Object keyToSearch = values.keySet().toArray()[0];
        byte[] sourceArray = values.get(keyToSearch).toArray();
        int sizeArray = sourceArray.length;

        // use it to generate new value
        int totalSize = sizeArray * fieldCount;
        byte[] bytes = new byte[totalSize];
        int offset = 0;
        for (int i = 0; i < fieldCount; i++) {
            System.arraycopy(sourceArray, 0, bytes, offset, sizeArray);
            offset += sizeArray;
        }
        //logger.trace("Value size: " + bytes.length);
        return bytes;
    }

    public static String bytesToHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for ( int j = 0; j < bytes.length; j++ ) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static boolean containsBlock(List<EncodedBlock> blocks, byte[] blockBytes) {
        for (EncodedBlock res : blocks) {
            if (Arrays.equals(res.getBytes(), blockBytes))
                return true;
        }
        return false;
    }

    public static List<byte[]> blocksToBytes(List<EncodedBlock> encodedBlocks) {
        List<byte[]> blockBytes = new ArrayList<byte[]>();
        for (EncodedBlock blockRes : encodedBlocks)
            blockBytes.add(blockRes.getBytes());
        return blockBytes;
    }

    public static List<String> computeBlockKeys(String key, int numBlocks) {
        List<String> blockKeys = new ArrayList<String>();
        for (int i = 0; i < numBlocks; i++) {
            blockKeys.add(key + i);
        }
        return blockKeys;
    }

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

    public static int computeBlockId(String key) {
        return Integer.parseInt(key.substring(key.length() - 1));
    }
}
