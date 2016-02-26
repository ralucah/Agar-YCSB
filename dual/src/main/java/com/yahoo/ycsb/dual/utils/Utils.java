package com.yahoo.ycsb.dual.utils;

import com.yahoo.ycsb.ByteIterator;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by ubuntu on 10.01.16.
 */
public class Utils {
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

    public static boolean containsBlock(List<BlockResult> blocks, byte[] blockBytes) {
        for (BlockResult res : blocks) {
            if (Arrays.equals(res.getBytes(), blockBytes))
                return true;
        }
        return false;
    }

    public static List<byte[]> blocksToBytes(List<BlockResult> blockResults) {
        List<byte[]> blockBytes = new ArrayList<byte[]>();
        for (BlockResult blockRes : blockResults)
            blockBytes.add(blockRes.getBytes());
        return blockBytes;
    }

}
