package com.yahoo.ycsb.dual;

import com.yahoo.ycsb.ByteIterator;

import java.util.HashMap;

/**
 * Created by ubuntu on 10.01.16.
 */
public class Utils {

    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();

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

}
