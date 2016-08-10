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

package com.yahoo.ycsb.client.utils;

import com.yahoo.ycsb.ByteIterator;
import org.apache.log4j.Logger;

import java.util.*;

public abstract class ClientUtils {
    final protected static char[] hexArray = "0123456789ABCDEF".toCharArray();
    private static Logger logger = Logger.getLogger(ClientUtils.class);

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
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    // hashcode of null is 0
    public static int bytesToHash(byte[] data) {
        return java.util.Arrays.hashCode(data);
    }

    public static List<byte[]> resultsToBytes(List<ReadResult> readResults) {
        List<byte[]> blockBytes = new ArrayList<byte[]>();
        for (ReadResult blockRes : readResults)
            blockBytes.add(blockRes.getBytes());
        return blockBytes;
    }

    public static boolean readResultsContains(List<ReadResult> readResults, String key) {
        for (ReadResult result : readResults) {
            if (result.getKey().equals(key))
                return true;
        }
        return false;
    }

    public static String getBaseKey(String blockKey) {
        return blockKey.substring(0, blockKey.length() - 1);
    }

    public static List<ReadResult> blocksToReadResults(String key, List<byte[]> blocks) {
        List<ReadResult> readResults = new ArrayList<ReadResult>();
        int counter = 0;
        for (byte[] block : blocks) {
            readResults.add(new ReadResult(key + counter, block));
            counter++;
        }
        return readResults;
    }

    public static String toCacheToString(Map<String, String> toCache) {
        String str = "";
        Iterator iter = toCache.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<String, String> pair = (Map.Entry<String, String>) iter.next();
            str += pair.getKey() + ":" + pair.getValue() + " ";
        }
        return str;
    }

    public static String readResultsToString(List<ReadResult> readResults) {
        String str = "";
        for (ReadResult readResult : readResults) {
            str += readResult.getKey() + " ";
        }
        return str;
    }

    /*public List<byte[]> readResultsToBlocks(List<ReadResult> readResults) {
        List<byte[]> blocks = new ArrayList<byte[]>();
        for (ReadResult result : readResults) {
            blocks.add(result.getBytes());
        }
        return blocks;
    }*/

    /*public static List<String> computeBlockKeys(String key, int numBlocks) {
        List<String> blockKeys = new ArrayList<String>();
        for (int i = 0; i < numBlocks; i++) {
            blockKeys.add(key + i);
        }
        return blockKeys;
    }*/

    /*public static byte[] listToBytes(List<String> list) {
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
    }*/

    /*public static List<String> bytesToList(byte[] bytes) {
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
    }*/

    /*public static int computeBlockId(String key) {
        return Integer.parseInt(key.substring(key.length() - 1));
    }*/
}
