package com.yahoo.ycsb.dual;

/**
 * Created by Raluca on 27.12.15.
 */
public class Mapper {

    public static int mapKeyToDatacenter(String key, int bucketsNum) {
        // hashcode is not safe!! collisions will occur
        int toRet = Math.abs(key.hashCode()) % bucketsNum;
        //DualClient.logger.debug(key + " to " + toRet);
        return toRet;
    }
}
