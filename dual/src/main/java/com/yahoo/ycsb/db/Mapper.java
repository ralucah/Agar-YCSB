package com.yahoo.ycsb.db;

/**
 * Created by Raluca on 27.12.15.
 */
public class Mapper {

    public static int mapKeyToBucket(String key, int bucketsNum) {
        // hashcode is not safe!! collisions will occur
        return Math.abs(key.hashCode()) % bucketsNum;
    }
}
