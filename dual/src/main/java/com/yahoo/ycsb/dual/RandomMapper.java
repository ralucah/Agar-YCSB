package com.yahoo.ycsb.dual;

/**
 * Created by Raluca on 19.01.16.
 */

public class RandomMapper extends Mapper {

    @Override
    public int assignToDataCenter(String key) {
        // hashcode is not safe!! collisions will occur
        int toRet = Math.abs(key.hashCode()) % numOfDataCenters;
        //DualClient.logger.debug(key + " to " + toRet);
        return toRet;
    }
}
