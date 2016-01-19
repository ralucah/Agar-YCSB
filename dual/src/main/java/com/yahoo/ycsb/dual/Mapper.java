package com.yahoo.ycsb.dual;

/**
 * Created by Raluca on 27.12.15.
 */
public abstract class Mapper {
    protected int numOfDataCenters;

    public void setNumOfDataCenters(int numOfDataCenters) {
        this.numOfDataCenters = numOfDataCenters;
    }

    public abstract int assignToDataCenter(String key);
}
