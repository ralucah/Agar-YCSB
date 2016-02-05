package com.yahoo.ycsb.dual.utils;

/**
 * Created by ubuntu on 02.02.16.
 */
public abstract class Region implements Comparable {

    protected double avgPingTime;

    protected Region() {
        avgPingTime = Double.MIN_VALUE;
    }

    public abstract void print();

    public double getAvgPingTime() {
        return avgPingTime;
    }

    public void setAvgPingTime(double avgPingTime) {
        this.avgPingTime = avgPingTime;
    }

    @Override
    public int compareTo(Object other) {
        Region otherRegion = (Region) other;

        if (this.avgPingTime == otherRegion.getAvgPingTime())
            return 0;
        else if (this.avgPingTime < otherRegion.getAvgPingTime())
            return -1;

        return 1;
    }

}
