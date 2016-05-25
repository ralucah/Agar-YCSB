package com.yahoo.ycsb.proxy;

import java.util.List;

public class CacheOption implements Comparable<CacheOption> {
    private String key;
    private int weight; // = space in cache (in num blocks)
    private double value; // = num of requests over last X minutes * latency improvement
    private List<String> regions;

    public CacheOption(String key, int weight, double value, List<String> regions) {
        this.key = key;
        this.weight = weight;
        this.value = value;
        this.regions = regions;
    }

    public String getKey() {
        return key;
    }

    public List<String> getRegions() {
        return regions;
    }

    public String prettyPrint() {
        //String str = key + " blocks:" + blocks + " weight:" + weight + " value:" + value + " value/weight:" + value / weight + " regions:";
        String str = key + " weight:" + weight + " value:" + value + " regions: " + regions;
        return str;
    }

    public double getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }

    /**
     * @param o
     * @return < 0, if crt object < o, > 0, if crt object > o, 0 if crt object = o
     */
    @Override
    public int compareTo(CacheOption o) {
        double thisObj = value; // value / weight;
        double otherObj = o.getValue(); // o.getValue() / o.getWeight();
        double diff = thisObj - otherObj;
        if (diff > 0)
            return 1;
        else if (diff < 0)
            return -1;
        return 0;
    }

    @Override
    public boolean equals(Object other) {
        CacheOption otherCacheOption = (CacheOption) other;
        if (key.equals(otherCacheOption.getKey()) &&
            weight == otherCacheOption.getWeight() &&
            value == otherCacheOption.getValue())
            return true;
        return false;
    }
}
