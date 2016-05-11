package com.yahoo.ycsb.proxy;

public class CacheOption implements Comparable<CacheOption> {
    private String key;
    // say which blocks to cache or only how many blocks to cache?
    // if i say which blocks to cache => C(k,1) + C(k, 2) + C(k, 3) + ... + C(k,k)
    // => search space grows
    // so keep the number of blocks, or somehow decide on some distant blocks
    private int blocks;
    private int weight; // = space in cache (in num blocks)
    private double value; // = num of requests over last X minutes * latency improvement
    // estimated latency improvement computed based on known distances between regions

    // how to set value and weight?

    public CacheOption(String key, int blocks, double value) {
        this.key = key;
        this.blocks = blocks;
        weight = blocks;
        this.value = value;
    }

    public String prettyPrint() {
        String str = key + " blocks:" + blocks + " weight:" + weight + " value:" + value + " value/weight:" + value / weight;
        return str;
    }

    public double getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }

    public String getKey() {
        return key;
    }

    public int getBlocks() {
        return blocks;
    }

    /**
     * @param o
     * @return < 0, if crt object < o, > 0, if crt object > o, 0 if crt object = o
     */
    @Override
    public int compareTo(CacheOption o) {
        double thisObj = value / weight;
        double otherObj = o.getValue() / o.getWeight();
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
            blocks == otherCacheOption.getBlocks())
            return true;
        return false;
    }
}
