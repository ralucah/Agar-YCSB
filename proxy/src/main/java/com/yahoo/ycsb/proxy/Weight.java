package com.yahoo.ycsb.proxy;

import java.util.List;

public class Weight {
    private int blocks;
    private double latency;
    private List<String> regions;

    public Weight(int blocks, double latency, List<String> regions) {
        this.blocks = blocks;
        this.latency = latency;
        this.regions = regions;
    }

    public int getBlocks() {
        return blocks;
    }

    public List<String> getRegions() {
        return regions;
    }
}
