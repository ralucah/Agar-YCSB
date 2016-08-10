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

package com.yahoo.ycsb.proxy;

// A region in the system deployment
public class Region implements Comparable<Region> {
    private String name;
    private double latency;
    private String endpoint;
    private int blocks; // how many blocks are stored in this region

    public Region(String name, String endpoint) {
        this.name = name;
        this.endpoint = endpoint;
        blocks = 0;
    }

    public double getLatency() {
        return latency;
    }

    public void setLatency(double latency) {
        this.latency = latency;
    }

    public int getBlocks() {
        return blocks;
    }

    public void setBlocks(int blocks) {
        this.blocks = blocks;
    }

    public void incrementBlocks() {
        blocks++;
    }

    public String prettyPrint() {
        String str = name + " " + (int) latency + " " + blocks;
        return str;
    }

    // Compare regions by latency
    @Override
    public int compareTo(Region o) {
        if (latency > o.getLatency())
            return 1;
        else if (latency < o.getLatency())
            return -1;
        return 0;
    }

    public String getName() {
        return name;
    }
}
