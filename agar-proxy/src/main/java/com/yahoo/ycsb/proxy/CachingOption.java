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

import java.util.ArrayList;
import java.util.List;

// a way of caching data and its implications: e.g., cache blocks from a set of regions => resulted weight and value
public class CachingOption implements Comparable<CachingOption> {
    private String key; // data item key
    private int weight; // cache space it takes, in blocks
    private double value; // number of requests over last X minutes * latency improvement
    private List<String> regions; // involves blocks from these regions

    public CachingOption(String key, int weight, double value, List<String> regions) {
        this.key = key;
        this.weight = weight;
        this.value = value;
        this.regions = regions;
    }

    public CachingOption(String key, int weight, double value) {
        this.key = key;
        this.weight = weight;
        this.value = value;
        regions = new ArrayList<>();
    }

    public String getKey() {
        return key;
    }

    public List<String> getRegions() {
        return regions;
    }

    public String prettyPrint() {
        return key + " weight:" + weight + " value:" + value; // + " regions: " + regions;
    }

    public double getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }

    // compare based on value
    @Override
    public int compareTo(CachingOption o) {
        double thisObj = value; // value / weight;
        double otherObj = o.getValue(); // o.getValue() / o.getWeight();
        double diff = thisObj - otherObj;
        if (diff > 0)
            return 1;
        else if (diff < 0)
            return -1;
        return 0;
    }

    // involves key, weight, value
    @Override
    public boolean equals(Object other) {
        CachingOption otherCachingOption = (CachingOption) other;
        if (key.equals(otherCachingOption.getKey()) &&
            weight == otherCachingOption.getWeight() &&
            value == otherCachingOption.getValue())
            return true;
        return false;
    }
}
