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

import com.yahoo.ycsb.utils.communication.ProxyReply;
import com.yahoo.ycsb.utils.properties.PropertyFactory;
import javafx.util.Pair;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LFUCacheManager extends CacheManagerBlueprint {
    public static Logger logger = Logger.getLogger(LFUCacheManager.class);

    private Map<String, Integer> frequency; // number of times each key was requested during the current period
    private Map<String, Double> weightedPopularity; // weighted popularity for all keys seen since the proxy start
    private double alpha; // coefficient for weighted popularity (between 0 and 1)

    private int cacheCapacity; // how many blocks fit in the cache
    private List<CachingOption> cache; // current cache configuration
    private int cachesize;

    private int k; // number of data chunks
    private int m; // number of redundant chunks

    private List<Pair<String, Double>> keys; // all keys seen since proxy start, sorted by popularity
    private int weight;

    private Map<String, CachingOption> cachingOptions; // (key, list of cache options) for all seen keys

    public LFUCacheManager() {
        System.out.println("LFUCacheManager constructor!");
        // erasure-coding params
        k = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        m = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_M_PROPERTY));

        // how many blocks to cache
        weight = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.BLOCKS_IN_CACHE));
        logger.debug("weight: " + weight);

        // cache size (in blocks); assume that 1 item = 1 mb (slab size in memcached)
        cacheCapacity = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.CACHE_SIZE_PROPERTY));

        // init cache, frequency, weighted popularity
        cache = Collections.synchronizedList(new ArrayList<>()); // thread that replies to client + thread that reconfigures cache
        frequency = Collections.synchronizedMap(new HashMap<>()); // thread that counts key acces + thread that reconfigures cache (resets)
        weightedPopularity = new HashMap<>(); // only accessed from thead that reconfigures cache

        // init cache options, max values, and chosen options; only accessed from thead that reconfigures cache
        cachingOptions = new HashMap<>();

        // init alpha, period
        alpha = Double.parseDouble(PropertyFactory.propertiesMap.get(PropertyFactory.ALPHA_PROPERTY));
        int period = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.PERIOD_PROPERTY));
        logger.debug("alpha: " + alpha + " period: " + period);

        // periodic thread that reconfigures cache
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate((Runnable) () -> computeCache(), period, period, TimeUnit.SECONDS);
    }

    // called periodically to reconfigure the cache according to the new access patterns
    private void computeCache() {
        logger.info("computeCache BEGIN");

        // adjust weighted popularity based on frequency, then reset frequency
        prettyPrintFrequency();
        computeWeightedPopularity();
        synchronized (frequency) {
            frequency.clear();
        }
        prettyPrintWeightedPopularity();

        keys = new ArrayList<>();
        for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
            String key = entry.getKey();
            double weightedPop = entry.getValue();
            keys.add(new Pair<>(key, weightedPop));
            //logger.debug("keys.add(" + key + "," + weightedPop + ")");
        }

        if (keys.size() > 0) {
            Collections.sort(keys, (o1, o2) -> o1.getValue().compareTo(o2.getValue()));
            Collections.reverse(keys);
            logger.debug("Keys sorted decreasingly by value: " + keys);

            cachingOptions.clear();

            // compute caching options
            for (Pair<String, Double> entry : keys) {
                String key = entry.getKey();
                double weightedPop = entry.getValue();
                CachingOption coKey = new CachingOption(key, weight, weightedPop);
                cachingOptions.put(key, coKey);
            }
            logger.debug("cachingoptions.size(): " + cachingOptions.size());

            // compute cache
            synchronized (cache) {
                cache.clear();
                cachesize = 0;
            }

            logger.debug("cachesize = " + cacheCapacity);

            for (Pair<String, Double> pair : keys) {
                String key = pair.getKey();

                if (cachesize < cacheCapacity) {
                    cache.add(cachingOptions.get(key));
                    cachesize += weight;
                } else {
                    break;
                }
            }
        }
        prettyPrintCache();

        logger.info("computeCache END");
    }

    private void incrementFrequency(String key) {
        synchronized (frequency) {
            if (frequency.get(key) == null) {
                frequency.put(key, 1);
            } else {
                int freq = frequency.get(key);
                frequency.put(key, freq + 1);
            }
        }
    }

    // for all keys seen since proxy start, compute / adjust the weighted popularity based on the frequency
    // values from the last period
    private void computeWeightedPopularity() {
        for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
            String key = entry.getKey();
            int freq = entry.getValue();

            double oldWeightedPopularity = 0;
            if (weightedPopularity.containsKey(key)) {
                oldWeightedPopularity = weightedPopularity.get(key);
            }

            double newWeightedPopularity = alpha * freq + (1 - alpha) * oldWeightedPopularity;
            weightedPopularity.put(key, newWeightedPopularity);
        }
        // also update weighted popularity for keys that were not encountered over the last period
        for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
            String key = entry.getKey();
            if (!frequency.containsKey(key)) {
                double oldWeightedPopularity = entry.getValue();
                double newWeightedPopularity = (1 - alpha) * oldWeightedPopularity; // + alpha * 0
                weightedPopularity.put(key, newWeightedPopularity);
            }
        }
    }


    private boolean cacheContains(String key) {
        for (CachingOption cachingOption : cache) {
            if (cachingOption.getKey().equals(key))
                return true;
        }
        return false;
    }

    public ProxyReply buildReply(String key) {
        // increment frequency
        incrementFrequency(key);

        synchronized (cache) {
            if (cacheContains(key))
                return new ProxyReply(weight);
            else
                return new ProxyReply(0);
        }
    }

    private void prettyPrintFrequency() {
        logger.info("frequency {");
        for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
            logger.info(entry.getKey() + " " + entry.getValue());
        }
        logger.info("}");
    }

    private void prettyPrintWeightedPopularity() {
        logger.info("weightedPopularity {");
        for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
            logger.info(entry.getKey() + " " + entry.getValue());
        }
        logger.info("}");
    }

    private void prettyPrintCache() {
        logger.info("cache {");
        for (CachingOption co : cache)
            logger.info(co.prettyPrint());
        logger.info("}");
    }
}
