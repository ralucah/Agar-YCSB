package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class GreedyCacheManager {
    protected static Logger logger = Logger.getLogger(GreedyCacheManager.class);
    private static List<CacheOption> cacheOptions;
    private static List<CacheOption> cache;
    private static Map<String, Integer> frequency;
    private static Map<String, Double> weightedPopularity;
    // gouverns when to recompute cache options
    private static int period;
    private static double alpha;
    private static AtomicInteger cachesizeMax; // in block numbers
    private static AtomicInteger cachesize; // in block numbers
    private static int k;
    private static RegionManager regionManager;

    public GreedyCacheManager() {
        // max cache size in blocks
        k = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        int cachesizeMB = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.CACHE_SIZE_PROPERTY));
        int fieldlength = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.FIELD_LENGTH_PROPERTY));
        int blocksize = fieldlength / k;
        cachesizeMax = new AtomicInteger((cachesizeMB * 1024 * 1024) / blocksize);
        //cachesizeMax = (cachesizeMB * 1024 * 1024) / blocksize;

        // current cache size
        cachesize = new AtomicInteger(0);
        cache = Collections.synchronizedList(new ArrayList<CacheOption>());
        cacheOptions = Collections.synchronizedList(new ArrayList<CacheOption>());

        regionManager = new RegionManager();

        frequency = Collections.synchronizedMap(new HashMap<String, Integer>());
        weightedPopularity = Collections.synchronizedMap(new HashMap<String, Double>());

        period = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.PERIOD_PROPERTY));
        alpha = Double.parseDouble(PropertyFactory.propertiesMap.get(PropertyFactory.ALPHA_PROPERTY));
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                GreedyCacheManager.periodicUpdateThread();
            }
        }, period, period, TimeUnit.MILLISECONDS);
    }

    private static void periodicUpdateThread() {
        System.err.println("periodic thread!");
        // update weighted popularity for keys that were encountered over the last period
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
            if (frequency.containsKey(key) == false) {
                double oldWeightedPopularity = entry.getValue();
                double newWeightedPopularity = (1 - alpha) * oldWeightedPopularity; // + alpha * 0
                weightedPopularity.put(key, newWeightedPopularity);
            }
        }

        // print stats
        printFrequency();
        //printWeightedPopularity();

        // reset frequency
        frequency.clear();

        computeGreedyCache();

        System.out.println("-----------------------------------");
    }

    // dummy
    private static void printFrequency() {
        System.out.println("frequency {");
        for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
        System.out.println("}");
    }

    // dummy
    private static void printWeightedPopularity() {
        System.out.println("weightedPopularity {");
        for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
            System.out.println(entry.getKey() + " " + entry.getValue());
        }
        System.out.println("}");
    }

    private static void computeGreedyCache() {
        // compute cache options
        computeCacheOptions();

        // update current cache according to new options
        //System.out.println("Current options, sorted:");
        //printCacheOptions(cacheOptions);
        //System.out.println("Current cache:");
        //printCacheOptions(cache);

        // populate the cache!
        cachesize.set(0);
        cache.clear();
        int index = 0;
        while (cachesize.intValue() < cachesizeMax.intValue() && index < cacheOptions.size()) {
            // get cache option with highest ratio
            CacheOption best = cacheOptions.get(index);
            if (cachesize.intValue() + best.getWeight() <= cachesizeMax.intValue() && cacheContains(best.getKey()) == -1) {
                cache.add(best);
                cachesize.addAndGet(best.getWeight());
            }
            index++;
        }

        System.out.println("Cache:");
        printCacheOptions(cache);
        System.out.println("cachesize:" + cachesize + " cachesizemax:" + cachesizeMax);
    }

    private static int cacheContains(String key) {
        int index = 0;
        for (CacheOption cacheOption : cache) {
            if (cacheOption.getKey().equals(key))
                return index;
            index++;
        }
        return -1;
    }

    private static boolean isKeyInCacheOptions(String key) {
        for (CacheOption cacheOption : cacheOptions) {
            if (cacheOption.getKey().equals(key))
                return true;
        }
        return false;
    }

    private static void computeCacheOptions() {
        cacheOptions.clear();
        for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
            String key = entry.getKey();
            //if (isKeyInCacheOptions(key) == false) {
            List<CacheOption> cacheOptionsKey = generateCacheOptions(key);
            cacheOptions.addAll(cacheOptionsKey);
            //}
        }

        cacheOptions.sort(CacheOption::compareTo);
        Collections.reverse(cacheOptions);

        System.out.println("All cache options:");
        printCacheOptions(cacheOptions);
    }

    private static List<CacheOption> generateCacheOptions(String key) {
        List<CacheOption> cacheOptionsKey = new ArrayList<CacheOption>();
        int blocks = 0;
        double latency = 0;

        for (int i = 0; i < regionManager.getRegions().size(); i++) {
            Region region = regionManager.getRegions().get(i);
            latency = region.getLatency();
            blocks += region.getBlocks();

            // value should be latency save * weighted popularity
            double value = 0;
            if (i < regionManager.getRegions().size() - 1)
                value = regionManager.getLatencyMax() - regionManager.getRegions().get(i + 1).getLatency();
            else
                value = regionManager.getLatencyMax();
            if (weightedPopularity.containsKey(key) == true)
                value *= weightedPopularity.get(key);
            if (blocks >= k) {
                CacheOption option = new CacheOption(key, k, value);
                cacheOptionsKey.add(option);
                break;
            } else {
                CacheOption option = new CacheOption(key, blocks, value);
                cacheOptionsKey.add(option);
            }
        }
        //System.out.println("CacheOptions for " + key);
        //printCacheOptions(cacheOptionsKey);
        return cacheOptionsKey;
    }

    private static void printCacheOptions(List<CacheOption> options) {
        for (CacheOption cacheOption : options)
            System.out.println(cacheOption.prettyPrint());
    }

    private void incrementFrequency(String key) {
        if (frequency.get(key) == null) {
            frequency.put(key, 1);
        } else {
            int freq = frequency.get(key);
            frequency.put(key, freq + 1);
        }
    }

    public int getCachedBlocks(String key) {
        incrementFrequency(key);
        int blocks = 0;

        synchronized (cache) {
            int index = cacheContains(key);
            if (index != -1)
                blocks = cache.get(index).getBlocks();
        }
        return blocks;
    }
}
