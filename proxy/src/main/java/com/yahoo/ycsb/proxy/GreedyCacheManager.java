package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class GreedyCacheManager {
    protected static Logger logger = Logger.getLogger(GreedyCacheManager.class);
    private static List<CacheOption> cacheOptions;
    private static List<CacheOption> cache;
    private static Map<String, Integer> frequency;
    private static Map<String, Double> weightedPopularity;
    // gouverns when to recompute cache options
    private static int period;
    private static double alpha;
    private final int cachesizeMax; // in block numbers
    private int cachesize; // in block numbers
    private int k;
    private RegionManager regionManager;

    public GreedyCacheManager() {
        // max cache size in blocks
        k = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        int cachesizeMB = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.CACHE_SIZE_PROPERTY));
        int fieldlength = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.FIELD_LENGTH_PROPERTY));
        int blocksize = fieldlength / k;
        cachesizeMax = (cachesizeMB * 1024 * 1024) / blocksize;

        // current cache size
        cachesize = 0;
        cache = new ArrayList<CacheOption>();
        cacheOptions = new ArrayList<CacheOption>();

        regionManager = new RegionManager();

        frequency = new HashMap<String, Integer>();
        weightedPopularity = new HashMap<String, Double>();

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
        printWeightedPopularity();

        // reset frequency
        frequency = new HashMap<String, Integer>();

        System.out.println("-----------------------------------");
        //printFrequency();
        //printWeightedPopularity();

        // generate cache options based on this new info!!!
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

    private void incrementFrequency(String key) {
        if (frequency.get(key) == null) {
            frequency.put(key, 1);
        } else {
            int freq = frequency.get(key);
            frequency.put(key, freq + 1);
        }
    }

    private void updateGreedyCache() {
        // compute cache options
        cacheOptions = updateCacheOptions();

        // sort by value (in decreasing order)
        //System.out.println("Before sort:");
        //printCacheOptions(cacheOptions);
        cacheOptions.sort(CacheOption::compareTo);
        Collections.reverse(cacheOptions);
        //System.out.println("After sort:");
        //printCacheOptions(cacheOptions);

        // update current cache according to new options
        
    }

    /*public void process(String key) {
        incrementFrequency(key);


        // if this key has a current cache option -> stick to it or update it?
    }*/

    private boolean isKeyInCacheOptions(String key) {
        for (CacheOption cacheOption : cacheOptions) {
            if (cacheOption.getKey().equals(key))
                return true;
        }
        return false;
    }

    private List<CacheOption> updateCacheOptions() {
        for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
            String key = entry.getKey();
            if (isKeyInCacheOptions(key) == false) {
                List<CacheOption> cacheOptionsKey = generateCacheOptions(key);
                cacheOptions.addAll(cacheOptionsKey);
            }
        }
        System.out.println("All cache options:");
        printCacheOptions(cacheOptions);
        return cacheOptions;
    }

    private List<CacheOption> generateCacheOptions(String key) {
        List<CacheOption> cacheOptionsKey = new ArrayList<CacheOption>();
        int blocks = 0;
        double latency = 0;

        for (Region region : regionManager.getRegions()) {
            latency += region.getLatency();
            blocks += region.getBlocks();

            // value should be latency save * weighted popularity
            double value = regionManager.getLatencyMax() - latency;
            if (weightedPopularity.containsKey(key) == true)
                value *= weightedPopularity.get(key);
            if (blocks > k) {
                CacheOption option = new CacheOption(key, k, value);
                cacheOptionsKey.add(option);
                break;
            } else {
                CacheOption option = new CacheOption(key, blocks, value);
                cacheOptionsKey.add(option);
            }
        }
        System.out.println("CacheOptions for " + key);
        printCacheOptions(cacheOptionsKey);
        return cacheOptionsKey;
    }

    /*public void generateCacheOptions(String key) {
        int blocks = 0;
        double latency = 0;

        for (Region region : regionManager.getRegions()) {
            latency += region.getLatency();
            blocks += region.getBlocks();

            // value should be latency save * weighted popularity
            double value =  regionManager.getLatencyMax() - latency;
            if (weightedPopularity.containsKey(key) == true)
                value *= weightedPopularity.get(key);
            if (blocks > k) {
                CacheOption copt = new CacheOption(key, k, value);
                if (allCacheOptions.contains(copt) == false)
                    allCacheOptions.add(copt);
                break;
            } else {
                CacheOption copt = new CacheOption(key, blocks, value);
                if (allCacheOptions.contains(copt) == false)
                    allCacheOptions.add(copt);
            }
        }
        /*for(CacheOption cacheOption : allCacheOptions)
            System.out.println(cacheOption.prettyPrint());*/
    //}

    public void printCacheOptions(List<CacheOption> options) {
        for (CacheOption cacheOption : options)
            System.out.println(cacheOption.prettyPrint());
    }

    public void removeAlternatives(CacheOption selectedCacheOption) {
        System.out.println("Before removeAlternatives:");
        printCacheOptions(allCacheOptions);
        for (int i = 0; i < allCacheOptions.size(); i++) {
            CacheOption cacheOption = allCacheOptions.get(i);
            if (cacheOption.getKey().equals(selectedCacheOption.getKey()) &&
                cacheOption.equals(selectedCacheOption) == false)
                allCacheOptions.remove(i);
        }
        System.out.println("After removeAlternatives:");
        printCacheOptions(allCacheOptions);
    }

    /*private void computeIdealCache(List<CacheOption> cacheOptions) {
        List<CacheOption> idealCache = new ArrayList<CacheOption>();

        // while still have space in cache
        int index = 0;
        while (cachesize < cachesizeMax && index < cacheOptions.size()) {
            // get cache option with highest ratio
            CacheOption best = cacheOptions.get(index);
            if (cachesize + best.getWeight() <= cachesizeMax) {
                selectedCacheOptions.add(bestOption);
                cachesize += bestOption.getWeight();
            }
            // remove other cache options for that key
            removeAlternatives(bestOption);
            index++;
        }
        System.out.println("Selected cache options:");
        printCacheOptions(selectedCacheOptions);
        System.out.println("cachesize:" + cachesize);
    }*/

    public int getCachedBlocks(String key) {
        incrementFrequency(key);

        for (CacheOption copt : selectedCacheOptions) {
            if (copt.getKey().equals(key)) {
                blocks = copt.getWeight(); // weight is num blocks atm
                break;
            }
        }
        return blocks;
    }
}
