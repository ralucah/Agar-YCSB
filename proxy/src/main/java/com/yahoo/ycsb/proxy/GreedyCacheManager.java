package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class GreedyCacheManager {
    protected static Logger logger = Logger.getLogger(GreedyCacheManager.class);
    private final int cachesizeMax; // in block numbers
    private List<CacheOption> allCacheOptions;
    private List<CacheOption> selectedCacheOptions;
    private int cachesize; // in block numbers
    private int k;

    private RegionManager regionManager;


    // cache registry representation?

    public GreedyCacheManager() {
        // max cache size in blocks
        k = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        int cachesizeMB = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.CACHE_SIZE_PROPERTY));
        int fieldlength = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.FIELD_LENGTH_PROPERTY));
        int blocksize = fieldlength / k;
        cachesizeMax = (cachesizeMB * 1024 * 1024) / blocksize;

        // current cache size
        cachesize = 0;

        allCacheOptions = new ArrayList<CacheOption>();
        selectedCacheOptions = new ArrayList<CacheOption>();

        regionManager = new RegionManager();
    }

    public synchronized void generateCacheOptions(String key) {
        int blocks = 0;
        double latency = 0;

        for (Region region : regionManager.getRegions()) {
            latency += region.getLatency();
            blocks += region.getBlocks();

            if (blocks > k) {
                CacheOption copt = new CacheOption(key, k, regionManager.getLatencyMax() - latency);
                if (allCacheOptions.contains(copt) == false)
                    allCacheOptions.add(copt);
                break;
            } else {
                CacheOption copt = new CacheOption(key, blocks, regionManager.getLatencyMax() - latency);
                if (allCacheOptions.contains(copt) == false)
                    allCacheOptions.add(copt);
            }
        }
        /*for(CacheOption cacheOption : allCacheOptions)
            System.out.println(cacheOption.prettyPrint());*/
    }

    public void printCacheOptions(List<CacheOption> options) {
        for (CacheOption cacheOption : options)
            System.out.println(cacheOption.prettyPrint());
    }

    public synchronized void sortCacheOptions() {
        System.out.println("Before sort:");
        printCacheOptions(allCacheOptions);
        allCacheOptions.sort(CacheOption::compareTo);
        Collections.reverse(allCacheOptions);
        System.out.println("After sort:");
        printCacheOptions(allCacheOptions);
    }

    public synchronized void removeAlternatives(CacheOption selectedCacheOption) {
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

    public synchronized void getBestOptions() {
        // sort cache options by value/weight ratio
        sortCacheOptions();
        // while still have space in cache
        int index = 0;
        while (cachesize < cachesizeMax && index < allCacheOptions.size()) {
            // get cache option with highest ratio
            CacheOption bestOption = allCacheOptions.get(index);
            if (cachesize + bestOption.getWeight() <= cachesizeMax) {
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
    }

    public int getCachedBlocks(String key) {
        generateCacheOptions(key);
        getBestOptions();
        int blocks = 0;
        for (CacheOption copt : selectedCacheOptions) {
            if (copt.getKey().equals(key)) {
                blocks = copt.getWeight(); // weight is num blocks atm
                break;
            }
        }
        return blocks;
    }
}
