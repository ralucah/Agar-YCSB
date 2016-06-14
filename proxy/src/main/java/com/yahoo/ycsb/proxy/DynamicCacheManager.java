package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.common.communication.ProxyReply;
import javafx.util.Pair;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class DynamicCacheManager {
    public static Logger logger = Logger.getLogger(DynamicCacheManager.class);

    private final int period; // how often to recompute cache (ms)

    private Map<String, Integer> frequency; // hnumber of times each key was requested during the current period
    private Map<String, Double> weightedPopularity; // weighted popularity for all keys seen since the proxy start
    private double alpha; // coefficient for weighted popularity (between 0 and 1)

    private int cacheCapacity; // how many blocks fit in the cache
    private List<CachingOption> cache; // current cache configuration

    private List<Region> regions; // regions in which the system is deployed, in increasing order of wget latency
    private int k; // number of data chunks
    private int m; // number of redundant chunks

    private List<Pair<String, Double>> keys; // all keys seen since proxy start, sorted by value
    private int[] weights; // weight options for the current deployment (weight increment = number of blocks in region)

    private Map<String, Map<Integer, CachingOption>> cachingOptions; // (key, list of cache options) for all seen keys
    private Map<Integer, Double> maxValue; // max value for each weight computed based on the caching options
    private Map<Integer, List<CachingOption>> chosenOptions; // for each weight, list of caching options that result in max value

    public DynamicCacheManager() {
        // erasure-coding params
        k = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        m = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_M_PROPERTY));

        // overview of system deployment; regions are sorted by latency, in increasing order
        RegionManager regionManager = new RegionManager();
        weights = regionManager.getWeights();
        regions = regionManager.getRegions();

        // cache size (in blocks); assume that 1 item = 1 mb (slab size in memcached)
        cacheCapacity = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.CACHE_SIZE_PROPERTY));

        // init cache, frequency, weighted popularity
        cache = Collections.synchronizedList(new ArrayList<CachingOption>()); // thread that replies to client + thread that reconfigures cache
        frequency = Collections.synchronizedMap(new HashMap<String, Integer>()); // thread that counts key acces + thread that reconfigures cache (resets)
        weightedPopularity = new HashMap<String, Double>(); // only accessed from thead that reconfigures cache

        // init cache options, max values, and chosen options; only accessed from thead that reconfigures cache
        cachingOptions = new HashMap<String, Map<Integer, CachingOption>>();
        maxValue = new HashMap<Integer, Double>();
        chosenOptions = new HashMap<Integer, List<CachingOption>>();

        // init alpha, period
        alpha = Double.parseDouble(PropertyFactory.propertiesMap.get(PropertyFactory.ALPHA_PROPERTY));
        period = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.PERIOD_PROPERTY));
        logger.debug("alpha: " + alpha + " period: " + period);

        // periodic thread that reconfigures cache
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                computeCache();
            }
        }, period, period, TimeUnit.SECONDS);
    }

    // called periodically to reconfigure the cache according to the new access patterns
    private void computeCache() {
        logger.debug("computeCache BEGIN");

        // adjust weighted popularity based on frequency, then reset frequency
        prettyPrintFrequency();
        computeWeightedPopularity();
        frequency.clear();
        prettyPrintWeightedPopularity();

        if (weightedPopularity.size() > 0) {
            // compute caching options: for each known key, compute the set of possible caching options and build a total caching options set
            // compute known keys set: for each key add the value of the first caching option
            cachingOptions.clear();
            keys = new ArrayList<Pair<String, Double>>();
            for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
                String key = entry.getKey();
                Map<Integer, CachingOption> cachingOptionsKey = computeCachingOptionsForKey(key);
                cachingOptions.put(key, cachingOptionsKey);

                CachingOption first = cachingOptionsKey.entrySet().iterator().next().getValue();
                keys.add(new Pair<>(key, first.getValue()));
                logger.debug("keys.add(" + key + "," + first.getValue() + ")");
            }
            logger.debug("Keys: " + keys);

            // sort keys decreasingly by value
            Collections.sort(keys, new Comparator<Pair<String, Double>>() {
                @Override
                public int compare(final Pair<String, Double> o1, final Pair<String, Double> o2) {
                    return o1.getValue().compareTo(o2.getValue());
                }
            });
            logger.debug("Keys sorted increasingly by value: " + keys);
            Collections.reverse(keys);
            logger.debug("Keys sorted decreasingly by value: " + keys);

            // knapsack solution using dynamic programming
            computeChosenOptions();

            // compute cache
            logger.debug("cachesize = " + cacheCapacity);
            for (int i = cacheCapacity; i >= 0; i--) {
                if (chosenOptions.get(i) != null) {
                    cache = Collections.synchronizedList(chosenOptions.get(i));
                    logger.debug("chosen cachesize = " + i);
                    break;
                }
            }
            prettyPrintCache();
        }
        logger.debug("computeCache END");
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
            if (frequency.containsKey(key) == false) {
                double oldWeightedPopularity = entry.getValue();
                double newWeightedPopularity = (1 - alpha) * oldWeightedPopularity; // + alpha * 0
                weightedPopularity.put(key, newWeightedPopularity);
            }
        }
    }

    /**
     * Compute caching options for a given key
     *
     * @param key for which to compute caching options
     * @return a set of caching options + their weights (in number of blocks)
     */
    private Map<Integer, CachingOption> computeCachingOptionsForKey(String key) {
        Map<Integer, CachingOption> cacheOptionsKey = new HashMap<Integer, CachingOption>();

        // attributes that define a caching option
        int blocks = 0; // how many blocks to cache
        List<String> regionNames = new ArrayList<String>(); // from which regions to cache blocks
        double latency = 0; // the latency saved by not accessing these regions

        // regions are in increasing order of wget latency
        int myRegionId = 0;
        int numBlocksInMyRegion = regions.get(myRegionId).getBlocks();
        int crtRegionId = regions.size() - 1;

        // compute which region to start from (when caching, priority is given to the blocks from the most distant region)
        int avoidedBlocks = 0;
        while (avoidedBlocks + regions.get(crtRegionId).getBlocks() < m) {
            crtRegionId--;
            avoidedBlocks += regions.get(crtRegionId).getBlocks();
        }

        int furthestCachedRegion = crtRegionId;
        while (crtRegionId > 0 && blocks + numBlocksInMyRegion < k) {
            //System.out.println(key + " " + blocks);
            Region region = regions.get(crtRegionId);
            //latency = region.getLatency();
            blocks += region.getBlocks();
            regionNames.add(region.getName());

            // value should be latency save * weighted popularity
            double value = regions.get(furthestCachedRegion).getLatency() - regions.get(crtRegionId - 1).getLatency();
            //System.out.println(key + " " + blocks + " " + value);
            if (weightedPopularity.containsKey(key) == true) {
                value *= weightedPopularity.get(key);
            }
            CachingOption option = new CachingOption(key, blocks, value, new ArrayList<String>(regionNames));
            cacheOptionsKey.put(blocks, option);
            crtRegionId--;
        }
        // add my region to cache
        regionNames.add(regions.get(myRegionId).getName());
        double value = regions.get(furthestCachedRegion).getLatency();
        //System.out.println(key + " " + k + " " + value);
        if (weightedPopularity.containsKey(key) == true) {
            value *= weightedPopularity.get(key);
        }
        blocks += regions.get(myRegionId).getBlocks();
        CachingOption option = new CachingOption(key, blocks, value, new ArrayList<String>(regionNames));
        cacheOptionsKey.put(blocks, option);

        logger.debug("CacheOptions for " + key);
        for (Map.Entry<Integer, CachingOption> entry : cacheOptionsKey.entrySet())
            logger.debug(entry.getKey() + " " + entry.getValue().prettyPrint());

        return cacheOptionsKey;
    }

    private void computeChosenOptions() {
        // clear
        maxValue.clear();
        chosenOptions.clear();

        // start by computing the max value entries for the first key
        Map<Integer, CachingOption> cacheOptionsKey1 = cachingOptions.get(keys.get(0).getKey());
        for (int weight : weights) {
            maxValue.put(weight, cacheOptionsKey1.get(weight).getValue());
            List<CachingOption> chosenCachingOptions = new ArrayList<CachingOption>();
            ;
            if (chosenOptions.get(weight) != null) {
                chosenCachingOptions = chosenOptions.get(weight);
            }
            chosenCachingOptions.add(cacheOptionsKey1.get(weight));
            chosenOptions.put(weight, chosenCachingOptions);
        }

        printMaxValue();
        printChosenOptions();

        // get max weight
        int maxWeight = weights[weights.length - 1];
        logger.debug("maxWeight=" + maxWeight);

        // TODO estimate number of keys
        int numKeys = cacheCapacity;
        if (numKeys > keys.size())
            numKeys = keys.size();
        logger.debug("numKeys=" + numKeys);

        // TODO keys.length -> numKeys
        for (int i = 1; i < keys.size(); i++) {
            // we need to update max weight only at the end of this for loop
            int newMaxWeight = maxWeight;

            // check cache options for this key
            Map<Integer, CachingOption> cacheOptionsKey = cachingOptions.get(keys.get(i).getKey());
            for (int j = weights.length - 1; j >= 0; j--) {
                CachingOption co = cacheOptionsKey.get(weights[j]);
                //int crtWeight = co.getWeight();

                // iterate through possible weights and try to relax a previous choice
                for (int weight = weights[weights.length - 1]; weight >= 0; weight--) {
                    // see if we can improve the last options by replacing a larger cacheoption with two smaller ones
                    int weightToRelax = maxWeight - weight;
                    if (!maxValue.containsKey(weightToRelax))
                        continue;
                    double prevValue = maxValue.get(weightToRelax);

                    // get cacheoption at the end of the list and try to split it
                    int last = chosenOptions.get(weightToRelax).size() - 1;
                    CachingOption lastAddedCachingOption = chosenOptions.get(weightToRelax).get(last);

                    // do not relax our own key
                    if (lastAddedCachingOption.getKey() == co.getKey())
                        continue;

                    if (lastAddedCachingOption.getWeight() > co.getWeight()) {
                        // we replace the lastAddedCacheOption with a smaller weight option and fill in the remaining space with the current cacheoption
                        double newValue = prevValue -
                            lastAddedCachingOption.getValue() +
                            co.getValue() +
                            cachingOptions.get(lastAddedCachingOption.getKey()).get(lastAddedCachingOption.getWeight() - co.getWeight()).getValue();

                        // if a better combination of cache options exists, then update the chosen options list (relaxation step)
                        if (newValue > prevValue) {
                            maxValue.put(weightToRelax, newValue);
                            chosenOptions.get(weightToRelax).remove(last);
                            chosenOptions.get(weightToRelax).add(last, cachingOptions.get(lastAddedCachingOption.getKey()).get(lastAddedCachingOption.getWeight() - co.getWeight()));
                            chosenOptions.get(weightToRelax).add(last + 1, co);

                        }
                    }
                }

                //try to add new co at the end
                for (int weight = weights[weights.length - 1]; weight >= 0; weight--) {
                    int weightToAddTo = maxWeight - weight;

                    if (!maxValue.containsKey(weightToAddTo))
                        continue;

                    int newWeight = weightToAddTo + co.getWeight();

                    if (newWeight > newMaxWeight)
                        newMaxWeight = newWeight;

                    if (maxValue.containsKey(newWeight)) {
                        double prevValue = maxValue.get(newWeight);

                        // we add co at the end of weightToAddTo
                        double newValue = maxValue.get(weightToAddTo) + co.getValue();

                        // if a better combination of cache options exists, then update the chosen options list (relaxation step)
                        if (newValue > prevValue) {
                            if (!cacheOptionsContainsKey(chosenOptions.get(weightToAddTo), co.getKey())) {
                                maxValue.put(newWeight, newValue);
                                chosenOptions.put(newWeight, new ArrayList<CachingOption>(chosenOptions.get(weightToAddTo)));
                                chosenOptions.get(newWeight).add(co);
                            }
                        }
                    } else {
                        if (!cacheOptionsContainsKey(chosenOptions.get(weightToAddTo), co.getKey())) {
                            double newValue = maxValue.get(weightToAddTo) + co.getValue();
                            maxValue.put(newWeight, newValue);
                            chosenOptions.put(newWeight, new ArrayList<CachingOption>(chosenOptions.get(weightToAddTo)));
                            chosenOptions.get(newWeight).add(co);
                        }
                    }
                }
            }
            maxWeight = newMaxWeight;

            // after this point the algorithm will never update maxValue[cacheCapacity]
            // which means that we have found the best value
            if (maxWeight > cacheCapacity + 2 * weights[weights.length - 1])
                break;
        }
        printChosenOptions();
    }

    private boolean cacheOptionsContainsKey(List<CachingOption> chosenOptions, String key) {
        for (CachingOption co : chosenOptions)
            if (co.getKey().equals(key)) {
                return true;
            }
        return false;
    }

    // handle client requests /////////////////////////////////////////////////////////////
    private void incrementFrequency(String key) {
        if (frequency.get(key) == null) {
            frequency.put(key, 1);
        } else {
            int freq = frequency.get(key);
            frequency.put(key, freq + 1);
        }
    }

    private int cacheContains(String key) {
        int index = 0;
        for (CachingOption cachingOption : cache) {
            if (cachingOption.getKey().equals(key))
                return index;
            index++;
        }
        return -1;
    }

    public ProxyReply buildReply(String key) {
        // increment frequency
        incrementFrequency(key);

        // build reply based on current cache configuration
        int blocksCache = 0;
        ProxyReply reply = new ProxyReply();

        // compute cache recipe
        // synchronized because the cache reconfiguration thread might try to modify cache concurrently
        synchronized (cache) {
            int index = cacheContains(key);
            if (index != -1) {
                blocksCache = cache.get(index).getWeight();
                reply.setCacheRecipe(cache.get(index).getRegions());
            }
        }

        // compute backend recipe
        int blocksBackend = k - blocksCache;
        int regionId = regions.size() - 1;
        while (blocksBackend > 0 && regionId >= 0) {
            Region region = regions.get(regionId);
            reply.addToBackendRecipe(region.getName());
            blocksBackend -= region.getBlocks();
            regionId--;
        }

        //logger.debug(reply.prettyPrint());
        return reply;
    }

    // pretty prints //////////////////////////////////////////////////////////////
    private void prettyPrintFrequency() {
        logger.debug("frequency {");
        for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
            logger.debug(entry.getKey() + " " + entry.getValue());
        }
        logger.debug("}");
    }

    private void prettyPrintWeightedPopularity() {
        logger.debug("weightedPopularity {");
        for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
            logger.debug(entry.getKey() + " " + entry.getValue());
        }
        logger.debug("}");
    }

    private void prettyPrintCache() {
        logger.debug("cache {");
        for (CachingOption co : cache)
            logger.debug(co.prettyPrint());
        logger.debug("}");
    }

    private void printChosenOptions() {
        logger.debug("chosenOptions {");
        for (Map.Entry<Integer, List<CachingOption>> entry : chosenOptions.entrySet()) {
            String str = entry.getKey().toString();
            for (CachingOption co : entry.getValue())
                str += " " + co.prettyPrint();
            logger.debug(str);
        }
        logger.debug("}");
    }

    private void printMaxValue() {
        logger.debug("maxValue {");
        for (Map.Entry<Integer, Double> entry : maxValue.entrySet()) {
            logger.debug(entry.getKey() + " " + entry.getValue());
        }
        logger.debug("}");
    }
}
