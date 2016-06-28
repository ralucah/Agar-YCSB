package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.common.communication.ProxyReply;
import com.yahoo.ycsb.common.properties.PropertyFactory;
import javafx.util.Pair;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class LFUCacheManager extends CacheManagerBlueprint {
    public static Logger logger = Logger.getLogger(LFUCacheManager.class);

    private Map<String, Integer> frequency; // number of times each key was requested during the current period

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

    public LFUCacheManager() {
        System.out.println("LFUCacheManager constructor!");
        // erasure-coding params
        k = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        m = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_M_PROPERTY));

        // overview of system deployment; regions are sorted by latency, in increasing order
        RegionManager regionManager = new RegionManager();
        weights = regionManager.getWeights();
        logger.debug("weights: " + Arrays.toString(weights));
        regions = regionManager.getRegions();

        // cache size (in blocks); assume that 1 item = 1 mb (slab size in memcached)
        cacheCapacity = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.CACHE_SIZE_PROPERTY));

        // init cache, frequency, weighted popularity
        cache = Collections.synchronizedList(new ArrayList<>()); // thread that replies to client + thread that reconfigures cache
        frequency = Collections.synchronizedMap(new HashMap<>()); // thread that counts key acces + thread that reconfigures cache (resets)

        // init cache options, max values, and chosen options; only accessed from thead that reconfigures cache
        cachingOptions = new HashMap<>();
        maxValue = new HashMap<>();
        chosenOptions = new HashMap<>();

        // init period
        int period = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.PERIOD_PROPERTY));
        logger.debug("period: " + period);

        // periodic thread that reconfigures cache
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate((Runnable) () -> computeCache(), period, period, TimeUnit.SECONDS);
    }

    // called periodically to reconfigure the cache according to the new access patterns
    private void computeCache() {
        logger.info("computeCache BEGIN");

        // display frequency
        prettyPrintFrequency();
        // TODO create copy
        //frequency.clear();

        if (frequency.size() > 0) {
            // for each known key, compute caching options and build a total caching options set
            // compute known keys set: for each key add the value of the first caching option
            cachingOptions.clear();
            keys = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
                String key = entry.getKey();
                Map<Integer, CachingOption> cachingOptionsKey = computeCachingOptionsForKey(key);
                cachingOptions.put(key, cachingOptionsKey);

                CachingOption first = cachingOptionsKey.entrySet().iterator().next().getValue();
                keys.add(new Pair<>(key, first.getValue()));
                logger.debug("keys.add(" + key + "," + first.getValue() + ")");
            }

            // sort keys decreasingly by value
            Collections.sort(keys, (o1, o2) -> o1.getValue().compareTo(o2.getValue()));
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
        logger.info("computeCache END");
    }

    /**
     * Compute caching options for a given key
     *
     * @param key for which to compute caching options
     * @return a set of caching options + their weights (in number of blocks)
     */
    private Map<Integer, CachingOption> computeCachingOptionsForKey(String key) {
        Map<Integer, CachingOption> cachingOptionsKey = new HashMap<>();

        // remember that regions are in increasing order of wget latency
        // compute which region to start from when caching blocks
        // the client only needs k blocks to reconstruct the data, but the system stores k + m blocks
        // so regions storing the most distant m blocks do not need to be accessed at all
        int crtRegionId = regions.size() - 1;
        int avoidedBlocks = 0;
        while (avoidedBlocks + regions.get(crtRegionId).getBlocks() < m) {
            crtRegionId--;
            avoidedBlocks += regions.get(crtRegionId).getBlocks();
        }

        // for iterating over the regions
        int myRegionId = 0;
        int blocksinMyRegion = regions.get(myRegionId).getBlocks(); // how many blocks can be read from the local backend
        int furthestCachedRegion = crtRegionId; // where to start caching blocks from
        // caching option attributes
        int blocks = 0; // how many blocks to cache
        List<String> regionNames = new ArrayList<>(); // from which regions to cache blocks
        while (crtRegionId > 0 && blocks + blocksinMyRegion < k) {
            Region region = regions.get(crtRegionId);
            blocks += region.getBlocks();
            regionNames.add(region.getName());

            // value = latency save * weighted popularity
            // latency save = latency(most distant region that i avoid) - latency(most distant region that i contact)
            double value = regions.get(furthestCachedRegion).getLatency() - regions.get(crtRegionId - 1).getLatency();
            if (frequency.containsKey(key)) {
                value *= frequency.get(key);
            }

            // add caching option
            CachingOption option = new CachingOption(key, blocks, value, new ArrayList<>(regionNames));
            cachingOptionsKey.put(blocks, option);

            // next iteration
            crtRegionId--;
        }

        // final caching option: cache blocks from local region too
        regionNames.add(regions.get(myRegionId).getName());
        double value = regions.get(furthestCachedRegion).getLatency();// - regions.get(myRegionId).getLatency();
        if (frequency.containsKey(key)) {
            value *= frequency.get(key);
        }
        blocks += regions.get(myRegionId).getBlocks();
        CachingOption option = new CachingOption(key, blocks, value, new ArrayList<>(regionNames));
        cachingOptionsKey.put(blocks, option);

        // print key caching options
        logger.debug("CacheOptions for " + key);
        for (Map.Entry<Integer, CachingOption> entry : cachingOptionsKey.entrySet())
            logger.debug(entry.getKey() + " " + entry.getValue().prettyPrint());

        return cachingOptionsKey;
    }

    /**
     * Dynamic solution to the Knapsack problem
     */
    private void computeChosenOptions() {
        // for each weight option, we keep the max value and the caching options to achieve it
        maxValue.clear();
        chosenOptions.clear();

        // for known weights, compute max value entries using the caching options of first key
        String firstKey = keys.get(0).getKey();
        Map<Integer, CachingOption> cachingOptionsFirstKey = cachingOptions.get(firstKey);
        for (int weight : weights) {
            maxValue.put(weight, cachingOptionsFirstKey.get(weight).getValue());
            List<CachingOption> chosenCachingOptions = new ArrayList<>();
            if (chosenOptions.get(weight) != null) {
                chosenCachingOptions = chosenOptions.get(weight);
            }
            chosenCachingOptions.add(cachingOptionsFirstKey.get(weight));
            chosenOptions.put(weight, chosenCachingOptions);
        }

        printMaxValue();
        printChosenOptions();

        // get max known weight (last in weights array)
        int maxWeight = weights[weights.length - 1];
        logger.debug("maxWeight=" + maxWeight);

        // estimate number of keys to consider
        int numKeysToConsider = cacheCapacity;
        if (numKeysToConsider > keys.size())
            numKeysToConsider = keys.size();
        logger.debug("keysToConsider=" + numKeysToConsider);

        for (int i = 1; i < numKeysToConsider; i++) {
            // we need to update max weight only at the end of this for loop
            int newMaxWeight = maxWeight;

            // check caching options for this key
            Map<Integer, CachingOption> cachingOptionsKey = cachingOptions.get(keys.get(i).getKey());
            for (int j = weights.length - 1; j >= 0; j--) {
                CachingOption co = cachingOptionsKey.get(weights[j]);

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
                    if (lastAddedCachingOption.getKey().equals(co.getKey()))
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
                            if (!cachingOptionsContainsKey(chosenOptions.get(weightToAddTo), co.getKey())) {
                                maxValue.put(newWeight, newValue);
                                chosenOptions.put(newWeight, new ArrayList<>(chosenOptions.get(weightToAddTo)));
                                chosenOptions.get(newWeight).add(co);
                            }
                        }
                    } else {
                        if (!cachingOptionsContainsKey(chosenOptions.get(weightToAddTo), co.getKey())) {
                            double newValue = maxValue.get(weightToAddTo) + co.getValue();
                            maxValue.put(newWeight, newValue);
                            chosenOptions.put(newWeight, new ArrayList<>(chosenOptions.get(weightToAddTo)));
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

    // helper for Knapsack algorithm
    private boolean cachingOptionsContainsKey(List<CachingOption> chosenOptions, String key) {
        for (CachingOption co : chosenOptions)
            if (co.getKey().equals(key)) {
                return true;
            }
        return false;
    }

    // handle client requests /////////////////////////////////////////////////////////////
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
        int regionId = 0;
        while (blocksBackend > 0 && regionId < regions.size() - 1) {
            Region region = regions.get(regionId);
            String regionName = region.getName();
            if (!reply.cacheRecipeContains(regionName)) {
                reply.addToBackendRecipe(regionName);
                blocksBackend -= region.getBlocks();
            }
            regionId++;
        }

        //logger.debug(reply.prettyPrint());
        return reply;
    }

    private void prettyPrintFrequency() {
        logger.info("frequency {");
        for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
            logger.info(entry.getKey() + " " + entry.getValue());
        }
        logger.info("}");
    }

    private void printMaxValue() {
        logger.debug("maxValue {");
        for (Map.Entry<Integer, Double> entry : maxValue.entrySet()) {
            logger.debug(entry.getKey() + " " + entry.getValue().intValue());
        }
        logger.debug("}");
    }

    private void printChosenOptions() {
        logger.debug("chosenOptions {");
        for (Map.Entry<Integer, List<CachingOption>> entry : chosenOptions.entrySet()) {
            String str = entry.getKey().toString();
            for (CachingOption co : entry.getValue()) {
                str += " " + co.prettyPrint();
            }
            logger.debug(str);
        }
        logger.debug("}");
    }

    private void prettyPrintCache() {
        logger.info("cache {");
        for (CachingOption co : cache)
            logger.info(co.prettyPrint());
        logger.info("}");
    }
}
