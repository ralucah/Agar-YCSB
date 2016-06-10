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

    private int period; // how often to recompute cache (ms)

    private Map<String, Integer> frequency; // how many times each key was requested during the current period
    private Map<String, Double> weightedPopularity; // weighted popularity for all keys seen since the proxy start
    private double alpha; // coefficient for weighted popularity (between 0 and 1)

    private int cacheCapacity; // how many blocks fit in the cache
    private List<CacheOption> cache; // current cache configuration

    private RegionManager regionManager; // computes an overview of the deployed system
    private int k; // number of data chunks
    private int m; // number of redundant chunks

    private int[] weights; // weight options (i.e., take blocks from one data center, two data centers, etc.)
    private List<Pair<String, Double>> keys; // all seen keys, sorted by value

    private Map<String, Map<Integer, CacheOption>> cacheOptions; // (key, list of cache options)
    private Map<Integer, Double> maxValue; // max value for each weight
    private Map<Integer, List<CacheOption>> chosenOptions; //  for each weight, list of options that we chose

    public DynamicCacheManager() {
        // erasure-coding params
        k = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        m = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_M_PROPERTY));

        // regions are sorted by latency, in increasing order
        regionManager = new RegionManager();

        // compute weights
        List<Region> selectedRegions = new ArrayList<Region>();
        int blocks = 0;
        for (Region region : regionManager.getRegions()) {
            blocks += region.getBlocks();
            selectedRegions.add(region);
            if (blocks >= k)
                break;
        }
        Collections.reverse(selectedRegions);
        weights = new int[selectedRegions.size()];
        int index = 0;
        int cummulativeWeight = 0;
        for (Region region : selectedRegions) {
            cummulativeWeight += region.getBlocks();
            weights[index] = cummulativeWeight;
            index++;
        }
        logger.debug("weights: " + Arrays.toString(weights));

        // cache size (in blocks); assume that 1 item = 1 mb (slab size in memcached)
        cacheCapacity = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.CACHE_SIZE_PROPERTY));

        // init cache, frequency, weighted popularity
        cache = Collections.synchronizedList(new ArrayList<CacheOption>());
        frequency = Collections.synchronizedMap(new HashMap<String, Integer>());
        weightedPopularity = Collections.synchronizedMap(new HashMap<String, Double>());

        // init cache options, chosen options
        cacheOptions = new HashMap<String, Map<Integer, CacheOption>>();
        chosenOptions = new HashMap<Integer, List<CacheOption>>();

        // init alpha, period
        alpha = Double.parseDouble(PropertyFactory.propertiesMap.get(PropertyFactory.ALPHA_PROPERTY));
        period = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.PERIOD_PROPERTY));
        logger.debug("alpha: " + alpha + " period: " + period);

        // init max value
        maxValue = new HashMap<Integer, Double>();

        // periodic thread that reconfigures cache
        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                computeCache();
            }
        }, period, period, TimeUnit.SECONDS);
    }

    private void computeWeightedPopularity() {
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

        prettyPrintWeightedPopularity();
    }

    private void computeCache() {
        logger.debug("computeCache BEGIN");

        // take a look at frequency
        prettyPrintFrequency();

        // use frequency to compute weighted popularity
        computeWeightedPopularity();

        // reset frequency
        frequency.clear();

        if (weightedPopularity.size() > 0) {
            // compute cache options + populate keys
            cacheOptions.clear();
            keys = new ArrayList<Pair<String, Double>>();
            //int keyNum = 0;
            for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
                String key = entry.getKey();

                Map<Integer, CacheOption> cacheOptionsKey = computeCacheOptionsForKey(key);
                cacheOptions.put(key, cacheOptionsKey);

                CacheOption first = cacheOptionsKey.entrySet().iterator().next().getValue();
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
            printCache();
        }

        logger.debug("computeCache END");
    }

    private void computeChosenOptions() {
        // clear
        maxValue.clear();
        chosenOptions.clear();

        // start by computing the max value entries for the first key
        Map<Integer, CacheOption> cacheOptionsKey1 = cacheOptions.get(keys.get(0).getKey());
        for (int weight : weights) {
            maxValue.put(weight, cacheOptionsKey1.get(weight).getValue());
            List<CacheOption> chosenCacheOptions = new ArrayList<CacheOption>();
            ;
            if (chosenOptions.get(weight) != null) {
                chosenCacheOptions = chosenOptions.get(weight);
            }
            chosenCacheOptions.add(cacheOptionsKey1.get(weight));
            chosenOptions.put(weight, chosenCacheOptions);
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
            Map<Integer, CacheOption> cacheOptionsKey = cacheOptions.get(keys.get(i).getKey());
            for (int j = weights.length - 1; j >= 0; j--) {
                CacheOption co = cacheOptionsKey.get(weights[j]);
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
                    CacheOption lastAddedCacheOption = chosenOptions.get(weightToRelax).get(last);

                    // do not relax our own key
                    if (lastAddedCacheOption.getKey() == co.getKey())
                        continue;

                    if (lastAddedCacheOption.getWeight() > co.getWeight()) {
                        // we replace the lastAddedCacheOption with a smaller weight option and fill in the remaining space with the current cacheoption
                        double newValue = prevValue -
                            lastAddedCacheOption.getValue() +
                            co.getValue() +
                            cacheOptions.get(lastAddedCacheOption.getKey()).get(lastAddedCacheOption.getWeight() - co.getWeight()).getValue();

                        // if a better combination of cache options exists, then update the chosen options list (relaxation step)
                        if (newValue > prevValue) {
                            maxValue.put(weightToRelax, newValue);
                            chosenOptions.get(weightToRelax).remove(last);
                            chosenOptions.get(weightToRelax).add(last, cacheOptions.get(lastAddedCacheOption.getKey()).get(lastAddedCacheOption.getWeight() - co.getWeight()));
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
                                chosenOptions.put(newWeight, new ArrayList<CacheOption>(chosenOptions.get(weightToAddTo)));
                                chosenOptions.get(newWeight).add(co);
                            }
                        }
                    } else {
                        if (!cacheOptionsContainsKey(chosenOptions.get(weightToAddTo), co.getKey())) {
                            double newValue = maxValue.get(weightToAddTo) + co.getValue();
                            maxValue.put(newWeight, newValue);
                            chosenOptions.put(newWeight, new ArrayList<CacheOption>(chosenOptions.get(weightToAddTo)));
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

    private boolean cacheOptionsContainsKey(List<CacheOption> chosenOptions, String key) {
        for (CacheOption co : chosenOptions)
            if (co.getKey().equals(key)) {
                return true;
            }
        return false;
    }

    private void printChosenOptions() {
        logger.debug("chosenOptions {");
        for (Map.Entry<Integer, List<CacheOption>> entry : chosenOptions.entrySet()) {
            String str = entry.getKey().toString();
            for (CacheOption co : entry.getValue())
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

    private Map<Integer, CacheOption> computeCacheOptionsForKey(String key) {
        Map<Integer, CacheOption> cacheOptionsKey = new HashMap<Integer, CacheOption>();

        int blocks = 0;
        List<String> regionNames = new ArrayList<String>();
        double latency = 0;

        // regions are in decreasing order of latency
        // regions[0] is the most distant
        // regions[size() - 1] is my region
        List<Region> regions = regionManager.getRegions();
        int myRegionId = 0;
        int numBlocksInMyRegion = regions.get(myRegionId).getBlocks();
        int crtRegionId = regions.size() - 1;

        // compute which region to start from
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
            CacheOption option = new CacheOption(key, blocks, value, new ArrayList<String>(regionNames));
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
        CacheOption option = new CacheOption(key, blocks, value, new ArrayList<String>(regionNames));
        cacheOptionsKey.put(blocks, option);

        logger.debug("CacheOptions for " + key);
        for (Map.Entry<Integer, CacheOption> entry : cacheOptionsKey.entrySet())
            logger.debug(entry.getKey() + " " + entry.getValue().prettyPrint());

        return cacheOptionsKey;
    }

    protected int cacheContains(String key) {
        int index = 0;
        for (CacheOption cacheOption : cache) {
            if (cacheOption.getKey().equals(key))
                return index;
            index++;
        }
        return -1;
    }

    public ProxyReply buildReply(String key) {
        incrementFrequency(key);
        int blocksCache = 0;

        ProxyReply reply = new ProxyReply();
        synchronized (cache) {
            int index = cacheContains(key);
            if (index != -1) {
                blocksCache = cache.get(index).getWeight();
                reply.setCacheRecipe(cache.get(index).getRegions());
            }
        }
        int blocksBackend = k - blocksCache;
        int regionId = regionManager.getRegions().size() - 1;
        while (blocksBackend > 0 && regionId >= 0) {
            Region region = regionManager.getRegions().get(regionId);
            reply.addToS3Recipe(region.getName());
            blocksBackend -= region.getBlocks();
            regionId--;
        }

        //logger.debug(reply.prettyPrint());
        return reply;
    }

    private void incrementFrequency(String key) {
        if (frequency.get(key) == null) {
            frequency.put(key, 1);
        } else {
            int freq = frequency.get(key);
            frequency.put(key, freq + 1);
        }
    }

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

    private void printCache() {
        logger.debug("cache {");
        for (CacheOption co : cache)
            logger.debug(co.prettyPrint());
        logger.debug("}");
    }
}
