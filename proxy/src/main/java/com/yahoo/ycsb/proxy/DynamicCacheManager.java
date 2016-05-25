package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.common.communication.ProxyReply;
import org.apache.log4j.Logger;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class DynamicCacheManager {
    public static Logger logger = Logger.getLogger(DynamicCacheManager.class);

    private int period; // how often to recompute cache (ms)
    private List<CacheOption> cache; // current cache configuration
    private Map<String, Integer> frequency; // frequency stats for keys
    private Map<String, Double> weightedPopularity; // weighted popularity for keys
    private double alpha; // coefficient for weighted popularity (between 0 and 1)
    private AtomicInteger cachesizeMax; // in block numbers
    private AtomicInteger cachesize; // in block numbers

    private RegionManager regionManager; // computes an overview of the deployed system
    private int k; // number of data chunks
    private int m; // number of redundant chunks

    private int[] weights;
    private String[] keys; // sorted by value

    private Map<String, Map<Integer, CacheOption>> cacheOptions; // (key, list of cache options)
    private Map<Integer, Double> maxValue;
    private Map<Integer, List<CacheOption>> chosenOptions; //  for each weight, list of options that we chose

    public DynamicCacheManager() {
        // erasure-coding params
        k = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        m = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_M_PROPERTY));

        // region are sorted by latency, in increasing order
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
        int cachesizeMB = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.CACHE_SIZE_PROPERTY));
        cachesizeMax = new AtomicInteger(cachesizeMB);
        cachesize = new AtomicInteger(0);
        cache = Collections.synchronizedList(new ArrayList<CacheOption>());

        frequency = Collections.synchronizedMap(new HashMap<String, Integer>());
        weightedPopularity = Collections.synchronizedMap(new HashMap<String, Double>());

        cacheOptions = new HashMap<String, Map<Integer, CacheOption>>();
        chosenOptions = new HashMap<Integer, List<CacheOption>>();

        alpha = Double.parseDouble(PropertyFactory.propertiesMap.get(PropertyFactory.ALPHA_PROPERTY));
        period = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.PERIOD_PROPERTY));
        System.err.println("alpha: " + alpha + " period: " + period);

        // max value
        maxValue = new HashMap<Integer, Double>();

        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                reconfigureCache();
            }
        }, period, period, TimeUnit.SECONDS);
    }

    private void reconfigureCache() {
        logger.debug("reconfigure cache BEGIN");
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
        prettyPrintFrequency();
        prettyPrintWeightedPopularity();

        // reset frequency
        frequency.clear();

        computeCache();

        logger.debug("reconfigure cache END");
    }

    private synchronized void computeCache() {
        // compute cache options
        cacheOptions.clear();
        for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
            String key = entry.getKey();
            Map<Integer, CacheOption> cacheOptionsKey = computeCacheOptionsForKey(key);
            cacheOptions.put(key, cacheOptionsKey);
        }
        //printCacheOptions();

        // build list of keys sorted by value
        keys = new String[cacheOptions.size()];
        SortedMap<Double, String> sortedKeys = new TreeMap<Double, String>();
        for (Map.Entry<String, Map<Integer, CacheOption>> entry : cacheOptions.entrySet()) {
            // get first entry
            CacheOption first = entry.getValue().entrySet().iterator().next().getValue();
            sortedKeys.put(first.getValue(), entry.getKey());
        }
        sortedKeys.values().toArray(keys);
        Collections.reverse(Arrays.asList(keys));
        logger.debug("Keys sorted increasingly by value: " + sortedKeys);
        logger.debug("Keys sorted decreasingly by value: " + Arrays.asList(keys));


        // clear both?
        maxValue.clear();
        chosenOptions.clear();

        // start by computing the max value entries for the first key
        Map<Integer, CacheOption> cacheOptionsKey1 = cacheOptions.get(keys[0]);
        for (int weight : weights) {
            // max value
            maxValue.put(weight, cacheOptionsKey1.get(weight).getValue());
            // chosen options
            List<CacheOption> chosenCacheOptions;
            if (chosenOptions.get(weight) == null) {
                chosenCacheOptions = new ArrayList<CacheOption>();
            } else {
                chosenCacheOptions = chosenOptions.get(weight);
            }
            chosenCacheOptions.add(cacheOptionsKey1.get(weight));
            chosenOptions.put(weight, chosenCacheOptions);
        }
        printMaxValue();
        int maxWeight = weights[weights.length - 1];
        logger.debug("maxWeight=" + maxWeight);
        printChosenOptions();

        int numKeys = cachesizeMax.intValue();
        if (numKeys > keys.length)
            numKeys = keys.length;
        logger.debug("numKeys=" + numKeys);

        for (int i = 1; i < keys.length; i++) {
            int newMaxWeight = maxWeight;
            Map<Integer, CacheOption> cacheOptionsKey = cacheOptions.get(keys[i]);
            for (int j = weights.length - 1; j >= 0; j--) {
                CacheOption co = cacheOptionsKey.get(weights[j]);
                int crtWeight = co.getWeight();

                // iterate through possible weights and try to relax a previous choice
                for (int weight : weights) {
                    // see if we can improve the last 5 options by replacing a larger cacheoption with two smaller ones
                    int weightToRelax = maxWeight - weight;
                    if (!maxValue.containsKey(weightToRelax))
                        continue;
                    double prevValue = maxValue.get(weightToRelax);
                    int last = chosenOptions.get(weightToRelax).size() - 1;
                    // get cacheoption at the end of the list and try to split it
                    CacheOption lastAddedCacheOption = chosenOptions.get(weightToRelax).get(last);

                    //do not relax our own key
                    if (lastAddedCacheOption.getKey() == co.getKey())
                        continue;

                    if (lastAddedCacheOption.getWeight() > co.getWeight()) {
                        // we replace the lastAddedCacheOption with a smaller weight option and fill in the remaining space with the current cacheoption
                        double newValue = prevValue -
                            lastAddedCacheOption.getValue() +
                            co.getValue() +
                            cacheOptions.get(lastAddedCacheOption.getKey()).get(lastAddedCacheOption.getWeight() -
                                co.getWeight()).getValue();

                        // if a better combination of cache options exists, then update the chosen options list (relaxation step)
                        if (newValue > prevValue) {
                            maxValue.put(weightToRelax, newValue);
                            chosenOptions.get(weightToRelax).remove(last);
                            chosenOptions.get(weightToRelax).add(last, cacheOptions.get(lastAddedCacheOption.getKey()).get(lastAddedCacheOption.getWeight() -
                                co.getWeight()));
                            chosenOptions.get(weightToRelax).add(last + 1, co);

                        }
                    }
                }

                //try to add new co at the end
                for (int weight : weights) {
                    int weightToAddTo = maxWeight - weight;
                    if (!maxValue.containsKey(weightToAddTo))
                        continue;

                    int newWeight = weightToAddTo + co.getWeight();

                    if (newWeight > newMaxWeight)
                        newMaxWeight = newWeight;

                    if (maxValue.containsKey(newWeight)) {
                        double prevValue = maxValue.get(weightToAddTo + co.getWeight());

                        // we add co at the end of weightToAddTo
                        double newValue = maxValue.get(weightToAddTo) + co.getValue();

                        // if a better combination of cache options exists, then update the chosen options list (relaxation step)
                        if (newValue > prevValue && !cacheOptionsContainsKey(chosenOptions.get(newWeight), co.getKey())) {
                            maxValue.put(newWeight, newValue);
                            chosenOptions.get(newWeight).add(co);
                        }
                    } else {
                        double newValue = maxValue.get(weightToAddTo) + co.getValue();
                        maxValue.put(newWeight, newValue);
                        chosenOptions.put(newWeight, new ArrayList<CacheOption>(chosenOptions.get(weightToAddTo)));
                        chosenOptions.get(newWeight).add(co);
                    }
                }
            }
            maxWeight = newMaxWeight;
        }
        printChosenOptions();
        System.exit(1);

        //cacheOptions.sort(CacheOption::compareTo);
        //Collections.reverse(cacheOptions);

        //logger.debug("All cache options:");
        //printCacheOptions(cacheOptions);

        // update current cache according to new options
        logger.debug("Current cache options:");
        //printCacheOptions(cacheOptions);
        //logger.debug("Current cache:");
        //printCacheOptions(cache);

        // populate the cache!
        cachesize.set(0);
        cache.clear();

        /*int index = 0;
        while (cachesize.intValue() < cachesizeMax.intValue() && index < cacheOptions.size()) {
            // get cache option with highest ratio
            CacheOption best = cacheOptions.get(index);
            if (cachesize.intValue() + best.getWeight() <= cachesizeMax.intValue() && cacheContains(best.getKey()) == -1) {
                cache.add(best);
                cachesize.addAndGet(best.getWeight());
            }
            index++;
        }*/

        //logger.debug("Cache:");
        //printCacheOptions(cache);
        logger.debug("cachesize:" + cachesize + " cachesizemax:" + cachesizeMax);
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

    private void printCacheOptions() {
        for (Map.Entry<String, Map<Integer, CacheOption>> entry : cacheOptions.entrySet()) {
            String str = entry.getKey() + " ";
            for (Map.Entry<Integer, CacheOption> cacheOptionEntry : entry.getValue().entrySet())
                str += cacheOptionEntry.getKey() + " " + cacheOptionEntry.getValue().prettyPrint() + " ";
            logger.debug(str);
        }
    }

    protected int cacheContains(String key) {
        int index = 0;
        for (CacheOption cacheOption : cache) {
            /*if (cacheOption.getKey().equals(key))
                return index;*/
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
                /*blocksCache = cache.get(index).getBlocks();*/
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

        return reply;
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

    /*private boolean isKeyInCacheOptions(String key) {
        for (CacheOption cacheOption : cacheOptions) {
            if (cacheOption.getKey().equals(key))
                return true;
        }
        return false;
    }*/

    private void incrementFrequency(String key) {
        if (frequency.get(key) == null) {
            frequency.put(key, 1);
        } else {
            int freq = frequency.get(key);
            frequency.put(key, freq + 1);
        }
    }
}
