package com.yahoo.ycsb.proxy;

public class GreedyCacheManager {
    /*public static Logger logger = Logger.getLogger(GreedyCacheManager.class);

    private int period; // how often to recompute cache (ms)
    private List<CacheOption> cacheOptions; // cache options for the seen keys
    private List<CacheOption> cache; // current cache configuration
    private Map<String, Integer> frequency; // frequency stats for keys
    private Map<String, Double> weightedPopularity; // weighted popularity for keys
    private double alpha; // coefficient for weighted popularity (between 0 and 1)
    private AtomicInteger cachesizeMax; // in block numbers
    private AtomicInteger cachesize; // in block numbers

    protected RegionManager regionManager; // computes an overview of the deployed system
    protected int k; // number of data chunks
    protected int m; // number of redundant chunks

    public GreedyCacheManager() {
        // max cache size in blocks
        int cachesizeMB = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.CACHE_SIZE_PROPERTY));
        // assume that 1 item = 1 mb (slab size in memcached)
        cachesizeMax = new AtomicInteger(cachesizeMB);

        regionManager = new RegionManager();
        k = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        m = Integer.valueOf(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_M_PROPERTY));

        // current cache size
        cachesize = new AtomicInteger(0);
        cache = Collections.synchronizedList(new ArrayList<CacheOption>());
        cacheOptions = Collections.synchronizedList(new ArrayList<CacheOption>());

        frequency = Collections.synchronizedMap(new HashMap<String, Integer>());
        weightedPopularity = Collections.synchronizedMap(new HashMap<String, Double>());

        period = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.PERIOD_PROPERTY));
        alpha = Double.parseDouble(PropertyFactory.propertiesMap.get(PropertyFactory.ALPHA_PROPERTY));
        System.err.println("alpha: " + alpha + " period: " + period);

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
        //prettyPrintFrequency();
        //prettyPrintWeightedPopularity();

        // reset frequency
        frequency.clear();

        computeGreedyCache();

        logger.debug("reconfigure cache END");
    }

    private void computeGreedyCache() {
        // compute cache options
        computeCacheOptions();

        // update current cache according to new options
        logger.debug("Current options, sorted:");
        printCacheOptions(cacheOptions);
        //logger.debug("Current cache:");
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

        logger.debug("Cache:");
        printCacheOptions(cache);
        logger.debug("cachesize:" + cachesize + " cachesizemax:" + cachesizeMax);
    }

    private void computeCacheOptions() {
        cacheOptions.clear();
        for (Map.Entry<String, Double> entry : weightedPopularity.entrySet()) {
            String key = entry.getKey();
            //if (isKeyInCacheOptions(key) == false) {
            List<CacheOption> cacheOptionsKey = computeCacheOptionForKey(key);
            cacheOptions.addAll(cacheOptionsKey);
            //}
        }

        cacheOptions.sort(CacheOption::compareTo);
        Collections.reverse(cacheOptions);

        //logger.debug("All cache options:");
        //printCacheOptions(cacheOptions);
    }

    private List<CacheOption> computeCacheOptionForKey(String key) {
        List<CacheOption> cacheOptionsKey = new ArrayList<CacheOption>();
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
            cacheOptionsKey.add(option);
            crtRegionId--;
        }
        // add my region to cache
        regionNames.add(regions.get(myRegionId).getName());
        double value = regions.get(furthestCachedRegion).getLatency();
        //System.out.println(key + " " + k + " " + value);
        if (weightedPopularity.containsKey(key) == true) {
            value *= weightedPopularity.get(key);
        }
        CacheOption option = new CacheOption(key, k, value, new ArrayList<String>(regionNames));
        cacheOptionsKey.add(option);

        //logger.debug("CacheOptions for " + key);
        //printCacheOptions(cacheOptionsKey);
        return cacheOptionsKey;
    }

    private void printCacheOptions(List<CacheOption> options) {
        for (CacheOption cacheOption : options)
            logger.debug(cacheOption.prettyPrint());
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
                blocksCache = cache.get(index).getBlocks();
                reply.setCacheRecipe(cache.get(index).getRegions());
            }
        }
        int blocksBackend = k - blocksCache;
        int regionId = regionManager.getRegions().size() - 1;
        while (blocksBackend > 0 && regionId >= 0) {
            Region region = regionManager.getRegions().get(regionId);
            reply.addToBackendRecipe(region.getName());
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

    private boolean isKeyInCacheOptions(String key) {
        for (CacheOption cacheOption : cacheOptions) {
            if (cacheOption.getKey().equals(key))
                return true;
        }
        return false;
    }

    private void incrementFrequency(String key) {
        if (frequency.get(key) == null) {
            frequency.put(key, 1);
        } else {
            int freq = frequency.get(key);
            frequency.put(key, freq + 1);
        }
    }*/
}
