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

    private int k; // number of data chunks
    private int m; // number of redundant chunks

    private List<Pair<String, Integer>> keys; // all keys seen since proxy start, sorted by value
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

        // init cache options, max values, and chosen options; only accessed from thead that reconfigures cache
        cachingOptions = new HashMap<>();

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

        synchronized (cache) {
            cache.clear();
        }

        // iterate Map<String, Integer> frequency
        //for (Map.Entry<String, Integer> freqEntry : frequency.entrySet()) {
        // for each key seen over the last period, compute a caching option and add it to the total caching options set
        //    String key = freqEntry.getKey();
        //}
        if (frequency.size() > 0) {
            // sort keys and compute caching options
            cachingOptions.clear();
            keys = new ArrayList<Pair<String, Integer>>();
            for (Map.Entry<String, Integer> entry : frequency.entrySet()) {
                String key = entry.getKey();
                int freq = entry.getValue();
                keys.add(new Pair<>(key, freq));
                CachingOption coKey = new CachingOption(key, weight, freq);
                cachingOptions.put(key, coKey);
            }

            // sort keys decreasingly by value
            Collections.sort(keys, (o1, o2) -> o1.getValue().compareTo(o2.getValue()));
            Collections.reverse(keys);
            logger.debug("Keys sorted decreasingly by value: " + keys);

            for (Pair<String, Integer> pair : keys) {
                String key = pair.getKey();
                if (cache.size() < cacheCapacity) {
                    cache.add(cachingOptions.get(key));
                } else {
                    break;
                }
            }
        }

        prettyPrintCache();

        // clear frequency
        frequency.clear();

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

    private void prettyPrintCache() {
        logger.info("cache {");
        for (CachingOption co : cache)
            logger.info(co.prettyPrint());
        logger.info("}");
    }
}
