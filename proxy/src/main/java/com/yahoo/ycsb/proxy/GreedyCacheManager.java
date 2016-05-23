package com.yahoo.ycsb.proxy;

public class GreedyCacheManager extends CacheManager {

    public GreedyCacheManager() {
        super();
    }

    @Override
    protected void populateCache() {
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
    }

}
