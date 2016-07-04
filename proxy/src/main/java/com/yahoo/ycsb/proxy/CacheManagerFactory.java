package com.yahoo.ycsb.proxy;

/**
 * Creates a CacheManager layer by dynamically classloading the specified ClassManager class.
 */
public class CacheManagerFactory {
    public static CacheManagerBlueprint newCacheManager(String cacheManagerName) {
        ClassLoader classLoader = CacheManagerFactory.class.getClassLoader();
        CacheManagerBlueprint ret = null;

        try {
            Class cacheManagerClass = classLoader.loadClass(cacheManagerName);
            //System.out.println("cacheManagerClass.getName() = " + cacheManagerClass.getName());
            ret = (CacheManagerBlueprint) cacheManagerClass.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return ret;
    }
}
