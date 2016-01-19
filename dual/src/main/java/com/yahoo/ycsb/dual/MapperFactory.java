package com.yahoo.ycsb.dual;

/**
 * Created by Raluca on 19.01.16.
 */

public class MapperFactory {
    public static Mapper newMapper(String mapperName) {
        ClassLoader classLoader = MapperFactory.class.getClassLoader();

        Mapper mapper = null;
        try {
            Class mapperClass = classLoader.loadClass(mapperName);
            mapper = (Mapper) mapperClass.newInstance();
            DualClient.logger.debug("Mapper: " + mapperName);
        } catch (Exception e) {
            DualClient.logger.error("Could not load mapper " + mapperName);
        }

        return mapper;
    }
}
