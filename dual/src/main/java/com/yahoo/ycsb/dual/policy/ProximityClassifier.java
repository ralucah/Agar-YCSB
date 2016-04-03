package com.yahoo.ycsb.dual.policy;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by ubuntu on 01.02.16.
 */
public abstract class ProximityClassifier {
    private static Logger logger = Logger.getLogger(ProximityClassifier.class);

    private static double ping(String host) {
        double avgTime = Double.MIN_VALUE;

        try {
            String command = "ping -c 5 " + host;
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader inputStream = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
            String s = "";
            // reading output stream of the command
            while ((s = inputStream.readLine()) != null) {
                if (s.contains("avg")) {
                    //System.out.println(s);
                    //System.out.println(s.split(" ")[3].split("/")[1]);
                    return new Double(s.split(" ")[3].split("/")[1]);
                }
            }

        } catch (Exception e) {
            logger.warn("Could not ping " + host);
        }

        return avgTime;
    }

    /*public static void sortS3Regions(List<S3Region> s3Regions) {
        // compute avg ping time
        for (S3Region region : s3Regions) {
            region.setAvgPingTime(ping(region.getEndPoint()));
        }
        // sort regions by avg ping time
        Collections.sort(s3Regions);

        for (S3Region region : s3Regions)
            logger.debug(region.getRegion());
    }*/

    /*public static void sortMemcachedRegions(List<MemcachedRegion> memcachedRegions) {
        // compute avg ping time
        for (MemcachedRegion region : memcachedRegions) {
            region.setAvgPingTime(ping(region.getIp()));
        }
        // sort regions by avg ping time
        Collections.sort(memcachedRegions);

        for (MemcachedRegion region : memcachedRegions)
            logger.debug(region.getIp() + ":" + region.getPort());
    }*/
}
