package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class RegionManager {
    protected static Logger logger = Logger.getLogger(RegionManager.class);

    private List<Region> regions;
    private int k;
    private int m;
    private double latencyMax;

    public RegionManager() {
        regions = new ArrayList<Region>();
        k = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        m = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_M_PROPERTY));

        List<String> regionNames = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_REGIONS_PROPERTY).split("\\s*,\\s*"));
        List<String> endpointNames = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_ENDPOINTS_PROPERTY).split("\\s*,\\s*"));

        latencyMax = 0;
        for (int i = 0; i < regionNames.size(); i++) {
            String regionName = regionNames.get(i);
            String endpointName = endpointNames.get(i);
            Region region = new Region(regionName, endpointName);
            double pingTime = ping(endpointName);
            region.setLatency(pingTime);
            latencyMax += pingTime;
            regions.add(region);
        }

        int regionsSize = regions.size();
        for (int i = 0; i < k + m; i++) {
            Region region = regions.get(i % regionsSize);
            region.incrementBlocks();
        }

        Collections.sort(regions);

        for (Region region : regions)
            System.out.println(region.prettyPrint());
    }

    private double ping(String host) {
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

    public List<Region> getRegions() {
        return regions;
    }

    public double getLatencyMax() {
        return latencyMax;
    }

}
