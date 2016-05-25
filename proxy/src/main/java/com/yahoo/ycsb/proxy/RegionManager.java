package com.yahoo.ycsb.proxy;

import org.apache.log4j.Logger;

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

        for (int i = 0; i < regionNames.size(); i++) {
            String regionName = regionNames.get(i);
            String endpointName = endpointNames.get(i);
            Region region = new Region(regionName, endpointName);
            double pingTime = wgetFake(endpointName);
            region.setLatency(pingTime);
            regions.add(region);
        }

        int regionsSize = regions.size();
        for (int i = 0; i < k + m; i++) {
            Region region = regions.get(i % regionsSize);
            region.incrementBlocks();
        }

        Collections.sort(regions);
        //Collections.reverse(regions);

        latencyMax = regions.get(regionsSize - 1).getLatency();

        for (Region region : regions)
            System.err.println(region.prettyPrint());
    }

    private double wgetFake(String host) {
        if (host.contains("sa-east-1"))  // sao
            return 1594;
        else if (host.contains("external-1")) // virginia
            return 618;
        else if (host.contains("eu-west-1")) // ireland
            return 356;
        else if (host.contains("eu-central-1")) // frankfurt
            return 184;
        else if (host.contains("ap-northeast-1")) // tokyo
            return 1685;
        else if (host.contains("ap-southeast-2")) // sydney
            return 1633;
        return Double.MIN_VALUE;
    }

    private double wget(String host) {
        double avgTime = Double.MIN_VALUE;

        try {
            //String command = "ping -c 5 " + host;
            String command = "wget ";
            if (host.contains("sa-east-1"))  // sao
                command += "https://s3-sa-east-1.amazonaws.com/sao101/key10003851782042273600";
            else if (host.contains("external-1")) // virginia
                command += "https://s3.amazonaws.com/virginia101/key10003851782042273601";
            else if (host.contains("eu-west-1")) // ireland
                command += "https://s3-eu-west-1.amazonaws.com/patrick101/key10003851782042273602";
            else if (host.contains("eu-central-1")) // frankfurt
                command += "https://s3.eu-central-1.amazonaws.com/frank101/key10003851782042273603";
            else if (host.contains("ap-northeast-1")) // tokyo
                command += "https://s3-ap-northeast-1.amazonaws.com/tokyo101/key10003851782042273604";
            else if (host.contains("ap-southeast-2")) // sydney
                command += "https://s3-ap-southeast-2.amazonaws.com/sydney101/key10003851782042273605";

            long start = System.currentTimeMillis();
            Process process = Runtime.getRuntime().exec(command);
            process.waitFor();
            avgTime = System.currentTimeMillis() - start;
            Runtime.getRuntime().exec("rm key10003851782042273600");
            Runtime.getRuntime().exec("rm key10003851782042273601");
            Runtime.getRuntime().exec("rm key10003851782042273602");
            Runtime.getRuntime().exec("rm key10003851782042273603");
            Runtime.getRuntime().exec("rm key10003851782042273604");
            Runtime.getRuntime().exec("rm key10003851782042273605");

            /*BufferedReader inputStream = new BufferedReader(
                new InputStreamReader(process.getInputStream()));
            String s = "";
            // reading output stream of the command
            while ((s = inputStream.readLine()) != null) {
                if (s.contains("avg")) {
                    //System.out.println(s);
                    //System.out.println(s.split(" ")[3].split("/")[1]);
                    return new Double(s.split(" ")[3].split("/")[1]);
                }
            }*/

        } catch (Exception e) {
            logger.warn("Could not ping " + host);
        }

        return avgTime;

        /*if (host.contains("eu-west-1"))
            return 38.392;
            //return 100;
        else if (host.contains("eu-central-1"))
            return 17.978;
            //return 150;
        else if (host.contains("external-1"))
            return 97.114;
        //return 10;
        return Double.MIN_VALUE;*/
    }

    public List<Region> getRegions() {
        return regions;
    }

    public int getNumRegions() {
        return regions.size();
    }

    public double getLatencyMax() {
        return latencyMax;
    }

}
