package com.yahoo.ycsb.proxy;

import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.common.properties.PropertyFactory;
import com.yahoo.ycsb.common.s3.S3Connection;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

// Provides an overview of the system deployment
public class RegionManager {
    protected static Logger logger = Logger.getLogger(RegionManager.class);

    private List<Region> regions; // regions where blocks are stored, sorted by read latency
    private int k; // number of data chunks

    public RegionManager() {
        // init regions, k, m
        regions = new ArrayList<>();
        k = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_K_PROPERTY));
        int m = Integer.parseInt(PropertyFactory.propertiesMap.get(PropertyFactory.LONGHAIR_M_PROPERTY));

        initS3();

        int regionsSize = regions.size();
        for (int i = 0; i < k + m; i++) {
            Region region = regions.get(i % regionsSize);
            region.incrementBlocks();
        }

        // sort regions by latency
        Collections.sort(regions);

        // latency to read blocks from the furthest data center
        //latencyMax = regions.get(regionsSize - 1).getLatency();

        for (Region region : regions)
            logger.error(region.prettyPrint());
    }

    private void initS3() {
        List<String> regionNames = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_REGIONS_PROPERTY).split("\\s*,\\s*"));
        List<String> endpointNames = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_ENDPOINTS_PROPERTY).split("\\s*,\\s*"));
        List<String> s3BucketNames = Arrays.asList(PropertyFactory.propertiesMap.get(PropertyFactory.S3_BUCKETS_PROPERTY).split("\\s*,\\s*"));
        if (s3BucketNames.size() != endpointNames.size() || endpointNames.size() != regionNames.size())
            logger.error("Configuration error: #buckets = #regions = #endpoints");

        List<S3Connection> s3Connections = new ArrayList<>();
        for (int i = 0; i < s3BucketNames.size(); i++) {
            String bucket = s3BucketNames.get(i);
            String regionName = regionNames.get(i);
            String endpoint = endpointNames.get(i);
            try {
                S3Connection client = new S3Connection(s3BucketNames.get(i), regionNames.get(i), endpointNames.get(i));
                s3Connections.add(client);
                logger.debug("S3 connection " + i + " " + bucket + " " + regionName + " " + endpoint);

                Region region = new Region(regionName, endpoint);
                double latency = read(endpoint, client);
                region.setLatency(latency);
                regions.add(region);
            } catch (ClientException e) {
                logger.error("Error connecting to " + s3BucketNames.get(i));
            }
        }
    }

    private double read(String host, S3Connection s3conn) {
        double avgTime = Double.MIN_VALUE;
        String key = null;
        if (host.contains("sa-east-1"))  // sao
            key = "key10003851782042273600";
        else if (host.contains("external-1")) // virginia
            key = "key10003851782042273601";
        else if (host.contains("eu-west-1")) // ireland
            key = "key10003851782042273602";
        else if (host.contains("eu-central-1")) // frankfurt
            key = "key10003851782042273603";
        else if (host.contains("ap-northeast-1")) // tokyo
            key = "key10003851782042273604";
        else if (host.contains("ap-southeast-2")) // sydney
            key = "key10003851782042273605";

        if (key != null) {
            long start = System.currentTimeMillis();
            try {
                s3conn.read(key);
            } catch (InterruptedException e) {
                logger.error("Error reading " + key + " from " + host);
            }
            avgTime = System.currentTimeMillis() - start;
        }

        return avgTime;
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

    // Computes the possible weights for the current deployment and returns it
    public int[] getWeights() {
        // select the regions to access in order to retrieve k data blocks
        List<Region> selectedRegions = new ArrayList<>();
        int blocks = 0;
        for (Region region : regions) {
            blocks += region.getBlocks();
            selectedRegions.add(region);
            if (blocks >= k)
                break;
        }

        // sort selected regions decreasingly (when caching, priority is given to blocks from regions that are farthest)
        Collections.reverse(selectedRegions);

        // to compute possible weights, consider we cache blocks from one region, then two, and so on.
        int[] weights = new int[selectedRegions.size()];
        int index = 0;
        int cummulativeWeight = 0;
        for (Region region : selectedRegions) {
            cummulativeWeight += region.getBlocks();
            weights[index] = cummulativeWeight;
            index++;
        }
        return weights;
    }

}
