package com.yahoo.ycsb.dual.utils;

public class S3Region extends Region {

    private String region;
    private String endPoint;
    private String bucket;

    public S3Region(String region, String endPoint, String bucket) {
        this.region = region;
        this.endPoint = endPoint;
        this.bucket = bucket;
    }


    public String getEndPoint() {
        return endPoint;
    }

    public String getRegion() {
        return region;
    }

    public void print() {
        System.out.println(endPoint + " " + region + " " + avgPingTime);
    }
}
