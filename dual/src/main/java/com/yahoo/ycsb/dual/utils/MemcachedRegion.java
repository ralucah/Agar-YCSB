package com.yahoo.ycsb.dual.utils;

public class MemcachedRegion extends Region {

    private String ip;
    private String port;

    public MemcachedRegion(String ip, String port) {
        this.ip = ip;
        this.port = port;
    }

    public String getIp() {
        return ip;
    }

    public String getPort() {
        return port;
    }

    public void print() {
        System.out.println(ip + ":" + port + " " + avgPingTime);
    }
}
