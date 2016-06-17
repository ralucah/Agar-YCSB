package com.yahoo.ycsb.common.communication;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ProxyReply implements Serializable {
    private static final long serialVersionUID = 7526472295622776148L; // for serialization

    private List<String> backendRecipe; // fetch from backend blocks from these regions
    private List<String> cacheRecipe; // fetch from cache blocks from these regions

    public ProxyReply() {
        backendRecipe = new ArrayList<String>();
        cacheRecipe = new ArrayList<String>();
    }

    public void addToBackendRecipe(String region) {
        backendRecipe.add(region);
    }

    public boolean cacheRecipeContains(String region) {
        return cacheRecipe.contains(region);
    }

    public List<String> getBackendRecipe() {
        return backendRecipe;
    }

    public List<String> getCacheRecipe() {
        return cacheRecipe;
    }

    public void setCacheRecipe(List<String> regionNames) {
        this.cacheRecipe = regionNames;
    }

    public String prettyPrint() {
        String str = "ProxyReply: Backend{ ";
        for (String s3Region : backendRecipe)
            str += s3Region + " ";
        str += "} Cache{ ";
        for (String cacheFromRegion : cacheRecipe)
            str += cacheFromRegion + " ";
        str += "}";
        return str;
    }
}
