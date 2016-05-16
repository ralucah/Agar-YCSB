package com.yahoo.ycsb.common.communication;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ProxyReply implements Serializable {
    private static final long serialVersionUID = 7526472295622776148L;

    private List<String> s3Recipe; // s3 region names
    private List<String> cacheRecipe; // s3 region names

    public ProxyReply() {
        s3Recipe = new ArrayList<String>();
        cacheRecipe = new ArrayList<String>();
    }

    public void addToS3Recipe(String region) {
        s3Recipe.add(region);
    }

    public List<String> getS3Recipe() {
        return s3Recipe;
    }

    public List<String> getCacheRecipe() {
        return cacheRecipe;
    }

    public void setCacheRecipe(List<String> regionNames) {
        this.cacheRecipe = regionNames;
    }

    public String prettyPrint() {
        String str = "ProxyReply: S3{ ";
        for (String s3Region : s3Recipe)
            str += s3Region + " ";
        str += "} Cache{ ";
        for (String cacheFromRegion : cacheRecipe)
            str += cacheFromRegion + " ";
        str += "}";
        return str;
    }
}
