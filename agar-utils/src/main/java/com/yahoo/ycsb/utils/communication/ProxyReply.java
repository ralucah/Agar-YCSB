/**
 * Copyright 2016 [Agar]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.ycsb.utils.communication;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class ProxyReply implements Serializable {
    private static final long serialVersionUID = 7526472295622776148L;

    private List<String> backendRecipe; // fetch from backend the blocks from these regions
    private List<String> cacheRecipe; // fetch from cache the blocks from these regions
    private int cachedBlocks;

    public ProxyReply(int cachedBlocks) {
        this.cachedBlocks = cachedBlocks;
        backendRecipe = new ArrayList<String>();
        cacheRecipe = new ArrayList<String>();
    }

    public ProxyReply() {
        backendRecipe = new ArrayList<String>();
        cacheRecipe = new ArrayList<String>();
    }

    public int getCachedBlocks() {
        return cachedBlocks;
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
