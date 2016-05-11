package com.yahoo.ycsb.common.communication;

import java.io.Serializable;

/**
 * Created by Raluca on 24.04.16.
 */
public class ProxyReply implements Serializable {
    private static final long serialVersionUID = 7526472295622776148L;

    private int numBlocks;

    public ProxyReply(int numBlocks) {
        this.numBlocks = numBlocks;
    }

    public int getNumBlocks() {
        return numBlocks;
    }

    public String prettyPrint() {
        return "ProxyReply: " + numBlocks + " blocks";
    }
}
