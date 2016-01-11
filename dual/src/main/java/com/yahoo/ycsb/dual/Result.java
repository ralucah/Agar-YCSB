package com.yahoo.ycsb.dual;

import com.yahoo.ycsb.Status;

/**
 * Created by Raluca on 11.01.16.
 */
public class Result {
    private Status status;
    private byte[] bytes;

    /*public Result(Status status, byte[] bytes) {
        this.status = status;
        this.bytes = bytes;
    }*/

    public byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        this.bytes = bytes;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }
}
