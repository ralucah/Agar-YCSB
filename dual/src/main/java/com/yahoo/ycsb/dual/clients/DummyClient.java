package com.yahoo.ycsb.dual.clients;

import com.yahoo.ycsb.ClientBlueprint;
import com.yahoo.ycsb.Status;

public class DummyClient extends ClientBlueprint {
    @Override
    public void cleanupRead() {

    }

    @Override
    public byte[] read(String key, int keyNum) {
        System.out.println("Read " + keyNum + " " + key);
        return new byte[0];
    }

    @Override
    public Status update(String key, byte[] value) {
        return null;
    }

    @Override
    public Status insert(String key, byte[] value) {
        System.out.println("Insert " + key + " " + value.length + " bytes");
        return Status.OK;
    }

    @Override
    public Status delete(String key) {
        return null;
    }
}
