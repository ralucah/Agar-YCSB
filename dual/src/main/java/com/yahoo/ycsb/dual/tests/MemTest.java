package com.yahoo.ycsb.dual.tests;

import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;
import com.yahoo.ycsb.dual.utils.ClientUtils;

import java.util.Random;

/**
 * Created by Raluca on 27.04.16.
 */
public class MemTest {

    private static void storeAndRead(MemcachedConnection memcachedConnection) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            String key = "key" + i;

            byte[] data = new byte[4194304];
            new Random().nextBytes(data);

            Status insertStatus = memcachedConnection.insert(key, data);
            if (insertStatus == Status.OK)
                System.out.println("Store: " + data.length + " bytes " + ClientUtils.bytesToHash(data));
            else
                System.out.println("Error storing " + key);

            byte[] read = memcachedConnection.read(key);
            if (read == null)
                System.out.println("Error reading " + key);
            else
                System.out.println("Read:: " + read.length + " bytes " + ClientUtils.bytesToHash(read));

            Thread.sleep(500);
        }
    }

    private static void store(MemcachedConnection memcachedConnection) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            String key = "key" + i;

            byte[] data = new byte[4194304];
            new Random().nextBytes(data);

            Status insertStatus = memcachedConnection.insert(key, data);
            if (insertStatus == Status.OK)
                System.out.println("Store: " + data.length + " bytes " + ClientUtils.bytesToHash(data));
            else
                System.out.println("Error storing " + key);
        }
    }

    private static void read(MemcachedConnection memcachedConnection) throws InterruptedException {
        for (int i = 0; i < 10; i++) {
            String key = "key" + i;

            byte[] read = memcachedConnection.read(key);
            if (read == null)
                System.out.println("Error reading " + key);
            else
                System.out.println("Read:: " + read.length + " bytes " + ClientUtils.bytesToHash(read));

            Thread.sleep(500);
        }
    }

    public static void main(String args[]) throws ClientException, InterruptedException {
        MemcachedConnection memcachedConnection = new MemcachedConnection("localhost:11211");

        // store
        store(memcachedConnection);

        // read
        read(memcachedConnection);


        memcachedConnection.cleanup();
    }
}
