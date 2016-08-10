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

package com.yahoo.ycsb.client.tests;

import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.client.utils.ClientUtils;
import com.yahoo.ycsb.utils.memcached.MemcachedConnection;

import java.util.Random;

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
