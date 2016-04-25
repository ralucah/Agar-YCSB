package com.yahoo.ycsb.dual.tests;

import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.common.memcached.MemcachedConnection;

import java.io.UnsupportedEncodingException;

/**
 * Created by Raluca on 22.04.16.
 */
public class MemDelTest {
    public static void main(String args[]) {
        try {
            MemcachedConnection memcachedConnection = new MemcachedConnection("127.0.0.1:11211");
            memcachedConnection.insert("mykey", "hello".getBytes());
            try {
                System.out.println(new String(memcachedConnection.read("mykey"), "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
            }
            memcachedConnection.delete("mykey");
            byte[] read = memcachedConnection.read("mykey");
            if (read == null)
                System.out.println("null");
            memcachedConnection.cleanup();
        } catch (ClientException e) {
            e.printStackTrace();
        }
    }
}
