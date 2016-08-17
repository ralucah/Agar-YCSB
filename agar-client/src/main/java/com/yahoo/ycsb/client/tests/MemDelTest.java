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
import com.yahoo.ycsb.utils.connection.MemcachedConnection;

import java.io.UnsupportedEncodingException;

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
