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

import com.yahoo.ycsb.utils.liberasure.LonghairLib;
import org.apache.log4j.Logger;

import java.util.Random;
import java.util.Set;

public class LonghairTest {
    private static Logger logger = Logger.getLogger(LonghairTest.class);

    private static String createDataSize(int msgSize) {
        StringBuilder sb = new StringBuilder(msgSize);
        for (int i = 0; i < msgSize; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    public static void main(String[] args) throws InterruptedException {
        LonghairLib.k = 4;
        LonghairLib.m = 2;

        if (LonghairLib.Longhair.INSTANCE._cauchy_256_init(2) != 0) {
            System.err.println("Failed to init longhair lib!");
        }

        // test encode
        byte[] original = new byte[4194304];
        new Random().nextBytes(original);
        //byte[] original = "Hello, kitty kitty kitty kitty kitty kitty kitty kitty kitty!".getBytes();
        //try {
        System.out.println(original.length + " " + original[0] + original[4 * 1024 * 1024 - 1]);
        //System.out.println("Original: " + new String(original, "US-ASCII"));
        //} catch (UnsupportedEncodingException e) {
        //    e.printStackTrace();
        //}
        Set<byte[]> blocks = LonghairLib.encode(original);

        blocks.remove(2);
        blocks.remove(2);

        // test decode
        byte[] decoded = LonghairLib.decode(blocks);
        System.out.println(decoded.length + " " + decoded[0] + decoded[4 * 1024 * 1024 - 1]);
        /*try {
            System.out.println("Decoded: " + new String(decoded, "US-ASCII"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }*/
    }
}
