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

package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SlashdotGenerator extends IntegerGenerator {
    private final int constant = 1000000;
    private double skew;
    private int delay;
    private double[] buckets;
    private double area = 0;
    private int recordCount;

    private volatile int offset = 0;

    public SlashdotGenerator(final int recordCount, final double skew, final int delay) {
        this.recordCount = recordCount;
        this.skew = skew;
        this.delay = delay;

        ZipfGenerator zipf = new ZipfGenerator(recordCount, skew);

        buckets = new double[recordCount];
        for (int i = 1; i <= recordCount; i++)
            buckets[i - 1] = zipf.getProbability(i);

        for (int i = 0; i < recordCount; i++)
            area += buckets[i];

        if (delay > 0) {
            ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
            exec.scheduleAtFixedRate(new Runnable() {
                @Override
                public void run() {
                    //System.err.println("offset!");
                    offset = (offset + 1) % recordCount;
                }
            }, delay, delay, TimeUnit.MILLISECONDS);
        }

    }

    public static void main(String[] args) {
        SlashdotGenerator slashgen = new SlashdotGenerator(100, 1.0, 10000);
        //slashgen.printBuckets();

        //System.out.println("nextInt(): ");
        for (int i = 0; i < 20000; i++)
            System.out.println(slashgen.nextInt());
    }

    @Override
    public int nextInt() {
        //if (x++ % 100 == 0) offset++;
        double number = Utils.random().nextInt((int) (constant * area));
        number = number / constant;

        int i;
        for (i = 0; i < (buckets.length - 1); i++) {
            number -= buckets[i];
            if (number <= 0) {
                return (int) Math.floorMod(i - offset, recordCount);
            }
        }
        return (int) Math.floorMod(i - offset, recordCount);
    }

    public void printBuckets() {
        System.out.println("Za buckets:");
        for (int i = 0; i < buckets.length; i++)
            System.out.println(i + " " + buckets[i]);
    }

    @Override
    public double mean() {
        return 0.0;
    }
}