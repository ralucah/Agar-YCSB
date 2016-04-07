package com.yahoo.ycsb.generator;

import com.yahoo.ycsb.Utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by Raluca on 07.04.16.
 */
public class SlashdotGenerator extends IntegerGenerator {

    private final double skew = 1;
    private final int constant = 1000000;
    private double[] buckets;
    private double area = 0;
    private volatile int offset = 0;
    private int delay = 50;
    private int recordCount;

    public SlashdotGenerator(final int recordCount) {
        this.recordCount = recordCount;
        ZipfGenerator zipf = new ZipfGenerator(recordCount, skew);

        buckets = new double[recordCount];
        for (int i = 1; i <= recordCount; i++)
            buckets[i - 1] = zipf.getProbability(i);

        for (int i = 0; i < recordCount; i++)
            area += buckets[i];

        ScheduledExecutorService exec = Executors.newSingleThreadScheduledExecutor();
        exec.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                offset = (offset + 1) % recordCount;
            }
        }, delay, delay, TimeUnit.MILLISECONDS);

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

    /*public static void main(String[] args) throws IOException {
        SlashdotGenerator slashgen = new SlashdotGenerator(10);
        //slashgen.printBuckets();

        System.out.println("nextInt(): ");
        for(int i = 0; i < 1000000; i++)
            System.out.println(slashgen.nextInt());
    }*/
}