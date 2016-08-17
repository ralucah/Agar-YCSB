/**
 * Copyright (c) 2014-2015 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/* DEMO */

package com.yahoo.ycsb.utils.connection;

import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class LocalFSConnection extends BackendConnection {
    private static Logger logger = Logger.getLogger(LocalFSConnection.class);
    private String bucket;
    private String region;
    private File bucketDir;

    public LocalFSConnection(String bucket, String region, String endPoint) throws ClientException {
        super(bucket, region, endPoint);
        this.bucket = bucket;
        this.region = region;
        bucketDir = new File("/tmp/agar/" + region + "/" + bucket);
        bucketDir.mkdirs();
    }

    public Status insert(String key, byte[] bytes) {
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(bucketDir + "/" + key);
            out.write(bytes);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return Status.OK;
    }

    public String getRegion() {
        return region;
    }

    public byte[] read(String key) throws InterruptedException {
        FileInputStream in = null;
        byte[] data  = null;
        try {
            in = new FileInputStream(bucketDir + "/" + key);
            int sizeOfFile = in.available();
            data = new byte[sizeOfFile];
            int offset = 0;
            while (offset < sizeOfFile) {
                int chunk_size;

                //read in 4k chunks
                chunk_size = sizeOfFile - offset > 4096 ? 4096 : sizeOfFile - offset;

                int nr_bytes_read = in.read(data, offset, sizeOfFile - offset);
                offset = offset + nr_bytes_read;

                if (Thread.interrupted()) {
                    //System.out.println("interrupt " + key);
                    in.close();
                    throw new InterruptedException();
                }
            }
            //System.out.println(data.length);
        } catch (FileNotFoundException e) {
            //.printStackTrace();
            System.err.println("Error creating file input stream");
            //System.exit(1);
        } catch (IOException e) {
            System.err.println("Error reading bytes");
            //e.printStackTrace();
        } finally {
            try {
                in.close();
            } catch (IOException e) {
                //e.printStackTrace();
                System.err.println("Error closing file input stream");
            }
        }
        return data;
        /*Path path = Paths.get(bucketDir + "/" + key);
        byte[] data = null;
        try {
            data = Files.readAllBytes(path);
        } catch (IOException e) {
            System.err.println("Error reading bytes");
            e.printStackTrace();
        }
        return data;*/
    }
}
