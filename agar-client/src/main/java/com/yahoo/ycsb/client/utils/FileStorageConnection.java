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

package com.yahoo.ycsb.client.utils;

import com.yahoo.ycsb.Status;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class FileStorageConnection {

    private String directory;

    public FileStorageConnection(String directory) {
        this.directory = directory;
    }

    public Status insert(String key, byte[] bytes) {
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(directory + "/" + key);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        try {
            out.write(bytes);
        } catch (IOException e) {
            //e.printStackTrace();
            System.err.println("Error writing bytes");
            return Status.ERROR;
        }
        try {
            out.close();
        } catch (IOException e) {
            //e.printStackTrace();
        }
        return Status.OK;
    }

    public byte[] read(String key) {
        Path path = Paths.get(directory + "/" + key);
        byte[] data = null;
        try {
            data = Files.readAllBytes(path);
        } catch (IOException e) {
            System.err.println("Error reading bytes");
            //e.printStackTrace();
        }
        return data;
    }
}
