package com.yahoo.ycsb.dual.connections;

import com.yahoo.ycsb.Status;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by Raluca on 28.04.16.
 */
public class FileStorage {

    private String directory;

    public FileStorage(String directory) {
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
