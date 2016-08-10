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

// An ecblock represents a block of erasure coded data
public class ECBlock {
    private String baseKey; // base key of the data the block is part of
    private int id; // id of block within the data
    private byte[] bytes; // encoded bytes
    private Storage storage; // where the block was read from: cache or backend; used for stats

    public ECBlock(String baseKey, int id, byte[] bytes, Storage storage) {
        this.baseKey = baseKey;
        this.id = id;
        this.bytes = bytes;
        this.storage = storage;
    }

    public int getId() {
        return id;
    }

    public String getBaseKey() {
        return baseKey + id;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public Storage getStorage() {
        return storage;
    }

}
