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

package com.yahoo.ycsb.client;

import com.yahoo.ycsb.ClientBlueprint;
import com.yahoo.ycsb.Status;

public class DummyClient extends ClientBlueprint {
    @Override
    public void cleanupRead() {

    }

    @Override
    public byte[] read(String key, int keyNum) {
        System.out.println("Read " + keyNum + " " + key);
        return new byte[0];
    }

    @Override
    public Status update(String key, byte[] value) {
        return null;
    }

    @Override
    public Status insert(String key, byte[] value) {
        System.out.println("Insert " + key + " " + value.length + " bytes");
        return Status.OK;
    }

    @Override
    public Status delete(String key) {
        return null;
    }
}
