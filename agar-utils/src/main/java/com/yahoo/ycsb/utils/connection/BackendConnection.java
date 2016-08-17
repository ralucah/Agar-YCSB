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

package com.yahoo.ycsb.utils.connection;

import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;

public abstract class BackendConnection {
    public BackendConnection(String bucket, String region, String endPoint) throws ClientException {

    }
    public abstract Status insert(String key, byte[] bytes);
    public abstract byte[] read(String key) throws InterruptedException;
    public abstract String getRegion();
}
