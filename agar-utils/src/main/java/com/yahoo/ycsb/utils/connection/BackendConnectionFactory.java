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

/**
 * Creates a BackendConnection layer by dynamically classloading the specified BackendConnection class.
 */
public class BackendConnectionFactory {
    public static BackendConnection newBackendConnection(String backendConnName) {
        ClassLoader classLoader = BackendConnectionFactory.class.getClassLoader();
        BackendConnection ret = null;

        try {
            Class cacheManagerClass = classLoader.loadClass(backendConnName);
            //System.out.println("cacheManagerClass.getName() = " + cacheManagerClass.getName());
            ret = (BackendConnection) cacheManagerClass.newInstance();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return ret;
    }
}
