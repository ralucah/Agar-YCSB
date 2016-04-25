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

package com.yahoo.ycsb.common.memcached;

import com.yahoo.ycsb.ClientException;
import com.yahoo.ycsb.Status;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

// TODO assumption: one connection to one memcached host
// TODO table does not matter, only key does
public class MemcachedConnection {
    public static final FailureMode FAILURE_MODE_PROPERTY_DEFAULT = FailureMode.Redistribute;
    private static final String TEMPORARY_FAILURE_MSG = "Temporary failure";
    private static final String CANCELLED_MSG = "cancelled";
    private static Logger logger = Logger.getLogger(MemcachedConnection.class);

    private static boolean checkOperationStatus = true;
    private static long shutdownTimeoutMillis = 30000;
    private static int objectExpirationTime = 2147483647;
    private static int bufferSize = 3000000;
    private static int opTimeout = 60000;
    private static String failureString = "Redistribute"; //  `Redistribute`, `Retry`, or `Cancel`.

    private MemcachedClient client;
    private String host;

    public MemcachedConnection(String hostsStr) throws ClientException {
        this.host = hostsStr;
        // turn off logging for spy memcached
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
        org.apache.log4j.Logger.getLogger("net.spy.memcached").setLevel(Level.OFF);

        try {
            client = createMemcachedClient();
        } catch (Exception e) {
            throw new ClientException(e);
        }
    }

    public String getHost() {
        return host;
    }

    protected net.spy.memcached.MemcachedClient memcachedClient() {
        return client;
    }

    protected MemcachedClient createMemcachedClient() throws Exception {
        ConnectionFactoryBuilder connectionFactoryBuilder = new ConnectionFactoryBuilder();

        connectionFactoryBuilder.setReadBufferSize(bufferSize);

        connectionFactoryBuilder.setOpTimeout(opTimeout);

        connectionFactoryBuilder.setFailureMode(
            failureString == null ? FAILURE_MODE_PROPERTY_DEFAULT
                : FailureMode.valueOf(failureString));

        // Note: this only works with IPv4 addresses due to its assumption of
        // ":" being the separator of hostname/IP and port; this is not the case
        // when dealing with IPv6 addresses.
        List<InetSocketAddress> addresses = new ArrayList<InetSocketAddress>();
        String[] hosts = host.split(","); //getProperties().getProperty(HOSTS_PROPERTY).split(",");
        for (String address : hosts) {
            int colon = address.indexOf(":");
            int port = 11211; // default
            String host = address;
            if (colon != -1) {
                port = Integer.parseInt(address.substring(colon + 1));
                host = address.substring(0, colon);
            }
            addresses.add(new InetSocketAddress(host, port));
        }
        return new net.spy.memcached.MemcachedClient(
            connectionFactoryBuilder.build(), addresses);
    }

    public byte[] read(String key) {
        byte[] bytes = null;
        try {
            GetFuture<Object> future = memcachedClient().asyncGet(key);
            bytes = (byte[]) future.get();
        } catch (Exception e) {
            logger.error("Error encountered for key: " + key, e);
        }
        return bytes;
    }

    public Status insert(String key, byte[] bytes) {
        try {
            OperationFuture<Boolean> future =
                memcachedClient().add(key, objectExpirationTime, bytes);
            return getReturnCode(future);
        } catch (Exception e) {
            logger.error("Error inserting value", e);
            return Status.ERROR;
        }
    }

    public Status delete(String key) {
        try {
            OperationFuture<Boolean> future = memcachedClient().delete(key);
            return getReturnCode(future);
        } catch (Exception e) {
            logger.error("Error deleting value", e);
            return Status.ERROR;
        }
    }

    private Status getReturnCode(OperationFuture<Boolean> future) {
        if (!checkOperationStatus) {
            return Status.OK;
        }
        if (future.getStatus().isSuccess()) {
            return Status.OK;
        } else if (TEMPORARY_FAILURE_MSG.equals(future.getStatus().getMessage())) {
            return new Status("TEMPORARY_FAILURE", TEMPORARY_FAILURE_MSG);
        } else if (CANCELLED_MSG.equals(future.getStatus().getMessage())) {
            return new Status("CANCELLED_MSG", CANCELLED_MSG);
        }
        return new Status("ERROR", future.getStatus().getMessage());
    }

    public void cleanup() throws ClientException {
        if (client != null) {
            memcachedClient().shutdown(shutdownTimeoutMillis, MILLISECONDS);
        }
    }

    public void flush() {
        if (client != null) {
            memcachedClient().flush().isDone();
        }
    }
}
