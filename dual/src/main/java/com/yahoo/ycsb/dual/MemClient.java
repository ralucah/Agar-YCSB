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

package com.yahoo.ycsb.dual;

import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.FailureMode;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.GetFuture;
import net.spy.memcached.internal.OperationFuture;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

// TODO Assumption: this class represents one connection to one memcached host
// TODO table does not matter, only key does
public class MemClient {
    public static final String SHUTDOWN_TIMEOUT_MILLIS_PROPERTY = "memcached.shutdownTimeoutMillis";
    public static final String DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = "30000";
    public static final String OBJECT_EXPIRATION_TIME_PROPERTY = "memcached.objectExpirationTime";
    public static final String DEFAULT_OBJECT_EXPIRATION_TIME = String.valueOf(Integer.MAX_VALUE);
    public static final String CHECK_OPERATION_STATUS_PROPERTY = "memcached.checkOperationStatus";
    public static final String CHECK_OPERATION_STATUS_DEFAULT = "true";
    public static final String READ_BUFFER_SIZE_PROPERTY = "memcached.readBufferSize";
    public static final String DEFAULT_READ_BUFFER_SIZE = "3000000";
    public static final String OP_TIMEOUT_PROPERTY = "memcached.opTimeoutMillis";
    public static final String DEFAULT_OP_TIMEOUT = "60000";
    public static final String FAILURE_MODE_PROPERTY = "memcached.failureMode";
    public static final FailureMode FAILURE_MODE_PROPERTY_DEFAULT = FailureMode.Redistribute;
    protected static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String TEMPORARY_FAILURE_MSG = "Temporary failure";
    private static final String CANCELLED_MSG = "cancelled";
    private static Logger logger = Logger.getLogger(MemClient.class);

    private static boolean checkOperationStatus;
    private static long shutdownTimeoutMillis;
    private static int objectExpirationTime;
    private static int bufferSize;
    private static int opTimeout;
    private static String failureString;
    private static boolean init = true;

    private MemcachedClient client;
    private String host;

    public MemClient(String hostsStr) throws DBException {
        this.host = hostsStr;
        // turn off logging for spy memcached
        System.setProperty("net.spy.log.LoggerImpl", "net.spy.memcached.compat.log.Log4JLogger");
        org.apache.log4j.Logger.getLogger("net.spy.memcached").setLevel(Level.OFF);

        if (MemClient.init == true) {
            init();
            MemClient.init = false;
        }

        try {
            client = createMemcachedClient();
        } catch (Exception e) {
            throw new DBException(e);
        }
    }

    private void init() {
        InputStream propFile = MemClient.class.getClassLoader().getResourceAsStream("memcached.properties");
        Properties props = new Properties();
        try {
            props.load(propFile);
        } catch (IOException e) {
            e.printStackTrace();
        }

        checkOperationStatus = Boolean.parseBoolean(
            props.getProperty(CHECK_OPERATION_STATUS_PROPERTY, CHECK_OPERATION_STATUS_DEFAULT));
        objectExpirationTime = Integer.parseInt(
            props.getProperty(OBJECT_EXPIRATION_TIME_PROPERTY, DEFAULT_OBJECT_EXPIRATION_TIME));
        shutdownTimeoutMillis = Integer.parseInt(
            props.getProperty(SHUTDOWN_TIMEOUT_MILLIS_PROPERTY, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS));

        bufferSize = Integer.parseInt(
            props.getProperty(READ_BUFFER_SIZE_PROPERTY, DEFAULT_READ_BUFFER_SIZE));

        opTimeout = Integer.parseInt(
            props.getProperty(OP_TIMEOUT_PROPERTY, DEFAULT_OP_TIMEOUT));

        failureString = props.getProperty(FAILURE_MODE_PROPERTY);
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

    protected Status getReturnCode(OperationFuture<Boolean> future) {
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

    public void cleanup() throws DBException {
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
