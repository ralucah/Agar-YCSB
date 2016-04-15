/**
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.
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

package com.yahoo.ycsb;

import com.yahoo.ycsb.measurements.Measurements;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;

/**
 * Wrapper around a "real" DB that measures latencies and counts return codes.
 * Also reports latency separately between OK and failed operations.
 */
public class ClientWrapper extends ClientBlueprint {
    private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY =
        "reportlatencyforeacherror";
    private static final String REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT =
        "false";
    private static final String LATENCY_TRACKED_ERRORS_PROPERTY =
        "latencytrackederrors";
    private ClientBlueprint _clientBlueprint;
    private Measurements _measurements;
    private boolean reportLatencyForEachError = false;
    private HashSet<String> latencyTrackedErrors = new HashSet<String>();

    public ClientWrapper(ClientBlueprint clientBlueprint) {
        _clientBlueprint = clientBlueprint;
        _measurements = Measurements.getMeasurements();
    }

    /**
     * Get the set of properties for this DB.
     */
    public Properties getProperties() {
        return _clientBlueprint.getProperties();
    }

    /**
     * Set the properties for this DB.
     */
    public void setProperties(Properties p) {
        _clientBlueprint.setProperties(p);
    }

    /**
     * Initialize any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void init() throws ClientException {
        _clientBlueprint.init();

        this.reportLatencyForEachError = Boolean.parseBoolean(getProperties().
            getProperty(REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY,
                REPORT_LATENCY_FOR_EACH_ERROR_PROPERTY_DEFAULT));

        if (!reportLatencyForEachError) {
            String latencyTrackedErrors = getProperties().getProperty(
                LATENCY_TRACKED_ERRORS_PROPERTY, null);
            if (latencyTrackedErrors != null) {
                this.latencyTrackedErrors = new HashSet<String>(Arrays.asList(
                    latencyTrackedErrors.split(",")));
            }
        }

    /*System.err.println("DBWrapper: report latency for each error is " +
        this.reportLatencyForEachError + " and specific error codes to track" +
        " for latency are: " + this.latencyTrackedErrors.toString());*/
    }

    /**
     * Cleanup any state for this DB.
     * Called once per DB instance; there is one DB instance per client thread.
     */
    public void cleanup() throws ClientException {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        _clientBlueprint.cleanup();
        long en = System.nanoTime();
        measure("CLEANUP", Status.OK, ist, st, en);
    }

    /**
     * Read a record from the database. Each field/value pair from the result
     * will be stored in a HashMap.
     *
     * @param key The record key of the record to read.
     * @return The result of the operation.
     */
    public byte[] read(String key) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        byte[] res = _clientBlueprint.read(key);
        long en = System.nanoTime();
        Status status = Status.OK;
        if (res == null)
            status = Status.ERROR;
        measure("READ", status, ist, st, en);
        _measurements.reportStatus("READ", status);
        return res;
    }

    private void measure(String op, Status result, long intendedStartTimeNanos,
                         long startTimeNanos, long endTimeNanos) {
        String measurementName = op;
        if (result != Status.OK) {
            if (this.reportLatencyForEachError ||
                this.latencyTrackedErrors.contains(result.getName())) {
                measurementName = op + "-" + result.getName();
            } else {
                measurementName = op + "-FAILED";
            }
        }
        _measurements.measure(measurementName,
            (int) ((endTimeNanos - startTimeNanos) / 1000));
        _measurements.measureIntended(measurementName,
            (int) ((endTimeNanos - intendedStartTimeNanos) / 1000));
    }

    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param key The record key of the record to write.
     * @return The result of the operation.
     */
    public Status update(String key, byte[] value) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _clientBlueprint.update(key, value);
        long en = System.nanoTime();
        measure("UPDATE", res, ist, st, en);
        _measurements.reportStatus("UPDATE", res);
        return res;
    }

    /**
     * Insert a record in the database. Any field/value pairs in the specified
     * values HashMap will be written into the record with the specified
     * record key.
     *
     * @param key The record key of the record to insert.
     * @return The result of the operation.
     */
    public Status insert(String key, byte[] value) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _clientBlueprint.insert(key, value);
        long en = System.nanoTime();
        measure("INSERT", res, ist, st, en);
        _measurements.reportStatus("INSERT", res);
        return res;
    }

    /**
     * Delete a record from the database.
     *
     * @param key The record key of the record to delete.
     * @return The result of the operation.
     */
    public Status delete(String key) {
        long ist = _measurements.getIntendedtartTimeNs();
        long st = System.nanoTime();
        Status res = _clientBlueprint.delete(key);
        long en = System.nanoTime();
        measure("DELETE", res, ist, st, en);
        _measurements.reportStatus("DELETE", res);
        return res;
    }
}
