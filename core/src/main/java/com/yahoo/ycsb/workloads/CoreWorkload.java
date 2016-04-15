package com.yahoo.ycsb.workloads;

import com.yahoo.ycsb.*;
import com.yahoo.ycsb.generator.*;
import com.yahoo.ycsb.measurements.Measurements;

import java.io.IOException;
import java.util.Properties;

/**
 * The core benchmark scenario. Represents a set of clients doing simple CRUD operations. The
 * relative proportion of different kinds of operations, and other properties of the workload,
 * are controlled by parameters specified at runtime.

 * Properties to control the client:
 * - fieldlength: the size of each field (default: 100)
 * - readproportion: what proportion of operations should be reads (default: 0.95)
 * - updateproportion: what proportion of operations should be updates (default: 0.05)
 * - insertproportion: what proportion of operations should be inserts (default: 0)
 * - requestdistribution: what distribution should be used to select the records to operate on - uniform, zipfian, hotspot, or latest (default: uniform)
 * - insertorder: should records be inserted in order by key ("ordered"), or in hashed order ("hashed") (default: hashed)
 */
public class CoreWorkload extends Workload {
    // "uniform", "zipfian" (favoring short records), "constant", "histogram".
    // "uniform", "zipfian" or "constant" - max field length given by the fieldlength property
    // "histogram" - read from the filename specified in the "fieldlengthhistogram" property.
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY = "fieldlengthdistribution";
    public static final String FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT = "constant";

    // field length in bytes
    public static final String FIELD_LENGTH_PROPERTY = "fieldlength";
    public static final String FIELD_LENGTH_PROPERTY_DEFAULT = "100";

    // for histogram distribution
    public static final String FIELDLENGTH_HISTOGRAM_FILE_PROPERTY = "fieldlengthhistogram";
    public static final String FIELDLENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT = "hist.txt";

    // proportion of transactions that are read
    public static final String READ_PROPORTION_PROPERTY = "readproportion";
    public static final String READ_PROPORTION_PROPERTY_DEFAULT = "0.95";

    // proportion of transactions that are updates
    public static final String UPDATE_PROPORTION_PROPERTY = "updateproportion";
    public static final String UPDATE_PROPORTION_PROPERTY_DEFAULT = "0.05";

    // proportion of transactions that are inserts
    public static final String INSERT_PROPORTION_PROPERTY = "insertproportion";
    public static final String INSERT_PROPORTION_PROPERTY_DEFAULT = "0.0";

    // distribution of requests across the keyspace: "uniform", "zipfian", "latest", "slashdot"
    public static final String REQUEST_DISTRIBUTION_PROPERTY = "requestdistribution";
    public static final String REQUEST_DISTRIBUTION_PROPERTY_DEFAULT = "uniform";

    // order to insert records: "ordered" or "hashed"
    public static final String INSERT_ORDER_PROPERTY = "insertorder";
    public static final String INSERT_ORDER_PROPERTY_DEFAULT = "hashed";

    // percentage data items that constitute the hot set.
    public static final String HOTSPOT_DATA_FRACTION = "hotspotdatafraction";
    public static final String HOTSPOT_DATA_FRACTION_DEFAULT = "0.2";

    // percentage operations that access the hot set.
    public static final String HOTSPOT_OPN_FRACTION = "hotspotopnfraction";
    public static final String HOTSPOT_OPN_FRACTION_DEFAULT = "0.8";

    // how many times to retry when insertion of a single item to a DB fails.
    public static final String INSERTION_RETRY_LIMIT = "core_workload_insertion_retry_limit";
    public static final String INSERTION_RETRY_LIMIT_DEFAULT = "0";

    // on average, how long to wait between the retries, in seconds.
    public static final String INSERTION_RETRY_INTERVAL = "core_workload_insertion_retry_interval";
    public static final String INSERTION_RETRY_INTERVAL_DEFAULT = "3";

    // skew and delay for Slashdot Generator
    public static final String SLASHDOT_SKEW_PROPERTY = "skew";
    public static final String SLASHDOT_DELAY_PROPERTY = "delay";
    public static final String SLASHDOT_SKEW_DEFAULT = "1.75";
    public static final String SLASHDOT_DELAY_DEFAULT = "10000"; // 10s

    // generator object that produces field lengths. Depends on the properties that start with "FIELD_LENGTH_"
    IntegerGenerator fieldlengthgenerator;
    IntegerGenerator keysequence;
    DiscreteGenerator operationchooser;
    IntegerGenerator keychooser;

    // generator fieldchooser;
    AcknowledgedCounterGenerator transactioninsertkeysequence;
    boolean orderedinserts;
    int insertionRetryLimit;
    int insertionRetryInterval;

    // set to true if want to check correctness of reads. Must also be set to true during loading phase to function.
    private Measurements _measurements = Measurements.getMeasurements();

    protected static IntegerGenerator getFieldLengthGenerator(Properties p) throws WorkloadException {
        IntegerGenerator fieldlengthgenerator;
        String fieldlengthdistribution = p.getProperty(FIELD_LENGTH_DISTRIBUTION_PROPERTY, FIELD_LENGTH_DISTRIBUTION_PROPERTY_DEFAULT);
        int fieldlength = Integer.parseInt(p.getProperty(FIELD_LENGTH_PROPERTY, FIELD_LENGTH_PROPERTY_DEFAULT));
        String fieldlengthhistogram = p.getProperty(FIELDLENGTH_HISTOGRAM_FILE_PROPERTY, FIELDLENGTH_HISTOGRAM_FILE_PROPERTY_DEFAULT);
        if (fieldlengthdistribution.compareTo("constant") == 0) {
            fieldlengthgenerator = new ConstantIntegerGenerator(fieldlength);
        } else if (fieldlengthdistribution.compareTo("uniform") == 0) {
            fieldlengthgenerator = new UniformIntegerGenerator(1, fieldlength);
        } else if (fieldlengthdistribution.compareTo("zipfian") == 0) {
            fieldlengthgenerator = new ZipfianGenerator(1, fieldlength);
        } else if (fieldlengthdistribution.compareTo("histogram") == 0) {
            try {
                fieldlengthgenerator = new HistogramGenerator(fieldlengthhistogram);
            } catch (IOException e) {
                throw new WorkloadException("Couldn't read field length histogram file: " + fieldlengthhistogram, e);
            }
        } else {
            throw new WorkloadException("Unknown field length distribution \"" + fieldlengthdistribution + "\"");
        }
        return fieldlengthgenerator;
    }

    // Initialize the scenario. Called once, in the main client thread, before any operations are started.
    public void init(Properties p) throws WorkloadException {
        fieldlengthgenerator = CoreWorkload.getFieldLengthGenerator(p);

        double readproportion = Double.parseDouble(p.getProperty(READ_PROPORTION_PROPERTY, READ_PROPORTION_PROPERTY_DEFAULT));
        double updateproportion = Double.parseDouble(p.getProperty(UPDATE_PROPORTION_PROPERTY, UPDATE_PROPORTION_PROPERTY_DEFAULT));
        double insertproportion = Double.parseDouble(p.getProperty(INSERT_PROPORTION_PROPERTY, INSERT_PROPORTION_PROPERTY_DEFAULT));
        String requestdistrib = p.getProperty(REQUEST_DISTRIBUTION_PROPERTY, REQUEST_DISTRIBUTION_PROPERTY_DEFAULT);
        int insertstart = Integer.parseInt(p.getProperty(INSERT_START_PROPERTY, INSERT_START_PROPERTY_DEFAULT));

        if (p.getProperty(INSERT_ORDER_PROPERTY, INSERT_ORDER_PROPERTY_DEFAULT).compareTo("hashed") == 0) {
            orderedinserts = false;
        } else if (requestdistrib.compareTo("exponential") == 0) {
            double percentile = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_PERCENTILE_PROPERTY, ExponentialGenerator.EXPONENTIAL_PERCENTILE_DEFAULT));
            double frac = Double.parseDouble(p.getProperty(ExponentialGenerator.EXPONENTIAL_FRAC_PROPERTY, ExponentialGenerator.EXPONENTIAL_FRAC_DEFAULT));
            keychooser = new ExponentialGenerator(percentile, frac);
        } else {
            orderedinserts = true;
        }

        keysequence = new CounterGenerator(insertstart);
        operationchooser = new DiscreteGenerator();
        if (readproportion > 0) {
            operationchooser.addValue(readproportion, "READ");
        }
        if (updateproportion > 0) {
            operationchooser.addValue(updateproportion, "UPDATE");
        }
        if (insertproportion > 0) {
            operationchooser.addValue(insertproportion, "INSERT");
        }

        transactioninsertkeysequence = new AcknowledgedCounterGenerator(1);
        if (requestdistrib.equals("slashdot")) {
            int recordCount = Integer.parseInt(p.getProperty(Client.RECORD_COUNT_PROPERTY, Client.DEFAULT_RECORD_COUNT));
            double skew = Double.parseDouble(p.getProperty(SLASHDOT_SKEW_PROPERTY, SLASHDOT_SKEW_DEFAULT));
            int delay = Integer.parseInt(p.getProperty(SLASHDOT_DELAY_PROPERTY, SLASHDOT_DELAY_DEFAULT));
            keychooser = new SlashdotGenerator(recordCount, skew, delay);
        } else if (requestdistrib.compareTo("uniform") == 0) {
            keychooser = new UniformIntegerGenerator(0, 1);
        } else if (requestdistrib.compareTo("zipfian") == 0) {
            // it does this by generating a random "next key" in part by taking the modulus over the number of keys.
            // If the number of keys changes, this would shift the modulus, and we don't want that to
            // change which keys are popular so we'll actually construct the scrambled zipfian generator
            // with a keyspace that is larger than exists at the beginning of the test. that is, we'll predict
            // the number of inserts, and tell the scrambled zipfian generator the number of existing keys
            // plus the number of predicted keys as the total keyspace. then, if the generator picks a key
            // that hasn't been inserted yet, will just ignore it and pick another key. this way, the size of
            // the keyspace doesn't change from the perspective of the scrambled zipfian generator
            int opcount = Integer.parseInt(p.getProperty(Client.OPERATION_COUNT_PROPERTY));
            int expectednewkeys = (int) ((opcount) * insertproportion * 2.0); // 2 is fudge factor
            keychooser = new ScrambledZipfianGenerator(expectednewkeys);
        } else if (requestdistrib.compareTo("latest") == 0) {
            keychooser = new SkewedLatestGenerator(transactioninsertkeysequence);
        } else if (requestdistrib.equals("hotspot")) {
            double hotsetfraction = Double.parseDouble(p.getProperty(HOTSPOT_DATA_FRACTION, HOTSPOT_DATA_FRACTION_DEFAULT));
            double hotopnfraction = Double.parseDouble(p.getProperty(HOTSPOT_OPN_FRACTION, HOTSPOT_OPN_FRACTION_DEFAULT));
            keychooser = new HotspotIntegerGenerator(0, 1, hotsetfraction, hotopnfraction);
        } else {
            throw new WorkloadException("Unknown request distribution \"" + requestdistrib + "\"");
        }

        insertionRetryLimit = Integer.parseInt(p.getProperty(INSERTION_RETRY_LIMIT, INSERTION_RETRY_LIMIT_DEFAULT));

        insertionRetryInterval = Integer.parseInt(p.getProperty(INSERTION_RETRY_INTERVAL, INSERTION_RETRY_INTERVAL_DEFAULT));
    }

    public String buildKeyName(long keynum) {
        if (!orderedinserts) {
            keynum = Utils.hash(keynum);
        }
        return "key" + keynum;
    }

    private byte[] buildDeterministicValue(String key) {
        int size = fieldlengthgenerator.nextInt();
        /*StringBuilder sb = new StringBuilder(size);
        sb.append(key);
        while (sb.length() < size) {
            sb.append(':');
            sb.append(sb.toString().hashCode());
        }
        sb.setLength(size);
        byte[] value = sb.toString().getBytes();
        return value;*/
        return new byte[size];
    }

    /**
     * Do one insert operation. Because it will be called concurrently from multiple client threads,
     * this function must be thread safe. However, avoid synchronized, or the threads will block waiting
     * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
     * have no side effects other than DB operations.
     */
    public boolean doInsert(ClientBlueprint clientBlueprint, Object threadstate) {
        int keynum = keysequence.nextInt();
        String dbkey = buildKeyName(keynum);
        //System.out.println(keynum + " " + dbkey);
        byte[] value = buildDeterministicValue(dbkey);

        Status status;
        int numOfRetries = 0;
        do {
            status = clientBlueprint.insert(dbkey, value);
            if (status == Status.OK) {
                break;
            }
            // Retry if configured. Without retrying, the load process will fail
            // even if one single insertion fails. User can optionally configure
            // an insertion retry limit (default is 0) to enable retry.
            if (++numOfRetries <= insertionRetryLimit) {
                System.err.println("Retrying insertion, retry count: " + numOfRetries);
                try {
                    // Sleep for a random number between [0.8, 1.2)*insertionRetryInterval.
                    int sleepTime = (int) (1000 * insertionRetryInterval * (0.8 + 0.4 * Math.random()));
                    Thread.sleep(sleepTime);
                } catch (InterruptedException e) {
                    break;
                }

            } else {
                System.err.println("Error inserting, not retrying any more. number of attempts: " + numOfRetries +
                    "Insertion Retry Limit: " + insertionRetryLimit);
                break;

            }
        } while (true);

        return (status == Status.OK);
    }

    /**
     * Do one transaction operation. Because it will be called concurrently from multiple client
     * threads, this function must be thread safe. However, avoid synchronized, or the threads will block waiting
     * for each other, and it will be difficult to reach the target throughput. Ideally, this function would
     * have no side effects other than DB operations.
     */
    public boolean doTransaction(ClientBlueprint clientBlueprint, Object threadstate) {
        String op = operationchooser.nextString();

        if (op.compareTo("READ") == 0) {
            doTransactionRead(clientBlueprint);
        } else if (op.compareTo("UPDATE") == 0) {
            doTransactionUpdate(clientBlueprint);
        } else if (op.compareTo("INSERT") == 0) {
            doTransactionInsert(clientBlueprint);
        }

        return true;
    }

    int nextKeynum() {
        int keynum;
        if (keychooser instanceof ExponentialGenerator) {
            do {
                keynum = transactioninsertkeysequence.lastInt() - keychooser.nextInt();
            } while (keynum < 0);
        } else {
            do {
                keynum = keychooser.nextInt();
            } while (keynum > transactioninsertkeysequence.lastInt());
        }
        return keynum;
    }

    public void doTransactionRead(ClientBlueprint clientBlueprint) {
        // choose a random key
        int keynum = keychooser.nextInt();
        String keyname = buildKeyName(keynum);
        //System.out.println(keynum + " " + keyname);
        byte[] result = clientBlueprint.read(keyname);
    }

    public void doTransactionUpdate(ClientBlueprint clientBlueprint) {
        // choose a random key
        int keynum = nextKeynum();
        String keyname = buildKeyName(keynum);
        byte[] value = buildDeterministicValue(keyname);
        clientBlueprint.update(keyname, value);
    }

    public void doTransactionInsert(ClientBlueprint clientBlueprint) {
        // choose the next key
        int keynum = transactioninsertkeysequence.nextInt();

        try {
            String dbkey = buildKeyName(keynum);
            byte[] value = buildDeterministicValue(dbkey);
            clientBlueprint.insert(dbkey, value);
        } finally {
            transactioninsertkeysequence.acknowledge(keynum);
        }
    }
}
