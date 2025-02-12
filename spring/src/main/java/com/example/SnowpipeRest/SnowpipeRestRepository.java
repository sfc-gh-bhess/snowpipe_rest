package com.example.SnowpipeRest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.streaming.internal.MemoryInfoProvider;
import net.snowflake.ingest.streaming.internal.MemoryInfoProviderFromRuntime;
import net.snowflake.ingest.utils.ParameterProvider;

import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.LinkedHashMap;
import java.util.UUID;
import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SnowpipeRestRepository {
    static Logger logger = LoggerFactory.getLogger(SnowpipeRestRepository.class);
    
    private ObjectMapper objectMapper = new ObjectMapper();
    private SnowflakeStreamingIngestClient snowpipe_client;
    private String suffix = UUID.randomUUID().toString();
    private ConcurrentMap<String,SnowpipeRestChannel> sp_channels = new ConcurrentHashMap<String,SnowpipeRestChannel>();
    private MemoryInfoProvider memoryInfoProvider = MemoryInfoProviderFromRuntime.getInstance();

    @Value("${snowpiperest.batch_size}")
    private int batch_size;

    @Value("${snowpiperest.purge_rate}")
    private int purge_rate;

    @Value("${snowpiperest.disable_buffering}")
    private int disable_buffering;

    @Value("${snowpiperest.thread_in_key}")
    private int thread_in_key;

    @Value("${snowpiperest.throttle_threshold_pct}")
    private int throttle_threshold_pct;

    @Value("${snowpiperest.throttle_delay_millis}")
    private int throttle_delay_millis;

    @Value("${snowpiperest.throttle_backoff_factor}")
    private double throttle_backoff_factor;

    @Value("${snowflake.url}")
    private String snowflake_url;

    @Value("${snowflake.user}")
    private String snowflake_user;

    @Value("${snowflake.role}")
    private String snowflake_role;

    @Value("${snowflake.private_key}")
    private String snowflake_private_key;

    public SnowpipeRestRepository() {
    }

    //------------------------------
    // Snowpipe Streaming Parameters
    @Value("${snowpiperest.insert_throttle_threshold_in_percentage}")
    private int INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE;

    @Value("${snowpiperest.max_client_lag}")
    private long MAX_CLIENT_LAG;

    @Value("${snowpiperest.max_channel_size_in_bytes}")
    private long MAX_CHANNEL_SIZE_IN_BYTES;

    @Value("${snowpiperest.max_chunk_size_in_bytes}")
    private long MAX_CHUNK_SIZE_IN_BYTES;

    @Value("${snowpiperest.io_time_cpu_ratio}")
    private long IO_TIME_CPU_RATIO;

    @Value("${snowpiperest.bdec_parquet_compression_algorithm}")
    private String BDEC_PARQUET_COMPRESSION_ALGORITHM;

    public void setParameters(Properties props) {
        logger.info(String.format("Setting Snowpipe Parameters"));
        logger.info(String.format("INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE: %d", this.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE));
        logger.info(String.format("MAX_CLIENT_LAG: %d", this.MAX_CLIENT_LAG));
        logger.info(String.format("MAX_CHANNEL_SIZE_IN_BYTES: %d", this.MAX_CHANNEL_SIZE_IN_BYTES));
        logger.info(String.format("MAX_CHUNK_SIZE_IN_BYTES: %d", this.MAX_CHUNK_SIZE_IN_BYTES));
        logger.info(String.format("BDEC_PARQUET_COMPRESSION_ALGORITHM: %s", this.BDEC_PARQUET_COMPRESSION_ALGORITHM));
        props.put(ParameterProvider.BDEC_PARQUET_COMPRESSION_ALGORITHM, this.BDEC_PARQUET_COMPRESSION_ALGORITHM);
        if (this.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE > 0)
            props.put(ParameterProvider.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE, this.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE);
        if (this.MAX_CLIENT_LAG > 0)
            props.put(ParameterProvider.MAX_CLIENT_LAG, this.MAX_CLIENT_LAG);
        if (this.MAX_CHANNEL_SIZE_IN_BYTES > 0)
            props.put(ParameterProvider.MAX_CHANNEL_SIZE_IN_BYTES, this.MAX_CHANNEL_SIZE_IN_BYTES);
        if (this.MAX_CHUNK_SIZE_IN_BYTES > 0)
            props.put(ParameterProvider.MAX_CHUNK_SIZE_IN_BYTES, this.MAX_CHUNK_SIZE_IN_BYTES);
        if (this.IO_TIME_CPU_RATIO > 0)
            props.put(ParameterProvider.IO_TIME_CPU_RATIO, this.IO_TIME_CPU_RATIO);
        props.put(ParameterProvider.ENABLE_SNOWPIPE_STREAMING_METRICS, true);
        if (this.disable_buffering != 0)
            logger.info("Disabling buffering");
        if (this.thread_in_key != 0)
            logger.info("Setting up channel-per-thread");
        if (this.throttle_threshold_pct >= 0) {
            logger.info(String.format("Throttle threshold percent: %d", this.throttle_threshold_pct));
            logger.info(String.format("Initial throttle delay: %d", this.throttle_delay_millis));
            logger.info(String.format("Throttle backoff factor: %f", this.throttle_backoff_factor));
        }
    }
    //------------------------------

    @PostConstruct
    private void init() {
        // get Snowflake credentials and put them in props
        java.util.Properties props = new Properties();
        props.put("url", snowflake_url);
        props.put("user", snowflake_user);
        props.put("role", snowflake_role);
        props.put("private_key", snowflake_private_key);
        setParameters(props);
        // Connect to Snowflake with credentials.
        try {
            // Make Snowflake Streaming Ingest Client
            this.snowpipe_client = SnowflakeStreamingIngestClientFactory.builder("SNOWPIPE_REST_" + this.suffix)
                    .setProperties(props).build();
            startReporter(INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE);
        } catch (Exception e) {
            // Handle Exception for Snowpipe Streaming objects
            throw new RuntimeException(e);
        }
    }

    @PreDestroy
    private void cleanup() {
        for (SnowpipeRestChannel c : sp_channels.values())
            c.cleanup();
    }

    private String makeKey(String database, String schema, String table) {
        if (this.thread_in_key == 0)
            return String.format("%s.%s.%s", database.toUpperCase(), schema.toUpperCase(), table.toUpperCase());
            return String.format("%s:%s.%s.%s", Thread.currentThread().getName(), database.toUpperCase(), schema.toUpperCase(), table.toUpperCase());
    }

    // Gets or creates and stores Snowflake Streaming Ingest Channel for the table
    private SnowpipeRestChannel getSnowpipeRestChannel(String database, String schema, String table) {
        if (null == database)
            throw new RuntimeException("Must specify database");
        if (null == schema)
            throw new RuntimeException("Must specify schema");
        if (null == table)
            throw new RuntimeException("Must specify table");
        String key = makeKey(database, schema, table);
        if (this.sp_channels.containsKey(key))
            return this.sp_channels.get(key);

        try {
            OpenChannelRequest request1 = OpenChannelRequest.builder("SNOWPIPE_REST_CHANNEL_" + this.suffix + "_" + key)
                    .setDBName(database)
                    .setSchemaName(schema)
                    .setTableName(table)
                    .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                    .build();
            openSnowpipeRestChannel(key, request1);
            return this.sp_channels.get(key);
        } catch (Exception e) {
            // Handle Exception for Snowpipe Streaming objects
            e.printStackTrace();
            throw new SnowpipeRestTableNotFoundException(String.format("Table not found (or no permissions): %s.%s.%s", database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()));
        }
    }

    // Let's have only one thread open the channel
    private synchronized void openSnowpipeRestChannel(String key, OpenChannelRequest request1) {
        this.sp_channels.computeIfAbsent(key, k -> new SnowpipeRestChannel(key, this.snowpipe_client, request1, this.purge_rate, 
                                            (this.disable_buffering != 0), (this.thread_in_key == 0)));
    }

    private boolean hasLowRuntimeMemory() {
        if (this.throttle_threshold_pct < 0)
            return false;
        long maxMemory = this.memoryInfoProvider.getMaxMemory();
        long freeMemoryInBytes = memoryInfoProvider.getFreeMemory();
        boolean hasLowRuntimeMemory =
        //     freeMemoryInBytes < insertThrottleThresholdInBytes &&
                freeMemoryInBytes * 100 / maxMemory < this.throttle_threshold_pct;
        return hasLowRuntimeMemory;
    }
    private void throttleIfNecessary() {
        int retry = 0;
        int sleep_ms = this.throttle_delay_millis;
        int total_sleep = 0;
        while (hasLowRuntimeMemory()) {
            try {
                Thread.sleep(sleep_ms);
                retry++;
                total_sleep += sleep_ms;
                sleep_ms = (int)((double)sleep_ms * this.throttle_backoff_factor);
            }        
            catch (InterruptedException e) {
                logger.info(e.getMessage());
            }
        }
        if (retry > 0) {
            logger.info(String.format("saveToSnowflake throttled: times=%d, total_ms=%d", retry, total_sleep));
        }
    }

    public SnowpipeInsertResponse saveToSnowflake(String database, String schema, String table, String body) {
        throttleIfNecessary();
        // Parse body
        List<Object> rowStrings;
        List<Map<String,Object>> rows;
        try {
            // Parse JSON body
            JsonNode jsonNode = this.objectMapper.readTree(body);
            // List of strings for error reporting
            rowStrings = objectMapper.convertValue(jsonNode, new TypeReference<List<Object>>() {});
            // List of Map<String,Object> for inserting
            rows = objectMapper.convertValue(jsonNode, new TypeReference<List<Map<String, Object>>>(){});
        }
        catch (JsonProcessingException je) {
            // throw new RuntimeException("Unable to parse body as list of JSON strings.");
            throw new SnowpipeRestJsonParseException("Unable to parse body as list of JSON strings.");
        }

        // Get ingest channel
        SnowpipeRestChannel sp_channel = this.getSnowpipeRestChannel(database, schema, table); // Need to get the channel so the buffer and count are created

        // Issue the insert
        List<List<Map<String,Object>>> batches = Collections.singletonList(rows); // = Lists.partition(rows, batch_size);
        List<List<Object>> batchStrings = Collections.singletonList(rowStrings); // = Lists.partition(rowStrings, batch_size);
        if (this.batch_size > 0) {
            logger.info(String.format("Batching..."));
            batches = Lists.partition(rows, batch_size);
            batchStrings = Lists.partition(rowStrings, batch_size);    
        }
        SnowpipeInsertResponse sp_resp = sp_channel.insertBatches(batches, batchStrings);
        return sp_resp;
    }

    // Metrics Reporting
    private ScheduledExecutorService scheduler;
    private void startReporter(int INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE) {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        Runnable task = new JmxPrinter(INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE); // SnowpipeRestRepository::fetchAndPrintMetrics;
        scheduler.scheduleAtFixedRate(task, 30, 30, TimeUnit.SECONDS);
    }

    class JmxPrinter implements Runnable {
        static Logger logger = LoggerFactory.getLogger(JmxPrinter.class);
        int insertThrottleThresholdInPercentage = -1;

        public JmxPrinter(int INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE) {
            this.insertThrottleThresholdInPercentage = INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE;
        }

        @Override
        public void run() {
            logger.info("Fetching JMX metrics");
            try {
                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                // Latency
                fetchAndPrintMetrics(mBeanServer, "snowflake.ingest.sdk:clientName=SNOWPIPE_REST_*,name=latency.*", "JMX latency: ");
    
                // Throughput
                fetchAndPrintMetrics(mBeanServer, "snowflake.ingest.sdk:clientName=SNOWPIPE_REST_*,name=throughput.*", "JMX throughput: ");
    
                // Blob
                fetchAndPrintMetrics(mBeanServer, "snowflake.ingest.sdk:clientName=SNOWPIPE_REST_*,name=blob.*", "JMX blob: ");
    
                // Memory Info
                MemoryInfoProvider memoryInfoProvider = MemoryInfoProviderFromRuntime.getInstance();
                long maxMemory = memoryInfoProvider.getMaxMemory();
                long freeMemoryInBytes = memoryInfoProvider.getFreeMemory();
                boolean hasLowRuntimeMemory =
                //     freeMemoryInBytes < insertThrottleThresholdInBytes &&
                        freeMemoryInBytes * 100 / maxMemory < insertThrottleThresholdInPercentage;
                logger.info(String.format("JMX memoryInfoProvider: maxMemory=%,d, freeMemoryInBytes=%,d, insertThrottleThresholdInPercentage=%d, hasLowRuntimeMemory=%B", maxMemory, freeMemoryInBytes, insertThrottleThresholdInPercentage, hasLowRuntimeMemory));
            } catch (Exception e) {
                logger.error("Unable to log JMX metrics", e);
            }
        }
    
        private void fetchAndPrintMetrics(MBeanServer mBeanServer, String queryNameString, String logPrefix) throws Exception {
            ObjectName queryName = new ObjectName(queryNameString);
            Set<ObjectName> names = mBeanServer.queryNames(queryName, null);
            if (names.isEmpty()) {
                logger.info("No JMX metrics found: " + queryNameString);
            }
            else {
                for (ObjectName name: names) {
                    MBeanInfo info = mBeanServer.getMBeanInfo(name);
                    MBeanAttributeInfo[] attrInfo = info.getAttributes();
                    Map<String, Object> attributes = new LinkedHashMap<>();
                    for (MBeanAttributeInfo attr : attrInfo) {
                        if (attr.isReadable())
                            attributes.put(attr.getName(), mBeanServer.getAttribute(name, attr.getName()));
                    }
                    // Constructing the key-value pair string dynamically
                    StringBuilder kvPairs = new StringBuilder();
                    kvPairs.append("metric=" + name.getCanonicalName());
                    for (Map.Entry<String, Object> entry : attributes.entrySet()) {
                        if (kvPairs.length() > 0) {
                            kvPairs.append(", ");
                        }
                        kvPairs.append(entry.getKey()).append("=").append(entry.getValue());
                    }
                    logger.info(logPrefix + kvPairs);
                }
            }
        }
    }
}
