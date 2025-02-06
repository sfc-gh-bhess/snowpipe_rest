package com.example.SnowpipeRest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
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

    @Value("${snowpiperest.batch_size}")
    private int batch_size;

    @Value("${snowpiperest.purge_rate}")
    private int purge_rate;

    @Value("${snowpiperest.disable_buffering}")
    private int disable_buffering;

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

    public void setParameters(Properties props) {
        logger.info(String.format("Setting Snowpipe Parameters"));
        logger.info(String.format("INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE: %d", this.INSERT_THROTTLE_THRESHOLD_IN_PERCENTAGE));
        logger.info(String.format("MAX_CLIENT_LAG: %d", this.MAX_CLIENT_LAG));
        logger.info(String.format("MAX_CHANNEL_SIZE_IN_BYTES: %d", this.MAX_CHANNEL_SIZE_IN_BYTES));
        logger.info(String.format("MAX_CHUNK_SIZE_IN_BYTES: %d", this.MAX_CHUNK_SIZE_IN_BYTES));
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
            startReporter();
        } catch (Exception e) {
            // Handle Exception for Snowpipe Streaming objects
            throw new RuntimeException(e);
        }
    }

    private String makeKey(String database, String schema, String table) {
        /*
         * TODO: Should we add the thread ID to the key: Thread.currentThread().getId()
         */
        return String.format("%s.%s.%s", database.toUpperCase(), schema.toUpperCase(), table.toUpperCase());
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
            if (this.sp_channels.containsKey(key))
                this.sp_channels.get(key).get_purger().cancel(true);
            OpenChannelRequest request1 = OpenChannelRequest.builder("SNOWPIPE_REST_CHANNEL_" + this.suffix)
                    .setDBName(database)
                    .setSchemaName(schema)
                    .setTableName(table)
                    .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                    .build();
            this.sp_channels.computeIfAbsent(key, k -> new SnowpipeRestChannel(key, this.snowpipe_client, request1, this.purge_rate));
            return this.sp_channels.get(key);
        } catch (Exception e) {
            // Handle Exception for Snowpipe Streaming objects
            e.printStackTrace();
            throw new SnowpipeRestTableNotFoundException(String.format("Table not found (or no permissions): %s.%s.%s", database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()));
        }
    }

    public SnowpipeInsertResponse saveToSnowflake(String database, String schema, String table, String body) {
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
    private static ScheduledExecutorService scheduler;
    private static void startReporter() {
        scheduler = Executors.newSingleThreadScheduledExecutor();
        Runnable task = SnowpipeRestRepository::fetchAndPrintMetrics;
        scheduler.scheduleAtFixedRate(task, 30, 30, TimeUnit.SECONDS);
    }

    private static void fetchAndPrintMetrics() {
        logger.info("Fetching JMX metrics");
        try {
            MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
            // Latency
            fetchAndPrintMetrics(mBeanServer, "snowflake.ingest.sdk:clientName=SNOWPIPE_REST_*,name=latency.*", "JMX latency: ");

            // Throughput
            fetchAndPrintMetrics(mBeanServer, "snowflake.ingest.sdk:clientName=SNOWPIPE_REST_*,name=throughput.*", "JMX throughput: ");

            // Blob
            fetchAndPrintMetrics(mBeanServer, "snowflake.ingest.sdk:clientName=SNOWPIPE_REST_*,name=blob.*", "JMX blob: ");

        } catch (Exception e) {
            logger.error("Unable to log JMX metrics", e);
        }
    }

    private static void fetchAndPrintMetrics(MBeanServer mBeanServer, String queryNameString, String logPrefix) throws Exception {
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
