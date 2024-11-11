package com.example.SnowpipeRest;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClientFactory;
import net.snowflake.ingest.utils.SFException;

import java.util.Map;
import java.util.List;
import java.util.Properties;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.google.common.collect.Lists;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SnowpipeRestRepository {
    Logger logger = LoggerFactory.getLogger(SnowpipeRestRepository.class);
    
    private ObjectMapper objectMapper = new ObjectMapper();
    private SnowflakeStreamingIngestClient snowpipe_client;
    private Map<String, SnowflakeStreamingIngestChannel> snowpipe_channels = new HashMap<String, SnowflakeStreamingIngestChannel>();
    private Map<String, Integer> insert_count = new HashMap<String, Integer>();
    private Map<String, ConcurrentHashMap<String,List<Map<String,Object>>>> buffers = new HashMap<String,ConcurrentHashMap<String,List<Map<String,Object>>>>();
    private String suffix = UUID.randomUUID().toString();
    private ExecutorService executorService = Executors.newFixedThreadPool(1);

    @Value("${snowpiperest.batch_size}")
    private int batch_size;

    @Value("${snowflake.url}")
    private String snowflake_url;

    @Value("${snowflake.user}")
    private String snowflake_user;

    @Value("${snowflake.role}")
    private String snowflake_role;

    @Value("${snowflake.private_key}")
    private String snowflake_private_key;

    @PostConstruct
    private void init() {
        // get Snowflake credentials and put them in props
        java.util.Properties props = new Properties();
        props.put("url", snowflake_url);
        props.put("user", snowflake_user);
        props.put("role", snowflake_role);
        props.put("private_key", snowflake_private_key);

        // Connect to Snowflake with credentials.
        try {
            // Make Snowflake Streaming Ingest Client
            this.snowpipe_client = SnowflakeStreamingIngestClientFactory.builder("SNOWPIPE_REST_" + this.suffix)
                    .setProperties(props).build();
        } catch (Exception e) {
            // Handle Exception for Snowpipe Streaming objects
            throw new RuntimeException(e);
        }
    }

    private String makeKey(String database, String schema, String table) {
        return String.format("%s.%s.%s", database.toUpperCase(), schema.toUpperCase(), table.toUpperCase());
    }

    // Gets or creates and stores Snowflake Streaming Ingest Channel for the table
    private SnowflakeStreamingIngestChannel getIngestChannel(String database, String schema, String table) {
        if (null == database)
            throw new RuntimeException("Must specify database");
        if (null == schema)
            throw new RuntimeException("Must specify schema");
        if (null == table)
            throw new RuntimeException("Must specify table");
        String key = makeKey(database, schema, table);
        if (this.snowpipe_channels.containsKey(key))
            return this.snowpipe_channels.get(key);

        try {
            OpenChannelRequest request1 = OpenChannelRequest.builder("SNOWPIPE_REST_CHANNEL_" + this.suffix)
                    .setDBName(database)
                    .setSchemaName(schema)
                    .setTableName(table)
                    .setOnErrorOption(OpenChannelRequest.OnErrorOption.CONTINUE)
                    .build();
            SnowflakeStreamingIngestChannel channel = this.snowpipe_client.openChannel(request1);
            this.snowpipe_channels.put(key, channel);
            this.insert_count.put(key, 0);
            if (!this.buffers.containsKey(key))
                this.buffers.put(key, new ConcurrentHashMap<String,List<Map<String,Object>>>());
            return channel;
        } catch (Exception e) {
            // Handle Exception for Snowpipe Streaming objects
            // throw new RuntimeException(e);
            e.printStackTrace();
            throw new SnowpipeRestTableNotFoundException(String.format("Table not found (or no permissions): %s.%s.%s", database.toUpperCase(), schema.toUpperCase(), table.toUpperCase()));
        }
    }

    private SnowflakeStreamingIngestChannel makeChannelValid(String database, String schema, String table) {
        String key = makeKey(database, schema, table);
        if (this.snowpipe_channels.containsKey(key)) {
            SnowflakeStreamingIngestChannel channel = this.snowpipe_channels.get(key);
            if (channel.isValid())
                return channel;
            else
                this.snowpipe_channels.remove(key);
        }
        SnowflakeStreamingIngestChannel channel = getIngestChannel(database, schema, table);
        replayBuffer(database, schema, table);
        return channel;
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
        // SnowflakeStreamingIngestChannel channel = this.getIngestChannel(database, schema, table);
        ConcurrentHashMap<String,List<Map<String,Object>>> buff = this.buffers.get(makeKey(database, schema, table));
        String insert_count_key = makeKey(database, schema, table);
        int insert_count = this.insert_count.get(insert_count_key);

        // Issue the insert
        List<List<Map<String,Object>>> batches = Lists.partition(rows, batch_size);
        List<List<Object>> batchStrings = Lists.partition(rowStrings, batch_size);
        SnowpipeInsertResponse sp_resp = new SnowpipeInsertResponse(0, 0, 0);
        for (int i = 0; i < batches.size(); i++) {
            insert_count++;
            String new_token = String.valueOf(insert_count);
            // InsertValidationResponse resp = channel.insertRows(batches.get(i), new_token);
            InsertValidationResponse resp = insertRows(batches.get(i), new_token, database, schema, table);
            buff.put(new_token, batches.get(i));
            this.insert_count.put(insert_count_key, insert_count);

            // Make response
            sp_resp.add_metrics(rows.size(), rows.size() - resp.getErrorRowCount(), resp.getErrorRowCount());
            for (InsertValidationResponse.InsertError insertError : resp.getInsertErrors()) {
                int idx = (int)insertError.getRowIndex();
                try {
                    sp_resp.addError(idx, objectMapper.writeValueAsString(batchStrings.get(idx)), insertError.getMessage());
                }
                catch (JsonProcessingException je) {
                    throw new RuntimeException(je);
                }    
            }
        }
        if (((ThreadPoolExecutor)(this.executorService)).getQueue().size() == 0)
            this.executorService.submit(() -> {
                freePlayed(insert_count_key);
            });
        return sp_resp;
    }

    private InsertValidationResponse insertRows(List<Map<String,Object>> batch, String new_token, 
                                                String database, String schema, String table) {
        SnowflakeStreamingIngestChannel channel = this.getIngestChannel(database, schema, table);
        // ConcurrentHashMap<String,List<Map<String,Object>>> buff = this.buffers.get(makeKey(database, schema, table));
        InsertValidationResponse resp;
        try {
            resp = channel.insertRows(batch, new_token);
        }
        catch (SFException ex) {
            makeChannelValid(database, schema, table);
            resp = insertRows(batch, new_token, database, schema, table);
        }
        // buff.put(new_token, batch);
        return resp;
    }

    private void freePlayed(String key) {
        SnowflakeStreamingIngestChannel channel = this.snowpipe_channels.get(key);
        int last_token = Integer.parseInt(channel.getLatestCommittedOffsetToken());
        ConcurrentHashMap<String,List<Map<String,Object>>> buff = this.buffers.get(key);
        for (String k : buff.keySet()) {
            if (Integer.parseInt(k) <= last_token)
                buff.remove(k);
        }
    }

    private void replayBuffer(String database, String schema, String table) {
        String key = makeKey(database, schema, table);
        freePlayed(key);
        // SnowflakeStreamingIngestChannel channel = this.snowpipe_channels.get(key);
        ConcurrentHashMap<String,List<Map<String,Object>>> buff = this.buffers.get(key);
        List<Integer> tokens = Collections.list(buff.keys()).stream().map(e -> Integer.parseInt(e)).toList();
        Collections.sort(tokens);
        for (Integer t : tokens) {
            String token = String.valueOf(t);
            try {
                insertRows(buff.get(token), token, database, schema, table);
            }
            catch (SFException ex) {
                makeChannelValid(database, schema, table);
            }
        }
    }
}
