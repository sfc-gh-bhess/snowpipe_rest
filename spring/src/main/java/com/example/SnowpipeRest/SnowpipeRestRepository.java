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

import java.util.Map;
import java.util.List;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;
import java.util.HashMap;
import java.util.UUID;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class SnowpipeRestRepository {
    private ObjectMapper objectMapper = new ObjectMapper();
    private SnowflakeStreamingIngestClient snowpipe_client;
    private Map<String, SnowflakeStreamingIngestChannel> snowpipe_channels = new HashMap<String, SnowflakeStreamingIngestChannel>();
    private Map<String, Integer> insert_count = new HashMap<String, Integer>();
    private String suffix = UUID.randomUUID().toString();

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
            return channel;
        } catch (Exception e) {
            // Handle Exception for Snowpipe Streaming objects
            // throw new RuntimeException(e);
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
        SnowflakeStreamingIngestChannel channel = this.getIngestChannel(database, schema, table);
        String insert_count_key = makeKey(database, schema, table);
        int insert_count = this.insert_count.get(insert_count_key);
        insert_count++;

        // Issue the insert
        String new_token = String.valueOf(insert_count);
        InsertValidationResponse resp = channel.insertRows(rows, new_token);
        this.insert_count.put(insert_count_key, insert_count);

        int maxRetries = 20;
        int retryCount = 0;
        do {
            String offsetTokenFromSnowflake = channel.getLatestCommittedOffsetToken();
            if (offsetTokenFromSnowflake != null
                    && offsetTokenFromSnowflake.equals(new_token)) {
                System.out.println("SUCCESSFULLY inserted");
                break;
            } else {
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            retryCount++;
        } while (retryCount < maxRetries);

        // Make response
        try {
            SnowpipeInsertResponse sp_resp = new SnowpipeInsertResponse(rows.size(), rows.size() - resp.getErrorRowCount(), resp.getErrorRowCount());
            for (InsertValidationResponse.InsertError insertError : resp.getInsertErrors()) {
                int idx = (int)insertError.getRowIndex();
                sp_resp.addError(idx, objectMapper.writeValueAsString(rowStrings.get(idx)), insertError.getMessage());
            }

            this.insert_count.put(insert_count_key, insert_count);
            return sp_resp;
        }
        catch (JsonProcessingException je) {
            throw new RuntimeException(je);
        }
    }

    public class SnowpipeInsertError {
        public int row_index;
        public String input;
        public String error;

        public SnowpipeInsertError(int row_index, String input, String error) {
            this.row_index = row_index;
            this.input = input;
            this.error = error;
        }

        public int getRow_index() {
            return this.row_index;
        }

        public String getInput() {
            return this.input;
        }

        public String getError() {
            return this.error;
        }

        public String toString() {
            return String.format("{\"row_index\": \"%s\", \"input\": \"%s\", \"error\": \"%s\"}", this.row_index, this.input, this.error);
        }
    }

    public class SnowpipeInsertResponse {
        int num_attempted;
        int num_succeeded;
        int num_errors;
        List<SnowpipeInsertError> errors;

        public SnowpipeInsertResponse(int num_attempted, int num_succeeded, int num_errors) {
            this(num_attempted, num_succeeded, num_errors, new ArrayList<SnowpipeInsertError>());
        }

        public SnowpipeInsertResponse(int num_attempted, int num_succeeded, int num_errors, List<SnowpipeInsertError> errors) {
            this.num_attempted = num_attempted;
            this.num_succeeded = num_succeeded;
            this.num_errors = num_errors;
            this.errors = errors;
        }

        public int getNum_attempted() {
            return this.num_attempted;
        }

        public int getNum_succeeded() {
            return this.num_succeeded;
        }

        public int getNum_errors() {
            return this.num_errors;
        }

        public List<SnowpipeInsertError> getErrors() {
            return this.errors;
        }

        public SnowpipeInsertResponse addError(int row_index, String input, String error) {
            return this.addError(new SnowpipeInsertError(row_index, input, error));
        }

        public SnowpipeInsertResponse addError(SnowpipeInsertError e) {
            errors.add(e);
            return this;
        }

        public String toString() {
            StringBuffer resp_body = new StringBuffer("{\n");
            resp_body.append(String.format(
                    "  \"inserts_attempted\": %d,\n  \"inserts_succeeded\": %d,\n  \"insert_errors\": %d,\n",
                    this.num_attempted, this.num_succeeded, this.num_errors));
            resp_body.append("  \"error_rows\":\n    [");
            String delim = " ";
            for (SnowpipeInsertError e: this.errors) {
                resp_body.append(String.format("\n    %s %s", delim, e.toString()));
                delim = ",";
            }
            resp_body.append("\n    ]");
            resp_body.append("\n}");
            return resp_body.toString();
        }
    }

}
