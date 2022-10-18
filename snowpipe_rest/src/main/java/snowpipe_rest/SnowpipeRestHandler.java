package snowpipe_rest;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyRequestEvent;
import com.amazonaws.services.lambda.runtime.events.APIGatewayProxyResponseEvent;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueRequest;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;

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
import java.util.Properties;
import java.util.HashMap;
import java.util.UUID;

public class SnowpipeRestHandler implements RequestHandler<APIGatewayProxyRequestEvent, APIGatewayProxyResponseEvent> {

  private ObjectMapper objectMapper = new ObjectMapper();
  private SnowflakeStreamingIngestClient snowpipe_client;
  private Map<String,SnowflakeStreamingIngestChannel> snowpipe_channels = new HashMap<String,SnowflakeStreamingIngestChannel>();
  private Map<String,Integer> insert_count = new HashMap<String,Integer>();
  private String suffix = UUID.randomUUID().toString();

  public SnowpipeRestHandler() {
    // get secret name from environment variable

    // get secret from Secrets Manager
    //  - url: Snowflake account URL
    //  - user: Snowflake user
    //  - private_key: Snowflake private key
    //  - role: Snowflake role to use

    // Get Secret from Secrets Manager. Secret name stored in environment variable.
    String region = System.getenv("AWS_REGION");
    String secret_name = System.getenv("SNOWFLAKE_SECRET");
    SecretsManagerClient secretsClient = SecretsManagerClient.builder()
      .region(Region.of(region))
      .credentialsProvider(DefaultCredentialsProvider.create())
      .build();
      
    String secret;
    try {
      GetSecretValueRequest secret_req = GetSecretValueRequest.builder().secretId(secret_name).build();
      GetSecretValueResponse secret_resp = secretsClient.getSecretValue(secret_req);
      secret = secret_resp.secretString();
    }
    catch (Exception e) {
      // Handle errors from Secrets Manager
      throw new RuntimeException(e);
    }

    // Parse Secret as JSON
    java.util.Properties props = new Properties();
    try {
      JsonNode credsJson = objectMapper.readTree(secret);
      Map<String,String> credsMap = objectMapper.convertValue(credsJson, new TypeReference<Map<String,String>>() {});
      props.putAll(credsMap);
    }
    catch (JsonProcessingException je) {
      // Handle errors from parsing JSON
      throw new RuntimeException("Unable to parse secret string as JSON.");
    }

    // Connect to Snowflake with credentials from Secret.
    try{
      // Make Snowflake Streaming Ingest Client
      this.snowpipe_client = SnowflakeStreamingIngestClientFactory.builder("SNOWPIPE_REST_" + this.suffix).setProperties(props).build();
    }
    catch (Exception e) {
      // Handle Exception for Snowpipe Streaming objects
      throw new RuntimeException(e);
    }
  }

  // Gets or creates and stores Snowflake Streaming Ingest Channel for the table
  private SnowflakeStreamingIngestChannel getIngestChannel(String database, String schema, String table) {
    if (null == database)
      throw new RuntimeException("Must specify database");
    if (null == schema)
      throw new RuntimeException("Must specify schema");
    if (null == table)
      throw new RuntimeException("Must specify table");
    String key = String.format("%s.%s.%s", database, schema, table);
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
    }
    catch (Exception e) {
      // Handle Exception for Snowpipe Streaming objects
      throw new RuntimeException(e);
    }
  }

  // Path should end with {database}/{schema}/{table}
  private String[] pathToTable(String path) {
    String[] tableName = {null, null, null};
    String[] pieces = path.split("/");
    if (3 > pieces.length) 
      throw new RuntimeException(String.format("Path (%s) [%s] is invalid (%d fields)", path, pieces.toString(), pieces.length));
    tableName[0] = pieces[pieces.length - 3];
    tableName[1] = pieces[pieces.length - 2];
    tableName[2] = pieces[pieces.length - 1];

    return tableName;
  }

  // Lambda handler
  @Override
  public APIGatewayProxyResponseEvent handleRequest(APIGatewayProxyRequestEvent event, Context context) {
    // Process body
    try {
      System.out.println(objectMapper.writeValueAsString(event));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
    String body = event.getBody();
    System.out.println(String.format("BODY: %s", body));
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
      throw new RuntimeException("Unable to parse body as list of JSON strings.");
    }

    // Get channel based on path variables
    String[] tableName = pathToTable(event.getPath());
    SnowflakeStreamingIngestChannel channel = getIngestChannel(tableName[0], tableName[1], tableName[2]);
    String key = String.format("%s.%s.%s", tableName[0], tableName[1], tableName[2]);
    int insert_count = this.insert_count.get(key);
    insert_count++;

    // Issue the insert
    String new_token = String.valueOf(insert_count);
    InsertValidationResponse resp = channel.insertRows(rows, new_token);
    this.insert_count.put(key, insert_count);

    int maxRetries = 20;
    int retryCount = 0;
    do {
      String offsetTokenFromSnowflake = channel.getLatestCommittedOffsetToken();
      if (offsetTokenFromSnowflake != null
          && offsetTokenFromSnowflake.equals(new_token)) {
        System.out.println("SUCCESSFULLY inserted");
        break;
      }
      else {
        try {
          Thread.sleep(1000);
        }
        catch (Exception e) {
          e.printStackTrace();
        }
      }
      retryCount++;
    } while (retryCount < maxRetries);

    // Create response
    APIGatewayProxyResponseEvent response = new APIGatewayProxyResponseEvent();
    response.setIsBase64Encoded(false);
    response.setStatusCode(200);
    HashMap<String, String> headers = new HashMap<String, String>();
    headers.put("Content-Type", "text/html");
    response.setHeaders(headers);

    try {
      StringBuffer resp_body = new StringBuffer("{\n");
      resp_body.append(String.format("  \"inserts_attempted\": %d,\n  \"inserts_succeeded\": %d,\n  \"insert_errors\": %d,\n", 
        rows.size(), rows.size() - resp.getErrorRowCount(), resp.getErrorRowCount()));
      resp_body.append("  \"error_rows\":\n    [");
      String delim = " ";
      for (InsertValidationResponse.InsertError insertError : resp.getInsertErrors()) {
        int idx = (int)insertError.getRowIndex();
        resp_body.append(String.format("\n    %s {\"row_index\": %d, \"input\": \"%s\", \"error\": \"%s\"}", 
          delim, idx, objectMapper.writeValueAsString(rowStrings.get(idx)), insertError.getMessage()));
        delim = ",";
      }
      resp_body.append("\n    ]");
      resp_body.append("\n}");
      response.setBody(resp_body.toString());
      return response;
    }
    catch (JsonProcessingException je) {
      throw new RuntimeException(je);
    }
  }
}