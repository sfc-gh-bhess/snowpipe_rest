package com.example.SnowpipeRest;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.snowflake.ingest.streaming.InsertValidationResponse;
import net.snowflake.ingest.streaming.OpenChannelRequest;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestChannel;
import net.snowflake.ingest.streaming.SnowflakeStreamingIngestClient;
import net.snowflake.ingest.utils.SFException;

public class SnowpipeRestChannel {
    static Logger logger = LoggerFactory.getLogger(SnowpipeRestChannel.class);

    private String key;
    private SnowflakeStreamingIngestChannel snowpipe_channel;
    private int insert_count;
    private Map<String,List<Map<String,Object>>> buffer;
    private CompletableFuture<Void> purger;
    private int purge_rate;
    private OpenChannelRequest openChannelRequest;
    private SnowflakeStreamingIngestClient client;
    private ObjectMapper objectMapper = new ObjectMapper();

    public SnowpipeRestChannel(String key, SnowflakeStreamingIngestClient snowpipe_client, OpenChannelRequest request1, int purge_rate) {
        this.key = key;
        this.client = snowpipe_client;
        this.openChannelRequest = request1;
        this.purge_rate = purge_rate;
        init();
    }

    private void init() {
        this.snowpipe_channel = this.client.openChannel(openChannelRequest);
        this.buffer = new ConcurrentHashMap<String,List<Map<String,Object>>>();
        this.purger = CompletableFuture.runAsync(() -> purger());
        this.insert_count = 0;
    }

    public SnowflakeStreamingIngestChannel get_snowpipe_channel() {
        return this.snowpipe_channel;
    }
    public int get_insert_count() {
        return this.insert_count;
    }
    public Map<String,List<Map<String,Object>>> get_buffer() {
        return this.buffer;
    }
    public CompletableFuture<Void> get_purger() {
        return this.purger;
    }

    public int set_insert_count(int i) {
        this.insert_count = i;
        return i;
    }

    private void purger() {
        try {
            while (true) {
                freePlayed();
                Thread.sleep(this.purge_rate);
            }
        }        
        catch (Exception e) {
            return;
        }
    }

    public void freePlayed() {
        String last_token_str = this.snowpipe_channel.getLatestCommittedOffsetToken();
        int last_token = -1;
        if (null == last_token_str)
            return;
        try {
            last_token = Integer.parseInt(last_token_str);
        }
        catch (Exception e) {
            logger.info(String.format("XXXX: %s", e.getMessage()));
        }
        int ttoken = last_token;
        List<String> pkeys = this.buffer.keySet().stream().filter(k -> Integer.parseInt(k) <= ttoken).toList();
        if (pkeys.size() > 0) {
            logger.info(String.format("Purging from %s: %s", key, pkeys));
            for (String k : pkeys) {
                this.buffer.remove(k);
            }
        }
    }

    private SnowpipeRestChannel makeChannelValid() {
        logger.info(String.format("Making channel valid: %s", key));
        if (this.snowpipe_channel.isValid())
            return this;
        init();
        replayBuffer();
        return this;
    }

    /*
     * TODO: Should this be synchronized or not?
     */
    public synchronized SnowpipeInsertResponse insertBatches(List<List<Map<String,Object>>> batches, List<List<Object>> batchStrings) {
        SnowpipeInsertResponse sp_resp = new SnowpipeInsertResponse(0, 0, 0);
        logger.info(String.format("Inserting %d batches.", batches.size()));
        for (int i = 0; i < batches.size(); i++) {
            InsertValidationResponse resp = this.insertBatch(batches.get(i));

            // Make response
            sp_resp.add_metrics(batches.get(i).size(), batches.get(i).size() - resp.getErrorRowCount(), resp.getErrorRowCount());
            for (InsertValidationResponse.InsertError insertError : resp.getInsertErrors()) {
                int idx = (int)insertError.getRowIndex();
                try {
                    sp_resp.addError(idx, this.objectMapper.writeValueAsString(batchStrings.get(i).get(idx)), insertError.getMessage());
                }
                catch (JsonProcessingException je) {
                    throw new RuntimeException(je);
                }    
            }
        }
        return sp_resp;
    }

    public InsertValidationResponse insertBatch(List<Map<String,Object>> batch) {
        this.insert_count++;
        String new_token = String.valueOf(insert_count);
        return this.insertBatch(batch, new_token);
    }

    private InsertValidationResponse insertBatch(List<Map<String,Object>> batch, String new_token) {
        InsertValidationResponse resp;
        try {
            resp = this.snowpipe_channel.insertRows(batch, new_token);
            this.buffer.put(new_token, batch);
        }
        catch (SFException ex) {
            makeChannelValid();
            resp = insertBatch(batch, new_token);
        }
        return resp;
    }

    private void replayBuffer() {
        logger.info(String.format("Replaying buffer: %s", key));
        freePlayed();
        List<Integer> tokens = this.buffer.keySet().stream().map(e -> Integer.parseInt(e)).sorted().toList();
        for (Integer t : tokens) {
            String token = String.valueOf(t);
            try {
                insertBatch(this.buffer.get(token), token);
            }
            catch (SFException ex) {
                makeChannelValid();
            }
        }
    }

}
