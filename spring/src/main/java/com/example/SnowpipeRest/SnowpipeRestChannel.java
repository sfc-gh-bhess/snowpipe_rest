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
    private Map<String,List<Map<String,Object>>> buffer = null;
    private CompletableFuture<Void> purger = null;
    private int purge_rate;
    private boolean disable_buffering;
    private boolean dosynchronized;
    private OpenChannelRequest openChannelRequest;
    private SnowflakeStreamingIngestClient client;
    private ObjectMapper objectMapper = new ObjectMapper();

    public SnowpipeRestChannel(String key, SnowflakeStreamingIngestClient snowpipe_client, 
                                OpenChannelRequest request1, int purge_rate, boolean disable_buffering) {
        this(key, snowpipe_client, request1, purge_rate, disable_buffering, false);
    }

    public SnowpipeRestChannel(String key, SnowflakeStreamingIngestClient snowpipe_client, 
                                OpenChannelRequest request1, int purge_rate, boolean disable_buffering, boolean dosynchronized) {
        this.key = key;
        this.client = snowpipe_client;
        this.openChannelRequest = request1;
        this.purge_rate = purge_rate;
        this.disable_buffering = disable_buffering;
        this.dosynchronized = dosynchronized;
        init();
    }

    private void init() {
        this.snowpipe_channel = this.client.openChannel(openChannelRequest);
        if (this.buffer == null) {
            this.buffer = new ConcurrentHashMap<String,List<Map<String,Object>>>();
            this.insert_count = 0;
        }
        if (this.purger != null)
            this.purger.cancel(true);
        this.purger = CompletableFuture.runAsync(() -> purger());
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
            logger.info(String.format("<%s> XXXX: %s", this.key, e.getMessage()));
        }
        int ttoken = last_token;
        List<String> pkeys = this.buffer.keySet().stream().filter(k -> Integer.parseInt(k) <= ttoken).toList();
        if (pkeys.size() > 0) {
            logger.info(String.format("<%s> Purging from %s: %s", key, key, pkeys));
            for (String k : pkeys) {
                this.buffer.remove(k);
            }
        }
    }

    private synchronized SnowpipeRestChannel makeChannelValid() {
        logger.info(String.format("<%s> Making channel valid: %s", key, key));
        if (this.snowpipe_channel.isValid())
            return this;
        init();
        replayBuffer();
        return this;
    }

    public SnowpipeInsertResponse insertBatches(List<List<Map<String,Object>>> batches, List<List<Object>> batchStrings) {
        if (this.dosynchronized)
            return insertBatches_syncrhonized(batches, batchStrings);
        return doInsertBatches(batches, batchStrings);
    }

    public synchronized SnowpipeInsertResponse insertBatches_syncrhonized(List<List<Map<String,Object>>> batches, List<List<Object>> batchStrings) {
        return doInsertBatches(batches, batchStrings);
    }

    public SnowpipeInsertResponse doInsertBatches(List<List<Map<String,Object>>> batches, List<List<Object>> batchStrings) {
        SnowpipeInsertResponse sp_resp = new SnowpipeInsertResponse(0, 0, 0);
        logger.info(String.format("<%s> Inserting %d batches.", this.key, batches.size()));
        for (int i = 0; i < batches.size(); i++) {
            InsertValidationResponse resp = this.insertBatch(batches.get(i), this.disable_buffering);

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

    public InsertValidationResponse insertBatch(List<Map<String,Object>> batch, boolean dont_buffer) {
        this.insert_count++;
        String new_token = String.valueOf(insert_count);
        return this.insertBatch(batch, new_token, dont_buffer);
    }

    private InsertValidationResponse insertBatch(List<Map<String,Object>> batch, String new_token, boolean dont_buffer) {
        InsertValidationResponse resp;
        try {
            resp = this.snowpipe_channel.insertRows(batch, new_token);
            if (!dont_buffer) {
                this.buffer.put(new_token, batch);
            }
        }
        catch (SFException ex) {
            makeChannelValid();
            resp = insertBatch(batch, new_token, dont_buffer);
        }
        return resp;
    }

    private void replayBuffer() {
        logger.info(String.format("<%s> Replaying buffer: %s", key, key));
        freePlayed();
        List<Integer> tokens = this.buffer.keySet().stream().map(e -> Integer.parseInt(e)).sorted().toList();
        for (Integer t : tokens) {
            String token = String.valueOf(t);
            try {
                insertBatch(this.buffer.get(token), token, true); // don't need to buffer since it's replaying the buffer
            }
            catch (SFException ex) {
                makeChannelValid();
            }
        }
    }

}
