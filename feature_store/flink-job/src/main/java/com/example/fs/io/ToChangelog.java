package com.example.fs.io;

import com.example.fs.model.BucketRow;
import com.example.fs.model.ChangelogRecord;
import com.example.fs.model.UpdateBatch;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/** One changelog record per bucket, keyed user|grain|bucket so compaction keeps the latest value. */
public class ToChangelog extends RichFlatMapFunction<UpdateBatch, ChangelogRecord> {

    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        mapper = new ObjectMapper();
    }

    @Override
    public void flatMap(UpdateBatch b, Collector<ChangelogRecord> out) throws Exception {
        for (BucketRow r : b.rows) {
            ObjectNode n = mapper.createObjectNode()
                    .put("user_id", b.userId)
                    .put("grain", r.grain)
                    .put("bucket", r.bucket)
                    .put("bucket_start_ms", r.startMs)
                    .put("txn_cnt", r.txnCnt)
                    .put("txn_amt", r.txnAmt)
                    .put("ord_cnt", r.ordCnt)
                    .put("ord_amt", r.ordAmt)
                    .put("version", r.version)
                    .put("updated_at_ms", r.updatedAtMs)
                    .put("expires_at_ms", r.expiresAtMs);
            out.collect(new ChangelogRecord(b.userId + "|" + r.grain + "|" + r.bucket, mapper.writeValueAsString(n)));
        }
    }
}
