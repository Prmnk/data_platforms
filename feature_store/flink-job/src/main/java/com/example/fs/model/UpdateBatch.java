package com.example.fs.model;

import java.util.ArrayList;
import java.util.List;

/** All buckets of one user that changed since the last flush. */
public class UpdateBatch {
    public String userId;
    public List<BucketRow> rows = new ArrayList<>();
    /** Kafka timestamp of the oldest event folded into this batch (for end-to-end latency). */
    public long oldestIngestMs;

    public UpdateBatch() {}

    public UpdateBatch(String userId, List<BucketRow> rows, long oldestIngestMs) {
        this.userId = userId;
        this.rows = rows;
        this.oldestIngestMs = oldestIngestMs;
    }
}
