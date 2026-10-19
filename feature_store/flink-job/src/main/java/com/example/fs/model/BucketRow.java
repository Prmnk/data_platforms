package com.example.fs.model;

/** Absolute (not incremental) value of one bucket, as written to the online store and changelog. */
public class BucketRow {
    public String grain;
    public String bucket;
    public long startMs;
    public long txnCnt;
    public long txnAmt;
    public long ordCnt;
    public long ordAmt;
    public long version;
    public long updatedAtMs;
    public long expiresAtMs;

    public BucketRow() {}

    public static BucketRow of(String grain, String bucket, Agg a) {
        BucketRow r = new BucketRow();
        r.grain = grain;
        r.bucket = bucket;
        r.startMs = a.startMs;
        r.txnCnt = a.txnCnt;
        r.txnAmt = a.txnAmt;
        r.ordCnt = a.ordCnt;
        r.ordAmt = a.ordAmt;
        r.version = a.version;
        r.updatedAtMs = a.updatedAtMs;
        r.expiresAtMs = a.expiresAtMs;
        return r;
    }

    /** Remaining TTL so a row expires at the same moment whether written live or reloaded. */
    public int ttlSeconds(long nowMs) {
        return (int) Math.max(0, (expiresAtMs - nowMs) / 1000);
    }
}
