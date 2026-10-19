package com.example.fs.model;

/** Running totals for one bucket of one user, held in Flink keyed state. */
public class Agg {
    public long txnCnt;
    public long txnAmt;
    public long ordCnt;
    public long ordAmt;
    /** Write timestamp (micros) used as Scylla USING TIMESTAMP; strictly increasing per bucket. */
    public long version;
    public long updatedAtMs;
    public long startMs;
    public long expiresAtMs;

    public Agg() {}
}
