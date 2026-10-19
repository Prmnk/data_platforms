package com.example.fs.model;

/** A transaction or order event. Amounts are in minor units (paise / cents). */
public class Event {
    public static final String TXN = "txn";
    public static final String ORDER = "order";

    public String eventId;
    public String userId;
    /** {@link #TXN} or {@link #ORDER}; derived from the source topic. */
    public String kind;
    /** txn: create | refund. order: create | cancel. */
    public String op;
    public long amount;
    public long eventTimeMs;
    /** For refund / cancel: time of the purchase being reversed (0 if not sent). */
    public long originalEventTimeMs;
    /** Kafka record timestamp (producer time), used for pipeline latency metrics. */
    public long ingestTimeMs;

    public Event() {}
}
