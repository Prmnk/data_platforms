package com.example.fs;

import com.example.fs.model.Agg;
import com.example.fs.model.BucketRow;
import com.example.fs.model.Event;
import com.example.fs.model.UpdateBatch;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Keyed by user. Dedupes events, updates hour/day/month/year buckets for the event's
 * own time, and emits the absolute values of changed buckets, coalesced per user
 * every {@code flushMs} so hot users don't produce one write per event.
 */
public class FeatureAggregator extends KeyedProcessFunction<String, Event, UpdateBatch> {

    private final Buckets buckets;
    private final long flushMs;
    private final long dedupTtlMs;

    private transient MapState<String, Long> seen;
    private transient Map<Buckets.Grain, MapState<String, Agg>> aggs;
    private transient MapState<String, Boolean> dirty;
    private transient ValueState<Long> pendingTimer;
    private transient ValueState<Long> oldestPendingIngest;

    private transient Counter duplicates;
    private transient Counter expired;
    private transient Counter invalid;
    private transient Counter reversalsWithoutOriginal;
    private transient volatile long lastIngestLagMs;

    public FeatureAggregator(Buckets buckets, long flushMs, long dedupTtlMs) {
        this.buckets = buckets;
        this.flushMs = flushMs;
        this.dedupTtlMs = dedupTtlMs;
    }

    @Override
    public void open(Configuration parameters) {
        seen = getRuntimeContext().getMapState(
                withTtl(new MapStateDescriptor<>("seen-event-ids", Types.STRING, Types.LONG), dedupTtlMs));

        aggs = new EnumMap<>(Buckets.Grain.class);
        for (Buckets.Grain g : Buckets.Grain.values()) {
            MapStateDescriptor<String, Agg> d =
                    new MapStateDescriptor<>("agg-" + g.name(), Types.STRING, TypeInformation.of(Agg.class));
            long ttl = g.maxPeriod.toMillis() + g.retention.toMillis();
            aggs.put(g, getRuntimeContext().getMapState(withTtl(d, ttl)));
        }

        dirty = getRuntimeContext().getMapState(new MapStateDescriptor<>("dirty", Types.STRING, Types.BOOLEAN));
        pendingTimer = getRuntimeContext().getState(new ValueStateDescriptor<>("pending-timer", Types.LONG));
        oldestPendingIngest = getRuntimeContext().getState(new ValueStateDescriptor<>("oldest-pending-ingest", Types.LONG));

        duplicates = getRuntimeContext().getMetricGroup().counter("duplicates_dropped");
        expired = getRuntimeContext().getMetricGroup().counter("expired_bucket_skips");
        invalid = getRuntimeContext().getMetricGroup().counter("invalid_events");
        reversalsWithoutOriginal = getRuntimeContext().getMetricGroup().counter("reversals_without_original_time");
        // processing time minus Kafka timestamp of the latest event: how far behind the stream the job is
        getRuntimeContext().getMetricGroup().gauge("ingest_lag_ms", (Gauge<Long>) () -> lastIngestLagMs);
    }

    private static <K, V> MapStateDescriptor<K, V> withTtl(MapStateDescriptor<K, V> d, long ttlMs) {
        d.enableTimeToLive(StateTtlConfig.newBuilder(Time.milliseconds(ttlMs))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
        return d;
    }

    @Override
    public void processElement(Event e, Context ctx, Collector<UpdateBatch> out) throws Exception {
        long cntDelta;
        long amtDelta;
        switch (e.op == null ? "create" : e.op) {
            case "create":
                cntDelta = 1;
                amtDelta = e.amount;
                break;
            case "refund":   // money back, the purchase still happened
                cntDelta = 0;
                amtDelta = -e.amount;
                break;
            case "cancel":   // the order no longer counts
                cntDelta = -1;
                amtDelta = -e.amount;
                break;
            default:
                invalid.inc();
                return;
        }

        if (seen.contains(e.eventId)) {
            duplicates.inc();
            return;
        }
        long now = ctx.timerService().currentProcessingTime();
        seen.put(e.eventId, now);
        lastIngestLagMs = Math.max(0, now - e.ingestTimeMs);
        Long oldest = oldestPendingIngest.value();
        if (oldest == null || e.ingestTimeMs < oldest) {
            oldestPendingIngest.update(e.ingestTimeMs);
        }

        // A reversal belongs to the purchase it reverses, so "orders today" means orders placed
        // today net of their later cancels. Without the original time, fall back to its own time.
        long bucketTimeMs = e.eventTimeMs;
        if (cntDelta != 1) {
            if (e.originalEventTimeMs > 0) {
                bucketTimeMs = e.originalEventTimeMs;
            } else {
                reversalsWithoutOriginal.inc();
            }
        }

        boolean isTxn = Event.TXN.equals(e.kind);
        for (Buckets.Grain g : Buckets.Grain.values()) {
            Buckets.Key k = buckets.keyFor(g, bucketTimeMs);
            if (k.expiresAtMs <= now) {
                expired.inc();
                continue;
            }
            MapState<String, Agg> state = aggs.get(g);
            Agg a = state.get(k.label);
            if (a == null) {
                a = new Agg();
                a.startMs = k.startMs;
                a.expiresAtMs = k.expiresAtMs;
            }
            if (isTxn) {
                a.txnCnt += cntDelta;
                a.txnAmt += amtDelta;
            } else {
                a.ordCnt += cntDelta;
                a.ordAmt += amtDelta;
            }
            a.version = Math.max(a.version + 1, now * 1000);
            a.updatedAtMs = now;
            state.put(k.label, a);
            dirty.put(g.name() + "|" + k.label, Boolean.TRUE);
        }

        if (flushMs <= 0) {
            flush(ctx.getCurrentKey(), now, out);
        } else if (pendingTimer.value() == null) {
            long at = now + flushMs;
            ctx.timerService().registerProcessingTimeTimer(at);
            pendingTimer.update(at);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<UpdateBatch> out) throws Exception {
        pendingTimer.clear();
        flush(ctx.getCurrentKey(), timestamp, out);
    }

    private void flush(String userId, long now, Collector<UpdateBatch> out) throws Exception {
        List<BucketRow> rows = new ArrayList<>();
        for (String dk : dirty.keys()) {
            int sep = dk.indexOf('|');
            Buckets.Grain g = Buckets.Grain.valueOf(dk.substring(0, sep));
            String label = dk.substring(sep + 1);
            Agg a = aggs.get(g).get(label);
            if (a != null && a.expiresAtMs > now) {
                rows.add(BucketRow.of(g.name(), label, a));
            }
        }
        dirty.clear();
        Long oldest = oldestPendingIngest.value();
        oldestPendingIngest.clear();
        if (!rows.isEmpty()) {
            out.collect(new UpdateBatch(userId, rows, oldest == null ? 0 : oldest));
        }
    }
}
