package com.example.fs.io;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Statement;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.runtime.metrics.DescriptiveStatisticsHistogram;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Async, bounded-concurrency Scylla sink with at-least-once semantics:
 * every checkpoint waits for all in-flight writes, and any failed write fails the
 * task so Flink replays from the last checkpoint. Replays are harmless because
 * writes carry absolute values with USING TIMESTAMP (idempotent, last-version-wins).
 */
public abstract class AsyncCqlSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {

    protected final CqlConfig cql;

    private transient CqlSession session;
    private transient Semaphore inFlight;
    private transient AtomicReference<Throwable> failure;

    private transient Counter writesOk;
    private transient Counter writesFailed;
    private transient Histogram writeLatencyMs;
    private transient Histogram e2eLatencyMs;

    protected AsyncCqlSink(CqlConfig cql) {
        this.cql = cql;
    }

    protected abstract void prepare(CqlSession session);

    protected abstract List<Statement<?>> toStatements(T value, long nowMs);

    /** Kafka timestamp of the oldest source event behind this value, or 0 if unknown. */
    protected long ingestTimeMs(T value) {
        return 0;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        failure = new AtomicReference<>();
        inFlight = new Semaphore(cql.maxInFlight);
        session = cql.connect(getClass().getClassLoader());
        prepare(session);

        writesOk = getRuntimeContext().getMetricGroup().counter("cql_writes_ok");
        writesFailed = getRuntimeContext().getMetricGroup().counter("cql_writes_failed");
        writeLatencyMs = getRuntimeContext().getMetricGroup().histogram("cql_write_latency_ms", new DescriptiveStatisticsHistogram(1000));
        // produced to Kafka -> acknowledged by Scylla: the feature freshness the API actually sees
        e2eLatencyMs = getRuntimeContext().getMetricGroup().histogram("e2e_latency_ms", new DescriptiveStatisticsHistogram(1000));
        getRuntimeContext().getMetricGroup().gauge("cql_in_flight", (Gauge<Integer>) () -> cql.maxInFlight - inFlight.availablePermits());
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        rethrowIfFailed();
        long ingest = ingestTimeMs(value);
        for (Statement<?> st : toStatements(value, System.currentTimeMillis())) {
            inFlight.acquire();
            long started = System.nanoTime();
            session.executeAsync(st).whenComplete((rs, err) -> {
                if (err != null) {
                    writesFailed.inc();
                    failure.compareAndSet(null, err);
                } else {
                    writesOk.inc();
                    record(writeLatencyMs, (System.nanoTime() - started) / 1_000_000);
                    if (ingest > 0) {
                        record(e2eLatencyMs, System.currentTimeMillis() - ingest);
                    }
                }
                inFlight.release();
            });
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        drain();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) {
        // stateless: durability comes from source offsets + idempotent writes
    }

    @Override
    public void close() throws Exception {
        try {
            if (session != null) {
                drain();
            }
        } finally {
            if (session != null) {
                session.close();
            }
        }
    }

    private static void record(Histogram h, long value) {
        synchronized (h) { // completions arrive on driver I/O threads
            h.update(value);
        }
    }

    private void drain() throws Exception {
        inFlight.acquire(cql.maxInFlight);
        inFlight.release(cql.maxInFlight);
        rethrowIfFailed();
    }

    private void rethrowIfFailed() throws IOException {
        Throwable t = failure.get();
        if (t != null) {
            throw new IOException("Scylla write failed", t);
        }
    }
}
