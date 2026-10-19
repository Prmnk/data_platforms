package com.example.fs.io;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchStatementBuilder;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.example.fs.model.BucketRow;
import com.example.fs.model.UpdateBatch;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * Writes D/M/Y buckets of a user as one single-partition UNLOGGED batch (atomic and
 * isolated, so today/MTD/YTD are never seen out of sync), and hourly buckets separately.
 */
public class AggSink extends AsyncCqlSink<UpdateBatch> {

    private transient List<PreparedStatement> insertAgg;
    private transient List<PreparedStatement> insertHourly;

    public AggSink(CqlConfig cql) {
        super(cql);
    }

    @Override
    protected void prepare(CqlSession session) {
        insertAgg = new ArrayList<>();
        insertHourly = new ArrayList<>();
        for (String ks : cql.keyspaces) {
            insertAgg.add(session.prepare("INSERT INTO " + ks + ".user_agg "
                    + "(user_id, grain, bucket, txn_cnt, txn_amt, ord_cnt, ord_amt, updated_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?"));
            insertHourly.add(session.prepare("INSERT INTO " + ks + ".user_agg_hourly "
                    + "(user_id, hour, txn_cnt, txn_amt, ord_cnt, ord_amt, updated_at) "
                    + "VALUES (?, ?, ?, ?, ?, ?, ?) USING TTL ? AND TIMESTAMP ?"));
        }
    }

    @Override
    protected long ingestTimeMs(UpdateBatch b) {
        return b.oldestIngestMs;
    }

    @Override
    protected List<Statement<?>> toStatements(UpdateBatch b, long nowMs) {
        List<Statement<?>> out = new ArrayList<>();
        for (int i = 0; i < cql.keyspaces.size(); i++) {
            BatchStatementBuilder batch = BatchStatement.builder(DefaultBatchType.UNLOGGED);
            int batched = 0;
            for (BucketRow r : b.rows) {
                int ttl = r.ttlSeconds(nowMs);
                if (ttl <= 0) {
                    continue;
                }
                Instant updatedAt = Instant.ofEpochMilli(r.updatedAtMs);
                if ("H".equals(r.grain)) {
                    out.add(insertHourly.get(i).bind(b.userId, Instant.ofEpochMilli(r.startMs),
                            r.txnCnt, r.txnAmt, r.ordCnt, r.ordAmt, updatedAt, ttl, r.version));
                } else {
                    batch.addStatement(insertAgg.get(i).bind(b.userId, r.grain, r.bucket,
                            r.txnCnt, r.txnAmt, r.ordCnt, r.ordAmt, updatedAt, ttl, r.version));
                    batched++;
                }
            }
            if (batched > 0) {
                out.add(batch.setIdempotence(true).build());
            }
        }
        return out;
    }
}
