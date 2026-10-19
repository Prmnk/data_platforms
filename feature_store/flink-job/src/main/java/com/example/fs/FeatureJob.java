package com.example.fs;

import com.example.fs.io.AggSink;
import com.example.fs.io.ChangelogSerializer;
import com.example.fs.io.EventDeserializer;
import com.example.fs.io.ProfileDeserializer;
import com.example.fs.io.ProfileSink;
import com.example.fs.io.ToChangelog;
import com.example.fs.model.ChangelogRecord;
import com.example.fs.model.Event;
import com.example.fs.model.Profile;
import com.example.fs.model.UpdateBatch;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

/**
 * transactions + orders  -> dedupe -> hour/day/month/year buckets -> Scylla (user_agg, user_agg_hourly)
 *                                                                 -> features.changelog (compacted)
 * users                  -> Scylla static profile columns
 *
 * Checkpointing, state backend and restart strategy come from Flink config (docker-compose).
 * No event-time windows are used, so no watermarks are needed: each event updates the
 * buckets of its own timestamp, which also handles late events.
 */
public final class FeatureJob {

    public static void main(String[] args) throws Exception {
        JobConfig cfg = new JobConfig();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<Event> eventSource = KafkaSource.<Event>builder()
                .setBootstrapServers(cfg.brokers)
                .setTopics(cfg.txnTopic, cfg.ordersTopic)
                .setGroupId(cfg.groupId)
                // Restored jobs use checkpointed offsets. A job with no state must replay from the
                // beginning: resuming committed offsets with empty bucket state would overwrite
                // correct totals in Scylla with partial ones.
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setDeserializer(new EventDeserializer(cfg.ordersTopic))
                .build();

        KafkaSource<Profile> profileSource = KafkaSource.<Profile>builder()
                .setBootstrapServers(cfg.brokers)
                .setTopics(cfg.usersTopic)
                .setGroupId(cfg.groupId + "-profiles")
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new ProfileDeserializer())
                .build();

        KafkaSink<ChangelogRecord> changelogSink = KafkaSink.<ChangelogRecord>builder()
                .setBootstrapServers(cfg.brokers)
                .setRecordSerializer(new ChangelogSerializer(cfg.changelogTopic))
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<UpdateBatch> updates = env
                .fromSource(eventSource, WatermarkStrategy.noWatermarks(), "txn-order-events")
                .uid("events-source")
                .keyBy(e -> e.userId)
                .process(new FeatureAggregator(new Buckets(cfg.timezone), cfg.flushMs, cfg.dedupTtlMs))
                .uid("bucket-aggregator")
                .name("bucket-aggregator");

        // Hot path: online store.
        updates.addSink(new AggSink(cfg.cql)).uid("scylla-agg-sink").name("scylla-agg-sink");

        // Changelog: feeds the offline store and is the source for fast online-store rebuilds.
        updates.flatMap(new ToChangelog()).uid("to-changelog").name("to-changelog")
                .sinkTo(changelogSink).uid("changelog-sink").name("changelog-sink");

        env.fromSource(profileSource, WatermarkStrategy.noWatermarks(), "user-profiles")
                .uid("profiles-source")
                .addSink(new ProfileSink(cfg.cql)).uid("scylla-profile-sink").name("scylla-profile-sink");

        env.execute("feature-store-aggregator");
    }
}
