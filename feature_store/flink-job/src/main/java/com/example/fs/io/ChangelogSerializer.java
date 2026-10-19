package com.example.fs.io;

import com.example.fs.model.ChangelogRecord;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.nio.charset.StandardCharsets;

public class ChangelogSerializer implements KafkaRecordSerializationSchema<ChangelogRecord> {

    private final String topic;

    public ChangelogSerializer(String topic) {
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(ChangelogRecord r, KafkaSinkContext context, Long timestamp) {
        return new ProducerRecord<>(topic,
                r.key.getBytes(StandardCharsets.UTF_8),
                r.value.getBytes(StandardCharsets.UTF_8));
    }
}
