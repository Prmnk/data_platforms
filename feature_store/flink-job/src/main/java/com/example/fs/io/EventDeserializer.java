package com.example.fs.io;

import com.example.fs.model.Event;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/** JSON -> Event. The kind (txn / order) comes from the topic. Malformed records are logged and skipped. */
public class EventDeserializer implements KafkaRecordDeserializationSchema<Event> {
    private static final Logger LOG = LoggerFactory.getLogger(EventDeserializer.class);

    private final String ordersTopic;
    private transient ObjectMapper mapper;

    public EventDeserializer(String ordersTopic) {
        this.ordersTopic = ordersTopic;
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Event> out) {
        if (record.value() == null) {
            return;
        }
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            JsonNode n = mapper.readTree(record.value());
            Event e = new Event();
            e.eventId = n.path("event_id").asText(null);
            e.userId = n.path("user_id").asText(null);
            e.op = n.path("op").asText("create");
            e.amount = n.path("amount").asLong();
            e.eventTimeMs = n.path("event_time").asLong(record.timestamp());
            e.originalEventTimeMs = n.path("original_event_time").asLong(0);
            e.ingestTimeMs = record.timestamp();
            e.kind = ordersTopic.equals(record.topic()) ? Event.ORDER : Event.TXN;
            if (e.eventId == null || e.userId == null) {
                LOG.warn("Skipping event without event_id/user_id at {}-{}@{}",
                        record.topic(), record.partition(), record.offset());
                return;
            }
            out.collect(e);
        } catch (IOException ex) {
            LOG.warn("Skipping malformed record at {}-{}@{}: {}",
                    record.topic(), record.partition(), record.offset(), ex.getMessage());
        }
    }

    @Override
    public TypeInformation<Event> getProducedType() {
        return TypeInformation.of(Event.class);
    }
}
