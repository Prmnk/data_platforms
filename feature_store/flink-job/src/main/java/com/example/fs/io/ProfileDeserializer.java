package com.example.fs.io;

import com.example.fs.model.Profile;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ProfileDeserializer implements KafkaRecordDeserializationSchema<Profile> {
    private static final Logger LOG = LoggerFactory.getLogger(ProfileDeserializer.class);

    private transient ObjectMapper mapper;

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Profile> out) {
        if (record.value() == null) {
            return;
        }
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        try {
            JsonNode n = mapper.readTree(record.value());
            Profile p = new Profile();
            p.userId = n.path("user_id").asText(null);
            p.segment = n.path("segment").asText(null);
            p.kycLevel = n.path("kyc_level").asText(null);
            p.signupDate = n.path("signup_date").asText(null);
            p.updatedAtMs = n.path("updated_at").asLong(record.timestamp());
            if (p.userId == null) {
                return;
            }
            out.collect(p);
        } catch (IOException ex) {
            LOG.warn("Skipping malformed profile at {}-{}@{}: {}",
                    record.topic(), record.partition(), record.offset(), ex.getMessage());
        }
    }

    @Override
    public TypeInformation<Profile> getProducedType() {
        return TypeInformation.of(Profile.class);
    }
}
