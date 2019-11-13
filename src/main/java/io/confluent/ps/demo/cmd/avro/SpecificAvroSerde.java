package io.confluent.ps.demo.cmd.avro;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashMap;
import java.util.Map;

public class SpecificAvroSerde implements Serde<Object> {
    private KafkaAvroSerializer serializer;
    private KafkaAvroDeserializer deserializer;

    public SpecificAvroSerde() {
    }

    @Override
    public void configure(Map<String, ?> inCfg, boolean isKey) {
        Map<String, Object> configs = new HashMap<>();
        configs.putAll(inCfg);
        configs.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        this.serializer = new KafkaAvroSerializer();
        this.serializer.configure(configs, isKey);

        this.deserializer = new KafkaAvroDeserializer();
        this.deserializer.configure(configs, isKey);
    }

    @Override
    public Serializer<Object> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<Object> deserializer() {
        return deserializer;
    }
}
