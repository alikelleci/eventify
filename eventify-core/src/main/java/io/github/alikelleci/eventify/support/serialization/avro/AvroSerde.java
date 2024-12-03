package io.github.alikelleci.eventify.support.serialization.avro;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class AvroSerde<T extends SpecificRecordBase> implements Serde<T> {
  private final AvroSerializer<T> avroSerializer;
  private final AvroDeserializer<T> avroDeserializer;

  public AvroSerde(Class<T> type) {
    this.avroSerializer = new AvroSerializer<>();
    this.avroDeserializer = new AvroDeserializer<>(type);
  }

  @Override
  public Serializer<T> serializer() {
    return this.avroSerializer;
  }

  @Override
  public Deserializer<T> deserializer() {
    return this.avroDeserializer;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.avroSerializer.configure(configs, isKey);
    this.avroDeserializer.configure(configs, isKey);
  }

  @Override
  public void close() {
    this.avroSerializer.close();
    this.avroDeserializer.close();
  }
}
