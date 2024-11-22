package io.github.alikelleci.eventify.support.serializer.avro;

import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class AvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
  private final Class<T> targetType;

  public AvroDeserializer(Class<T> targetType) {
    this.targetType = targetType;
  }

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // Optional configuration if needed
  }

  @Override
  public T deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      SpecificDatumReader<T> reader = new SpecificDatumReader<>(targetType.newInstance().getSchema());
      BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
      return reader.read(null, decoder);
    } catch (Exception e) {
      throw new RuntimeException("Failed to deserialize Avro data for topic: " + topic, e);
    }
  }

  @Override
  public void close() {
    // No resources to close
  }
}
