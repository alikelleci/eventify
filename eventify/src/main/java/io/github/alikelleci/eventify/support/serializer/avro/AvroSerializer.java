package io.github.alikelleci.eventify.support.serializer.avro;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

public class AvroSerializer<T extends SpecificRecordBase> implements Serializer<T> {

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    // Optional configuration if needed
  }

  @Override
  public byte[] serialize(String topic, T data) {
    if (data == null) {
      return null;
    }

    try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      SpecificDatumWriter<T> writer = new SpecificDatumWriter<>(data.getSchema());
      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(outputStream, null);
      writer.write(data, encoder);
      encoder.flush();
      return outputStream.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException("Failed to serialize Avro data for topic: " + topic, e);
    }
  }

  @Override
  public void close() {
    // No resources to close
  }
}
