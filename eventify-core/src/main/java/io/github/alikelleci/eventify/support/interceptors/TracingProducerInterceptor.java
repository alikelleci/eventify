package io.github.alikelleci.eventify.support.interceptors;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;

public class TracingProducerInterceptor implements ProducerInterceptor<String, Object> {

  @Override
  public ProducerRecord<String, Object> onSend(ProducerRecord<String, Object> producerRecord) {
    producerRecord.headers()
        .remove(CORRELATION_ID)
        .add(CORRELATION_ID, UUID.randomUUID().toString().getBytes(StandardCharsets.UTF_8));

    return producerRecord;
  }

  @Override
  public void onAcknowledgement(RecordMetadata recordMetadata, Exception e) {

  }

  @Override
  public void close() {

  }

  @Override
  public void configure(Map<String, ?> map) {

  }
}
