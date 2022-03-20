package io.github.alikelleci.eventify.messaging.eventhandling.gateway;

import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import io.github.alikelleci.eventify.util.CommonUtils;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

@Slf4j
public class DefaultEventGateway implements EventGateway {

  private final Producer<String, Message> producer;

  @Builder
  public DefaultEventGateway(Properties producerConfig) {
    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());
  }

  @Override
  public void publish(Object payload, Metadata metadata) {
    validatePayload(payload);

    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    String aggregateId = CommonUtils.getAggregateId(payload);
    String messageId = CommonUtils.createMessageId(aggregateId);
    Instant timestamp = Instant.now();
    String topic = CommonUtils.getTopicInfo(payload).value();
    String correlationId = UUID.randomUUID().toString();

    Event event = Event.builder()
        .aggregateId(aggregateId)
        .id(messageId)
        .timestamp(timestamp)
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(Metadata.CORRELATION_ID, correlationId)
            .build())
        .build();

    ProducerRecord<String, Message> record = new ProducerRecord<>(topic, null, timestamp.toEpochMilli(), aggregateId, event);

    log.debug("Publishing event: {} ({})", payload.getClass().getSimpleName(), event.getAggregateId());
    producer.send(record);
  }
}
