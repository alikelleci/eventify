package io.github.alikelleci.eventify.messaging.eventhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.support.serializer.json.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.UUID;

import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;

@Slf4j
public class DefaultEventGateway implements EventGateway {

  private final Producer<String, Event> producer;

  protected DefaultEventGateway(Properties producerConfig, ObjectMapper objectMapper) {
    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>(Event.class, objectMapper));
  }

  @Override
  public void publish(Object payload, Metadata metadata, Instant timestamp) {
    Event event = Event.builder()
        .timestamp(timestamp)
        .payload(payload)
        .metadata(Metadata.builder()
            .addAll(metadata)
            .add(CORRELATION_ID, UUID.randomUUID().toString())
            .build())
        .build();

    ProducerRecord<String, Event> producerRecord = new ProducerRecord<>(event.getTopicInfo().value(), null, event.getTimestamp().toEpochMilli(), event.getAggregateId(), event);

    log.trace("Publishing event: {} ({})", event.getType(), event.getAggregateId());
    producer.send(producerRecord);
  }
}
