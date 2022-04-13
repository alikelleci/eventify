package io.github.alikelleci.eventify.messaging.eventhandling.gateway;

import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Properties;

@Slf4j
public class DefaultEventGateway implements EventGateway {

  private final Producer<String, Event> producer;

  protected DefaultEventGateway(Properties producerConfig) {
    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());
  }

  @Override
  public void publish(Object payload, Metadata metadata, Instant timestamp) {
    Event event = Event.builder()
        .payload(payload)
        .metadata(Metadata.builder()
            .addAll(metadata)
            .build())
        .timestamp(timestamp)
        .build();

    validate(event);
    ProducerRecord<String, Event> record = new ProducerRecord<>(event.getTopicInfo().value(), null, event.getTimestamp().toEpochMilli(), event.getAggregateId(), event);

    log.debug("Publishing event: {} ({})", event.getType(), event.getAggregateId());
    producer.send(record);
  }
}
