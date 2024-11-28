package io.github.alikelleci.eventify.messaging.eventhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.support.serializer.json.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class DefaultEventGateway implements EventGateway {

  private final Producer<String, Event> producer;

  protected DefaultEventGateway(Properties producerConfig, ObjectMapper objectMapper) {
    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>(objectMapper));
  }

  @Override
  public void publish(Event event) {
    ProducerRecord<String, Event> producerRecord = new ProducerRecord<>(event.getTopicInfo().value(), null, event.getTimestamp().toEpochMilli(), event.getAggregateId(), event);

    log.trace("Publishing event: {} ({})", event.getType(), event.getAggregateId());
    producer.send(producerRecord);
  }
}
