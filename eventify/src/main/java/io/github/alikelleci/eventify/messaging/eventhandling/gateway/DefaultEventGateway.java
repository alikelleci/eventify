package io.github.alikelleci.eventify.messaging.eventhandling.gateway;

import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.UUID;

@Slf4j
public class DefaultEventGateway implements EventGateway {

  private final Producer<String, Message> producer;

  public DefaultEventGateway(Properties producerConfig) {
    producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//    interceptors.add(TracingProducerInterceptor.class.getName());
//
//    this.producerConfig.putIfAbsent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());
  }

  @Override
  public void publish(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = new Metadata();
    }

    Event event = Event.builder()
        .payload(payload)
        .metadata(metadata.filter()
            .add(Metadata.CORRELATION_ID, UUID.randomUUID().toString()))
        .build();

    validatePayload(event);
    ProducerRecord<String, Message> record = new ProducerRecord<>(event.getTopicInfo().value(), null, event.getTimestamp().toEpochMilli(), event.getAggregateId(), event);

    log.debug("Publishing event: {} ({})", event.getPayload().getClass().getSimpleName(), event.getAggregateId());
    producer.send(record);
  }
}
