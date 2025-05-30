package io.github.alikelleci.eventify.core.messaging.eventhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class KafkaEventGateway implements EventGateway {

  private final Producer<String, Event> producer;

  protected KafkaEventGateway(Properties producerConfig, ObjectMapper objectMapper) {
    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>(objectMapper));
  }

  @Override
  public void publish(Event event) {
    ProducerRecord<String, Event> producerRecord = new ProducerRecord<>(event.getTopicInfo().value(), null, event.getTimestamp().toEpochMilli(), event.getAggregateId(), event);

    log.debug("Publishing event: {} ({})", event.getType(), event.getAggregateId());
    producer.send(producerRecord);
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Properties producerConfig;
    private ObjectMapper objectMapper;

    public Builder producerConfig(Properties producerConfig) {
      this.producerConfig = producerConfig;
      this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
      this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
      this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      this.producerConfig.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//    interceptors.add(TracingProducerInterceptor.class.getName());
//
//    this.producerConfig.putIfAbsent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

      return this;
    }

    public Builder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public KafkaEventGateway build() {
      if (this.objectMapper == null) {
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      return new KafkaEventGateway(
          this.producerConfig,
          this.objectMapper);
    }
  }
}
