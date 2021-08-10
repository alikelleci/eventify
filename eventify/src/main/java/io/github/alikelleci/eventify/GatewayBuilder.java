package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.commandhandling.gateway.CommandGateway;
import io.github.alikelleci.eventify.messaging.commandhandling.gateway.DefaultCommandGateway;
import io.github.alikelleci.eventify.messaging.eventhandling.gateway.DefaultEventGateway;
import io.github.alikelleci.eventify.messaging.eventhandling.gateway.EventGateway;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class GatewayBuilder {

  private final Properties producerConfig;

  public GatewayBuilder(Properties producerConfig) {
    this.producerConfig = producerConfig;
    this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
    this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//    interceptors.add(TracingProducerInterceptor.class.getName());
//
//    this.producerConfig.putIfAbsent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
  }

  public CommandGateway commandGateway() {
    return new DefaultCommandGateway(producer());
  }

  public EventGateway eventGateway() {
    return new DefaultEventGateway(producer());
  }

  private Producer<String, Message> producer() {
    return new KafkaProducer<>(this.producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());
  }

}
