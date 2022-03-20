package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.messaging.commandhandling.gateway.CommandGateway;
import io.github.alikelleci.eventify.messaging.commandhandling.gateway.DefaultCommandGateway;
import io.github.alikelleci.eventify.messaging.eventhandling.gateway.DefaultEventGateway;
import io.github.alikelleci.eventify.messaging.eventhandling.gateway.EventGateway;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public class GatewayBuilder {

  private final Properties producerConfig;
  private final Properties consumerConfig;

  public GatewayBuilder(Properties producerConfig, Properties consumerConfig) {
    this.producerConfig = producerConfig;
    this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
    this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

    this.consumerConfig = consumerConfig;
    this.consumerConfig.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    this.consumerConfig.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    this.consumerConfig.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    this.consumerConfig.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    this.consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    this.consumerConfig.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());


//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//    interceptors.add(TracingProducerInterceptor.class.getName());
//
//    this.producerConfig.putIfAbsent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
  }

  public CommandGateway commandGateway() {
    return DefaultCommandGateway.builder()
        .producerConfig(producerConfig)
        .consumerConfig(consumerConfig)
        .build();
  }

  public EventGateway eventGateway() {
    return DefaultEventGateway.builder()
        .producerConfig(producerConfig)
        .build();
  }


}
