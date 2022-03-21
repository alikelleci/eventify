package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import io.github.alikelleci.eventify.messaging.AbstractMessageListener;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

@Slf4j
public abstract class AbstractCommandGateway extends AbstractMessageListener {

  private final Producer<String, Message> producer;

  protected AbstractCommandGateway(Properties producerConfig, Properties consumerConfig) {
    super(consumerConfig);

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

  public void dispatch(Command command) {
    ProducerRecord<String, Message> record = new ProducerRecord<>(command.getTopicInfo().value(), null, command.getTimestamp().toEpochMilli(), command.getAggregateId(), command);

    log.debug("Sending command: {} ({})", command.getPayload().getClass().getSimpleName(), command.getAggregateId());
    producer.send(record);
  }
}
