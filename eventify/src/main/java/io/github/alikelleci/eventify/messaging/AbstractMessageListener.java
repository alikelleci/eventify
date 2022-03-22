package io.github.alikelleci.eventify.messaging;

import io.github.alikelleci.eventify.support.serializer.JsonDeserializer;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractMessageListener implements MessageListener {

  private final Consumer<String, Message> consumer;

  protected AbstractMessageListener(Properties consumerConfig) {
    consumerConfig.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    consumerConfig.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerConfig.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    this.consumer = new KafkaConsumer<>(consumerConfig,
        new StringDeserializer(),
        new JsonDeserializer<>(Message.class));
  }

  protected void listen(String topic) {
    AtomicBoolean closed = new AtomicBoolean(false);

    Thread thread = new Thread(() -> {
      consumer.subscribe(Collections.singletonList(topic));
      try {
        while (!closed.get()) {
          ConsumerRecords<String, Message> consumerRecords = consumer.poll(Duration.ofMillis(1000));
          onMessage(consumerRecords);
        }
      } catch (WakeupException e) {
        // Ignore exception if closing
        if (!closed.get()) throw e;
      } finally {
        consumer.close();
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      closed.set(true);
      consumer.wakeup();
    }));
    thread.start();
  }

}
