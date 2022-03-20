package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.MessageListener;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.eventify.support.serializer.JsonDeserializer;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import io.github.alikelleci.eventify.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DefaultCommandGateway implements CommandGateway, MessageListener {

  private final Producer<String, Message> producer;
  private final Consumer<String, Message> consumer;
  private final String replyTopic;

  private final Cache<String, CompletableFuture<Object>> cache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofMinutes(5))
      .build();

  public DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic) {
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

    consumerConfig.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    consumerConfig.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerConfig.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    this.consumer = new KafkaConsumer<>(consumerConfig,
        new StringDeserializer(),
        new JsonDeserializer<>(Message.class));

    this.replyTopic = replyTopic;
  }

  private void dispatch(Command command) {
    String topic = CommonUtils.getTopicInfo(command.getPayload()).value();

    ProducerRecord<String, Message> record = new ProducerRecord<>(topic, null, command.getTimestamp().toEpochMilli(), command.getAggregateId(), command);

    log.debug("Sending command: {} ({})", command.getPayload().getClass().getSimpleName(), command.getAggregateId());
    producer.send(record);
  }

  @Override
  public CompletableFuture<Object> send(Object payload, Metadata metadata) {
    validatePayload(payload);

    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    String aggregateId = CommonUtils.getAggregateId(payload);
    String messageId = CommonUtils.createMessageId(aggregateId);
    Instant timestamp = Instant.now();

    Command command = Command.builder()
        .aggregateId(aggregateId)
        .id(messageId)
        .timestamp(timestamp)
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(Metadata.CORRELATION_ID, UUID.randomUUID().toString())
            .entry(Metadata.REPLY_TO, replyTopic)
            .build())
        .build();

    dispatch(command);

    CompletableFuture<Object> future = new CompletableFuture<>();
    cache.put(command.getId(), future);

    return future;
  }


  @Override
  public void listen() {
    AtomicBoolean closed = new AtomicBoolean(false);

    Thread thread = new Thread(() -> {
      try {
        consumer.subscribe(Collections.singletonList(replyTopic));
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

  private void onMessage(ConsumerRecords<String, Message> consumerRecords) {
    consumerRecords.forEach(record -> {
      String messageId = record.value().getId();
      if (StringUtils.isBlank(messageId)) {
        return;
      }

      CompletableFuture<Object> future = cache.getIfPresent(messageId);
      if (future != null) {
        Exception exception = checkForErrors(record);
        if (exception == null) {
          future.complete(record.value().getPayload());
        } else {
          future.completeExceptionally(exception);
        }
        cache.invalidate(messageId);
      }
    });
  }

  private Exception checkForErrors(ConsumerRecord<String, Message> record) {
    Message message = record.value();
    Metadata metadata = message.getMetadata();

    if (metadata.get(Metadata.RESULT).equals("failure")) {
      return new CommandExecutionException(metadata.get(Metadata.CAUSE));
    }

    return null;
  }
}
