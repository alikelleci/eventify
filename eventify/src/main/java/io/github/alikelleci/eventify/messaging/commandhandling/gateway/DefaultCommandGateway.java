package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.MessageListener;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.eventify.support.serializer.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class DefaultCommandGateway extends AbstractCommandGateway implements CommandGateway, MessageListener {

  //private final Map<String, CompletableFuture<Object>> futures = new ConcurrentHashMap<>();
  private final Cache<String, CompletableFuture<Object>> cache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofMinutes(5))
      .build();

  private final Consumer<String, Message> consumer;
  private final String replyTopic;


  protected DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic) {
    super(producerConfig);

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

  @Override
  public CompletableFuture<Object> send(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = new Metadata();
    }

    Command command = Command.builder()
        .payload(payload)
        .metadata(metadata.filter()
            .add(Metadata.CORRELATION_ID, UUID.randomUUID().toString())
            .add(Metadata.REPLY_TO, replyTopic))
        .build();

    validatePayload(command);
    super.dispatch(command);

    CompletableFuture<Object> future = new CompletableFuture<>();
    cache.put(command.getId(), future);

    return future;
  }

  @Override
  public void listen() {
    AtomicBoolean closed = new AtomicBoolean(false);

    Thread thread = new Thread(() -> {
      consumer.subscribe(Collections.singletonList(replyTopic));
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

  private void onMessage(ConsumerRecords<String, Message> consumerRecords) {
    consumerRecords.forEach(record -> {
      String messageId = record.value().getId();
      if (StringUtils.isBlank(messageId)) {
        return;
      }
      // CompletableFuture<Object> future = futures.remove(messageId);
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
