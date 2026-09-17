package io.github.alikelleci.eventify.core.messaging.commandhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static io.github.alikelleci.eventify.core.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.core.messaging.Metadata.REPLY_TO;
import static io.github.alikelleci.eventify.core.messaging.Metadata.RESULT;

@Slf4j
public class DefaultCommandGateway extends AbstractCommandResultListener implements CommandGateway {

  /** How long a command waits for its reply before its future fails. */
  private static final Duration TIMEOUT = Duration.ofMinutes(5);

  private final Cache<String, CompletableFuture<Object>> cache;

  private final Producer<String, Command> producer;

  protected DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic, ObjectMapper objectMapper) {
    this(producerConfig, consumerConfig, replyTopic, objectMapper, TIMEOUT);
  }

  DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic, ObjectMapper objectMapper, Duration timeout) {
    super(consumerConfig, replyTopic, objectMapper);

    this.cache = Caffeine.newBuilder()
        .expireAfterWrite(timeout)
        // Expires on time: without a scheduler, entries only expire when the cache is used, e.g. by the next command.
        .scheduler(Scheduler.systemScheduler())
        .removalListener((String key, CompletableFuture<Object> future, RemovalCause cause) -> {
          if (cause.wasEvicted()) {
            future.completeExceptionally(new TimeoutException("Command timed out: no reply received within the allowed time."));
          }
        })
        .build();

    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>(objectMapper));

    start();
  }

  @Override
  public <R> CompletableFuture<R> send(Command command) {
    command.getMetadata().put(REPLY_TO, getReplyTopic());

    // Built first: a command that can't be sent (e.g. without @TopicInfo) fails here, without leaving a future behind.
    ProducerRecord<String, Command> producerRecord = new ProducerRecord<>(command.getTopicInfo().value(), null, command.getTimestamp().toEpochMilli(), command.getAggregateId(), command);

    CompletableFuture<Object> future = new CompletableFuture<>();
    cache.put(command.getId(), future);

    log.debug("Sending command: {} ({})", command.getType(), command.getAggregateId());
    try {
      // A command that doesn't reach Kafka never gets a result: its future fails with the reason right away, instead of
      // with a timeout later (e.g. no access to the topic, a command too large, or the broker unreachable too long).
      producer.send(producerRecord, (metadata, exception) -> {
        if (exception != null) {
          failSend(command, future, exception);
        }
      });
    } catch (RuntimeException e) {
      failSend(command, future, e);
      throw e;
    }

    return (CompletableFuture<R>) future;
  }

  private void failSend(Command command, CompletableFuture<Object> future, Exception exception) {
    log.warn("Failed to send command: {} ({})", command.getType(), command.getAggregateId(), exception);
    cache.invalidate(command.getId());
    future.completeExceptionally(exception);
  }

  @Override
  protected void onMessage(ConsumerRecords<String, Command> consumerRecords) {
    consumerRecords.forEach(consumerRecord -> {
      try {
        onReply(consumerRecord);
      } catch (Exception e) {
        log.warn("Skipping reply on {}-{} at offset {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e);
      }
    });
  }

  private void onReply(ConsumerRecord<String, Command> consumerRecord) {
    Command command = consumerRecord.value();
    if (command == null || StringUtils.isBlank(command.getId())) {
      return;
    }
    CompletableFuture<Object> future = cache.getIfPresent(command.getId());
    if (future != null) {
      Exception exception = checkForErrors(consumerRecord);
      if (exception == null) {
        future.complete(command.getPayload());
      } else {
        future.completeExceptionally(exception);
      }
      cache.invalidate(command.getId());
    }
  }

  private Exception checkForErrors(ConsumerRecord<String, Command> consumerRecord) {
    Command command = consumerRecord.value();
    Metadata metadata = command.getMetadata();

    if ("failure".equals(metadata.get(RESULT))) {
      return new CommandExecutionException(metadata.get(CAUSE));
    }

    return null;
  }

}
