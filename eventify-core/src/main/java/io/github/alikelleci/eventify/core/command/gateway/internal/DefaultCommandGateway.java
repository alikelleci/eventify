package io.github.alikelleci.eventify.core.command.gateway.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.command.exception.CommandExecutionException;
import io.github.alikelleci.eventify.core.command.gateway.CommandGateway;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
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
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.REPLY_TO;

@Slf4j
public class DefaultCommandGateway implements CommandGateway {

  /** How long a command waits for its reply before its future fails. */
  private static final Duration TIMEOUT = Duration.ofMinutes(5);

  /** How long {@link #close()} waits for the commands still being sent to reach Kafka. */
  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

  private final Cache<String, CompletableFuture<CommandResult.Success>> cache;

  private final Producer<String, Command> producer;

  private final ReplyConsumer replies;

  public DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic, ObjectMapper objectMapper) {
    this(producerConfig, consumerConfig, replyTopic, objectMapper, TIMEOUT);
  }

  DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic, ObjectMapper objectMapper, Duration timeout) {
    this.cache = Caffeine.newBuilder()
        .expireAfterWrite(timeout)
        // Expires on time: without a scheduler, entries only expire when the cache is used, e.g. by the next command.
        .scheduler(Scheduler.systemScheduler())
        .removalListener((String key, CompletableFuture<CommandResult.Success> future, RemovalCause cause) -> {
          if (cause.wasEvicted()) {
            future.completeExceptionally(new TimeoutException("Command timed out: no reply received within the allowed time."));
          }
        })
        .build();

    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>(objectMapper));

    this.replies = new ReplyConsumer(consumerConfig, replyTopic, objectMapper, this::onReplies);
    replies.start();
  }

  @Override
  public CompletableFuture<CommandResult.Success> send(Command command) {
    if (replies.isClosed()) {
      throw new IllegalStateException("The command gateway is closed.");
    }
    command.getMetadata().put(REPLY_TO, replies.getReplyTopic());

    // Built first: a command that can't be sent (e.g. without @Topic) fails here, without leaving a future behind.
    ProducerRecord<String, Command> producerRecord = new ProducerRecord<>(command.getTopic().value(), null, command.getTimestamp().toEpochMilli(), command.getAggregateId(), command);

    CompletableFuture<CommandResult.Success> future = new CompletableFuture<>();
    // One future per command id. The same command sent again while it still waits for its reply would be handled twice,
    // and would replace the first future, which then never completes: it is refused instead.
    CompletableFuture<CommandResult.Success> waiting = cache.asMap().putIfAbsent(command.getId(), future);
    if (waiting != null) {
      return CompletableFuture.failedFuture(new IllegalStateException("Command " + command.getId() + " was already sent and still waits for its result."));
    }

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

    return future;
  }

  private void failSend(Command command, CompletableFuture<CommandResult.Success> future, Exception exception) {
    log.warn("Failed to send command: {} ({})", command.getType(), command.getAggregateId(), exception);
    cache.asMap().remove(command.getId(), future);
    future.completeExceptionally(exception);
  }

  /**
   * Sends the commands that are still buffered, stops listening for replies, and fails the commands that still wait
   * for one with a {@link CancellationException}: once closed, their replies can't be received. The commands themselves
   * are still handled. After closing, {@link #send} throws.
   */
  @Override
  public void close() {
    if (replies.isClosed()) {
      return;
    }
    replies.stopListening();
    producer.close(CLOSE_TIMEOUT);
    cache.asMap().forEach((id, future) ->
        future.completeExceptionally(new CancellationException("The command gateway was closed before command " + id + " got its result.")));
    cache.invalidateAll();
  }

  /** Completes the futures of the commands the replies are for. Doesn't throw: a reply it can't handle is skipped. */
  private void onReplies(ConsumerRecords<String, CommandResult> consumerRecords) {
    consumerRecords.forEach(consumerRecord -> {
      try {
        onReply(consumerRecord);
      } catch (Exception e) {
        log.warn("Skipping reply on {}-{} at offset {}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e);
      }
    });
  }

  private void onReply(ConsumerRecord<String, CommandResult> consumerRecord) {
    CommandResult result = consumerRecord.value();
    if (result == null || result.command() == null || StringUtils.isBlank(result.command().getId())) {
      return;
    }
    String commandId = result.command().getId();
    CompletableFuture<CommandResult.Success> future = cache.getIfPresent(commandId);
    if (future != null) {
      if (result instanceof CommandResult.Failure failure) {
        future.completeExceptionally(new CommandExecutionException(failure.cause()));
      } else {
        future.complete((CommandResult.Success) result);
      }
      cache.invalidate(commandId);
    }
  }

}
