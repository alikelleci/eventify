package io.github.alikelleci.eventify.core.command.gateway.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.ScheduleToken;
import io.github.alikelleci.eventify.core.command.TopicNames;
import io.github.alikelleci.eventify.core.command.exception.CommandExecutionException;
import io.github.alikelleci.eventify.core.command.gateway.CommandGateway;
import io.github.alikelleci.eventify.core.command.internal.HeaderNames;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;

@Slf4j
public class DefaultCommandGateway implements CommandGateway {

  /** How long a command waits for its reply before its future fails. */
  private static final Duration TIMEOUT = Duration.ofMinutes(5);

  /** How long {@link #close()} waits for the commands still being sent to reach Kafka. */
  private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);

  private final Cache<String, CompletableFuture<List<Event>>> cache;

  private final Producer<String, Command> producer;

  private final ReplyConsumer replies;

  /**
   * Completes the futures, so callbacks never block the reply, producer or cache threads.
   * Unbounded: a callback may wait for another reply and hold its thread.
   */
  private final ExecutorService completions = Executors.newCachedThreadPool(runnable -> {
    Thread thread = new Thread(runnable, "eventify-command-gateway-completion");
    thread.setDaemon(true);
    return thread;
  });

  public DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic, ObjectMapper objectMapper) {
    this(producerConfig, consumerConfig, replyTopic, objectMapper, TIMEOUT);
  }

  DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic, ObjectMapper objectMapper, Duration timeout) {
    this.cache = Caffeine.newBuilder()
        .expireAfterWrite(timeout)
        // Without a scheduler, entries only expire when the cache is used.
        .scheduler(Scheduler.systemScheduler())
        .removalListener((String key, CompletableFuture<List<Event>> future, RemovalCause cause) -> {
          if (cause.wasEvicted()) {
            completeExceptionally(future, new TimeoutException("Command timed out: no reply received within the allowed time."));
          }
        })
        .build();

    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new CommandSerde(objectMapper).serializer());

    this.replies = new ReplyConsumer(consumerConfig, replyTopic, objectMapper, this::onReplies);
    try {
      replies.start();
    } catch (RuntimeException e) {
      producer.close(CLOSE_TIMEOUT);
      throw e;
    }
  }

  @Override
  public CompletableFuture<List<Event>> send(Command command) {
    if (replies.isClosed()) {
      throw new IllegalStateException("The command gateway is closed.");
    }

    // Built first: a command without @Topic fails here, before a future is registered.
    ProducerRecord<String, Command> producerRecord = new ProducerRecord<>(command.getTopic().value(), null, command.getTimestamp().toEpochMilli(), command.getAggregateId(), command,
        List.of(new RecordHeader(HeaderNames.REPLY_TO, replies.getReplyTopic().getBytes(StandardCharsets.UTF_8))));

    CompletableFuture<List<Event>> future = new CompletableFuture<>();
    // Refused while the same command still waits: it would be handled twice and replace the first future.
    CompletableFuture<List<Event>> waiting = cache.asMap().putIfAbsent(command.getId(), future);
    if (waiting != null) {
      return CompletableFuture.failedFuture(new IllegalStateException("Command " + command.getId() + " was already sent and still waits for its result."));
    }

    log.debug("Sending command: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId());
    try {
      // A failed send fails the future right away instead of by timeout.
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

  @Override
  public CompletableFuture<ScheduleToken> schedule(Command command, Instant deliverAt) {
    if (replies.isClosed()) {
      throw new IllegalStateException("The command gateway is closed.");
    }
    Objects.requireNonNull(deliverAt, "deliverAt");
    if (deliverAt.toEpochMilli() < 0) {
      throw new IllegalArgumentException("A delivery time can't be before 1970, not " + deliverAt + ".");
    }
    // In milliseconds, as it is stored: a cancel finds the command by it.
    ScheduleToken token = new ScheduleToken(command.getTopic().value(), command.getAggregateId(), command.getId(), Instant.ofEpochMilli(deliverAt.toEpochMilli()));

    // No reply header: nobody waits when the command is handled.
    ProducerRecord<String, Command> producerRecord = new ProducerRecord<>(TopicNames.scheduledTopicOf(token.topic()), null, command.getTimestamp().toEpochMilli(), token.aggregateId(), command,
        List.of(deliverAtHeader(token)));

    log.debug("Scheduling command: commandId={}, commandType={}, aggregateId={}, deliverAt={}", command.getId(), command.getType(), command.getAggregateId(), token.deliverAt());
    return write(producerRecord, token, e -> log.warn("Failed to schedule command: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId(), e));
  }

  @Override
  public CompletableFuture<Void> cancel(ScheduleToken token) {
    if (replies.isClosed()) {
      throw new IllegalStateException("The command gateway is closed.");
    }
    Objects.requireNonNull(token, "token");

    // Same key as the scheduled command: the same partition, so the same store. No value: nothing to deliver.
    ProducerRecord<String, Command> producerRecord = new ProducerRecord<>(TopicNames.scheduledTopicOf(token.topic()), null, null, token.aggregateId(), null,
        List.of(new RecordHeader(HeaderNames.CANCEL, token.commandId().getBytes(StandardCharsets.UTF_8)), deliverAtHeader(token)));

    log.debug("Cancelling scheduled command: commandId={}, aggregateId={}, deliverAt={}", token.commandId(), token.aggregateId(), token.deliverAt());
    return write(producerRecord, null, e -> log.warn("Failed to cancel scheduled command: commandId={}, aggregateId={}", token.commandId(), token.aggregateId(), e));
  }

  private static RecordHeader deliverAtHeader(ScheduleToken token) {
    return new RecordHeader(HeaderNames.DELIVER_AT, Long.toString(token.deliverAt().toEpochMilli()).getBytes(StandardCharsets.UTF_8));
  }

  /** Completes with the value once Kafka has the record, or with the reason it doesn't. */
  private <T> CompletableFuture<T> write(ProducerRecord<String, Command> producerRecord, T value, Consumer<Exception> logFailure) {
    CompletableFuture<T> future = new CompletableFuture<>();
    producer.send(producerRecord, (metadata, exception) -> {
      if (exception != null) {
        logFailure.accept(exception);
        completeExceptionally(future, exception);
      } else {
        complete(() -> future.complete(value));
      }
    });
    return future;
  }

  private void failSend(Command command, CompletableFuture<List<Event>> future, Exception exception) {
    log.warn("Failed to send command: commandId={}, commandType={}, aggregateId={}", command.getId(), command.getType(), command.getAggregateId(), exception);
    cache.asMap().remove(command.getId(), future);
    completeExceptionally(future, exception);
  }

  private void completeExceptionally(CompletableFuture<?> future, Throwable exception) {
    complete(() -> future.completeExceptionally(exception));
  }

  private void complete(Runnable completion) {
    try {
      completions.execute(completion);
    } catch (RejectedExecutionException e) {
      completion.run(); // closed meanwhile: its future must still complete
    }
  }

  /** Flushes pending sends, stops listening, and fails waiting commands with {@link CancellationException}. */
  @Override
  public void close() {
    if (replies.isClosed()) {
      return;
    }
    replies.stopListening();
    producer.close(CLOSE_TIMEOUT);
    // On this thread: every waiting command has failed when close() returns.
    cache.asMap().forEach((id, future) ->
        future.completeExceptionally(new CancellationException("The command gateway was closed before command " + id + " got its result.")));
    cache.invalidateAll();
    completions.shutdown(); // still runs the completions already handed to it
  }

  /** Completes the waiting futures; a reply that can't be handled is skipped. */
  private void onReplies(ConsumerRecords<String, CommandResult> consumerRecords) {
    consumerRecords.forEach(consumerRecord -> {
      try {
        onReply(consumerRecord);
      } catch (Exception e) {
        log.warn("Skipping command reply, handling failed: topic={}, partition={}, offset={}", consumerRecord.topic(), consumerRecord.partition(), consumerRecord.offset(), e);
      }
    });
  }

  private void onReply(ConsumerRecord<String, CommandResult> consumerRecord) {
    CommandResult result = consumerRecord.value();
    if (result == null || result.command() == null || StringUtils.isBlank(result.command().getId())) {
      return;
    }
    // Removed before it completes: its callbacks may send the same command again.
    CompletableFuture<List<Event>> future = cache.asMap().remove(result.command().getId());
    if (future == null) {
      return;
    }
    if (result instanceof CommandResult.Failure failure) {
      completeExceptionally(future, new CommandExecutionException(failure.cause()));
    } else {
      complete(() -> future.complete(((CommandResult.Success) result).events()));
    }
  }

}
