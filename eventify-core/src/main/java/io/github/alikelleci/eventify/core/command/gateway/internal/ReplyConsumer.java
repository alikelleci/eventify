package io.github.alikelleci.eventify.core.command.gateway.internal;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

/** Receives the gateway's replies on its own thread, from partition 0 of the reply topic. */
@Slf4j
public class ReplyConsumer {

  /** Pause after a poll error, so a lasting one isn't retried in a busy loop. */
  private static final Duration ERROR_PAUSE = Duration.ofSeconds(1);

  /** How long {@link #stopListening()} waits for the thread to close the consumer. */
  private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);

  /** How long startup waits for the end of the reply topic. */
  private static final Duration START_TIMEOUT = Duration.ofSeconds(10);

  private final Consumer<String, CommandResult> consumer;
  private final String replyTopic;
  /** Handles a batch of replies; must not throw. */
  private final java.util.function.Consumer<ConsumerRecords<String, CommandResult>> onReplies;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private Thread thread;
  private Thread shutdownHook;

  public ReplyConsumer(Properties config, String replyTopic, ObjectMapper objectMapper,
                       java.util.function.Consumer<ConsumerRecords<String, CommandResult>> onReplies) {
    Properties consumerConfig = new Properties();
    consumerConfig.putAll(config);
    consumerConfig.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerConfig.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    this.consumer = new KafkaConsumer<>(consumerConfig,
        new StringDeserializer(),
        new JsonDeserializer<>(CommandResult.class, objectMapper));

    this.replyTopic = replyTopic;
    this.onReplies = onReplies;
  }

  /** Starts listening; called once the gateway is constructed, since replies reach it from another thread. */
  public void start() {
    if (thread != null) {
      throw new IllegalStateException("The reply consumer for " + replyTopic + " is already started.");
    }
    TopicPartition partition = new TopicPartition(replyTopic, 0);
    try {
      consumer.assign(List.of(partition));
      // Fix the position now: "latest" is only resolved on first poll, and could skip an early reply.
      consumer.seekToEnd(List.of(partition));
      consumer.position(partition, START_TIMEOUT);
    } catch (RuntimeException e) {
      closed.set(true);
      consumer.close();
      throw new IllegalStateException("Could not start the reply consumer for " + replyTopic + ".", e);
    }

    thread = new Thread(this::listen, "eventify-command-gateway-" + replyTopic);
    // A gateway that is never closed doesn't keep the JVM running.
    thread.setDaemon(true);
    // A gateway that is never closed still leaves the group cleanly when the JVM exits.
    shutdownHook = new Thread(this::signalStop, "eventify-command-gateway-shutdown");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    thread.start();
  }

  private void listen() {
    try {
      while (!closed.get()) {
        ConsumerRecords<String, CommandResult> consumerRecords;
        try {
          consumerRecords = consumer.poll(Duration.ofMillis(1000));
        } catch (RecordDeserializationException e) {
          // Skip an unreadable record, so later replies still arrive.
          log.warn("Skipping unreadable record on {} at offset {}", e.topicPartition(), e.offset(), e);
          consumer.seek(e.topicPartition(), e.offset() + 1);
          continue;
        } catch (WakeupException e) {
          throw e;
        } catch (Exception e) {
          // Keep listening: if this thread stops, every command times out.
          log.error("Failed to poll replies from {}", replyTopic, e);
          if (!pause()) {
            break;
          }
          continue;
        }
        try {
          onReplies.accept(consumerRecords);
        } catch (Exception e) {
          log.error("Failed to handle replies from {}", replyTopic, e);
        }
      }
    } catch (WakeupException e) {
      // closing: only signalStop() wakes the consumer
    } finally {
      consumer.close();
    }
  }

  /** Ends the poll loop, closes the consumer and waits a while for that; later replies are not received. */
  public void stopListening() {
    signalStop();
    removeShutdownHook();
    if (thread != null && thread != Thread.currentThread()) {
      try {
        thread.join(STOP_TIMEOUT.toMillis());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  private void signalStop() {
    if (closed.compareAndSet(false, true)) {
      consumer.wakeup();
    }
  }

  private void removeShutdownHook() {
    if (shutdownHook == null) {
      return;
    }
    try {
      Runtime.getRuntime().removeShutdownHook(shutdownHook);
    } catch (IllegalStateException e) {
      // The JVM is shutting down already: the hook runs anyway.
    }
  }

  public boolean isClosed() {
    return closed.get();
  }

  /** @return {@code false} when the thread was interrupted while waiting, and must stop */
  private static boolean pause() {
    try {
      Thread.sleep(ERROR_PAUSE.toMillis());
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }


  public String getReplyTopic() {
    return replyTopic;
  }
}
