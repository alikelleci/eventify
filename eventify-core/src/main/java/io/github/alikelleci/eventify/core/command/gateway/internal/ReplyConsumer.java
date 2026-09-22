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
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
/**
 * Receives the replies to the commands a gateway sent, on its own thread, and hands them to the gateway. Reads
 * partition 0 of the reply topic: the replies are written there.
 */
public class ReplyConsumer {

  /** How long to wait before polling again after an unexpected error, so a lasting one isn't retried in a busy loop. */
  private static final Duration ERROR_PAUSE = Duration.ofSeconds(1);

  /** How long {@link #stopListening()} waits for the listening thread to finish its poll and close the consumer. */
  private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);

  /** How long gateway creation waits for its reply consumer to start at the end of the reply topic. */
  private static final Duration START_TIMEOUT = Duration.ofSeconds(10);

  private final Consumer<String, CommandResult> consumer;
  private final String replyTopic;
  /** Handles a batch of replies. It must not throw: a record it can't handle is skipped by it. */
  private final java.util.function.Consumer<ConsumerRecords<String, CommandResult>> onReplies;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final CountDownLatch ready = new CountDownLatch(1);
  private final AtomicReference<Throwable> startupFailure = new AtomicReference<>();
  private Thread thread;
  private Thread shutdownHook;

  public ReplyConsumer(Properties consumerConfig, String replyTopic, ObjectMapper objectMapper,
                       java.util.function.Consumer<ConsumerRecords<String, CommandResult>> onReplies) {
    consumerConfig.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//    consumerConfig.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    consumerConfig.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerConfig.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    this.consumer = new KafkaConsumer<>(consumerConfig,
        new StringDeserializer(),
        new JsonDeserializer<>(CommandResult.class, objectMapper));

    this.replyTopic = replyTopic;
    this.onReplies = onReplies;
  }

  /**
   * Starts listening for replies. Called once the gateway is fully constructed: replies are handed to it on another
   * thread, which must not see the gateway's fields before they are set.
   */
  public void start() {
    if (thread != null) {
      throw new IllegalStateException("The reply consumer for " + replyTopic + " is already started.");
    }
    thread = new Thread(() -> {
      TopicPartition partition = new TopicPartition(replyTopic, 0);
      try {
        consumer.assign(Collections.singletonList(partition));
        // latest only means "latest when the position is established". Establish it before a command can be sent,
        // otherwise a fast reply between construction and the first poll could be skipped forever.
        consumer.seekToEnd(Collections.singletonList(partition));
        consumer.position(partition, START_TIMEOUT);
        ready.countDown();
        while (!closed.get()) {
          ConsumerRecords<String, CommandResult> consumerRecords;
          try {
            consumerRecords = consumer.poll(Duration.ofMillis(1000));
          } catch (RecordDeserializationException e) {
            // Not a reply this gateway can read: skipped, so the replies after it still arrive.
            log.warn("Skipping unreadable record on {} at offset {}", e.topicPartition(), e.offset(), e);
            consumer.seek(e.topicPartition(), e.offset() + 1);
            continue;
          } catch (WakeupException e) {
            throw e;
          } catch (Exception e) {
            // The thread keeps listening: once it stops, every command sent through this gateway would time out.
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
        // Ignore exception if closing
        if (!closed.get()) {
          if (ready.getCount() != 0) {
            startupFailure.compareAndSet(null, e);
            return;
          }
          throw e;
        }
      } catch (Exception e) {
        if (ready.getCount() != 0) {
          startupFailure.compareAndSet(null, e);
          return;
        }
        throw new IllegalStateException("Reply consumer stopped unexpectedly for " + replyTopic + ".", e);
      } finally {
        ready.countDown();
        consumer.close();
      }
    }, "eventify-command-gateway-" + replyTopic);
    // A daemon: a gateway that is never closed doesn't keep the JVM running.
    thread.setDaemon(true);

    // For a gateway that is never closed: its consumer still leaves cleanly when the JVM exits.
    shutdownHook = new Thread(this::signalStop, "eventify-command-gateway-shutdown");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    thread.start();
    try {
      awaitReady();
    } catch (RuntimeException e) {
      signalStop();
      removeShutdownHook();
      throw e;
    }
  }

  /**
   * Stops listening: ends the poll loop, closes the consumer and waits (a while) for that to finish. Replies that
   * arrive afterwards are not received.
   */
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

  private void awaitReady() {
    try {
      if (!ready.await(START_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS)) {
        signalStop();
        throw new IllegalStateException("The reply consumer for " + replyTopic + " did not reach the end of its topic within "
            + START_TIMEOUT.toSeconds() + " seconds.");
      }
    } catch (InterruptedException e) {
      signalStop();
      Thread.currentThread().interrupt();
      throw new IllegalStateException("Interrupted while starting the reply consumer for " + replyTopic + ".", e);
    }
    Throwable failure = startupFailure.get();
    if (failure != null) {
      throw new IllegalStateException("Could not start the reply consumer for " + replyTopic + ".", failure);
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
