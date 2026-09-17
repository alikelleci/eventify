package io.github.alikelleci.eventify.core.messaging.commandhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
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
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public abstract class AbstractCommandResultListener {

  /** How long to wait before polling again after an unexpected error, so a lasting one isn't retried in a busy loop. */
  private static final Duration ERROR_PAUSE = Duration.ofSeconds(1);

  /** How long {@link #stopListening()} waits for the listening thread to finish its poll and close the consumer. */
  private static final Duration STOP_TIMEOUT = Duration.ofSeconds(10);

  private final Consumer<String, Command> consumer;
  private final String replyTopic;
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private Thread thread;
  private Thread shutdownHook;

  protected AbstractCommandResultListener(Properties consumerConfig, String replyTopic, ObjectMapper objectMapper) {
    consumerConfig.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    consumerConfig.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
//    consumerConfig.putIfAbsent(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
    consumerConfig.putIfAbsent(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
    consumerConfig.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
    consumerConfig.putIfAbsent(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());

    this.consumer = new KafkaConsumer<>(consumerConfig,
        new StringDeserializer(),
        new JsonDeserializer<>(Command.class, objectMapper));

    this.replyTopic = replyTopic;
  }

  /**
   * Starts listening for replies. Called by the subclass once it is fully constructed: replies are handed to
   * {@link #onMessage} on another thread, which must not see the subclass's fields before they are set.
   */
  protected void start() {
    thread = new Thread(() -> {
      consumer.assign(Collections.singletonList(new TopicPartition(replyTopic, 0)));
//      consumer.subscribe(Collections.singletonList(this.replyTopic));
      try {
        while (!closed.get()) {
          ConsumerRecords<String, Command> consumerRecords;
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
            onMessage(consumerRecords);
          } catch (Exception e) {
            log.error("Failed to handle replies from {}", replyTopic, e);
          }
        }
      } catch (WakeupException e) {
        // Ignore exception if closing
        if (!closed.get()) throw e;
      } finally {
        consumer.close();
      }
    }, "eventify-command-gateway-" + replyTopic);
    // A daemon: a gateway that is never closed doesn't keep the JVM running.
    thread.setDaemon(true);

    // For a gateway that is never closed: its consumer still leaves cleanly when the JVM exits.
    shutdownHook = new Thread(this::signalStop, "eventify-command-gateway-shutdown");
    Runtime.getRuntime().addShutdownHook(shutdownHook);
    thread.start();
  }

  /**
   * Stops listening: ends the poll loop, closes the consumer and waits (a while) for that to finish. Replies that
   * arrive afterwards are not received.
   */
  protected void stopListening() {
    signalStop();
    if (shutdownHook != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
      } catch (IllegalStateException e) {
        // The JVM is shutting down already: the hook runs anyway.
      }
    }
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

  protected boolean isClosed() {
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

  /** Handles a batch of replies. It must not throw: a record it can't handle is skipped by it. */
  protected abstract void onMessage(ConsumerRecords<String, Command> consumerRecords);

  protected String getReplyTopic() {
    return replyTopic;
  }
}
