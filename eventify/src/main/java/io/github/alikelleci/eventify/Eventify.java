package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandStream;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventhandling.EventStream;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultStream;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

@Slf4j
public class Eventify {

  private final Properties streamsConfig;
  private KafkaStreams.StateListener stateListener;
  private StreamsUncaughtExceptionHandler uncaughtExceptionHandler;

  private KafkaStreams kafkaStreams;

  protected Eventify(Properties streamsConfig) {
    this.streamsConfig = streamsConfig;
  }

  protected Eventify(Properties streamsConfig, KafkaStreams.StateListener stateListener, StreamsUncaughtExceptionHandler uncaughtExceptionHandler) {
    this.streamsConfig = streamsConfig;
    this.stateListener = stateListener;
    this.uncaughtExceptionHandler = uncaughtExceptionHandler;
  }

  public void start() {
    if (kafkaStreams != null) {
      log.info("Eventify already started.");
      return;
    }

    Topology topology = buildTopology();
    if (topology.describe().subtopologies().isEmpty()) {
      log.info("Eventify is not started: consumer is not subscribed to any topics or assigned any partitions");
      return;
    }

    this.kafkaStreams = new KafkaStreams(topology, this.streamsConfig);
    setUpListeners();

    log.info("Eventify is starting...");
    kafkaStreams.start();
  }

  public void stop() {
    if (kafkaStreams == null) {
      log.info("Eventify already stopped.");
      return;
    }

    log.info("Eventify is shutting down...");
    kafkaStreams.close(Duration.ofMillis(1000));
    kafkaStreams = null;
  }

  private Topology buildTopology() {
    StreamsBuilder builder = new StreamsBuilder();

    // Event store
    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("event-store"), Serdes.String(), CustomSerdes.Json(Event.class))
        .withLoggingEnabled(Collections.emptyMap()));

    // Snapshot Store
    builder.addStateStore(Stores
        .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("snapshot-store"), Serdes.String(), CustomSerdes.Json(Aggregate.class))
        .withLoggingEnabled(Collections.emptyMap()));

    if (CollectionUtils.isNotEmpty(Topics.COMMANDS)) {
      CommandStream commandStream = new CommandStream();
      commandStream.buildStream(builder);
    }

    if (CollectionUtils.isNotEmpty(Topics.EVENTS)) {
      EventStream eventStream = new EventStream();
      eventStream.buildStream(builder);
    }

    if (CollectionUtils.isNotEmpty(Topics.RESULTS)) {
      ResultStream resultStream = new ResultStream();
      resultStream.buildStream(builder);
    }

    return builder.build();
  }

  private void setUpListeners() {
    if (stateListener != null) {
      kafkaStreams.setStateListener(stateListener);
    } else {
      kafkaStreams.setStateListener((newState, oldState) -> {
        log.warn("State changed from {} to {}", oldState, newState);
      });
    }

    if (uncaughtExceptionHandler != null) {
      kafkaStreams.setUncaughtExceptionHandler(uncaughtExceptionHandler);
    } else {
      kafkaStreams.setUncaughtExceptionHandler((throwable) ->
          StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD);
    }

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Eventify is shutting down...");
      kafkaStreams.close(Duration.ofMillis(1000));
    }));

    kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
      @Override
      public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        //log.debug("onRestoreStart: endingOffset={}", endingOffset);

      }

      @Override
      public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        //log.debug("onBatchRestored: numRestored={}", numRestored);

      }

      @Override
      public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        log.debug("onRestoreEnd: totalRestored={}", totalRestored);
      }
    });
  }
}
