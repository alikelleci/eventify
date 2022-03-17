package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.constants.Config;
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
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.util.Collections;

@Slf4j
public class Eventify {

  private KafkaStreams kafkaStreams;

  protected Eventify() {
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

    this.kafkaStreams = new KafkaStreams(topology, Config.streamsConfig);
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
    kafkaStreams.setStateListener(Config.stateListener);
    kafkaStreams.setUncaughtExceptionHandler(Config.uncaughtExceptionHandler);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Eventify is shutting down...");
      kafkaStreams.close(Duration.ofMillis(1000));
    }));

    kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
      @Override
      public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        log.debug("State restoration started: topic={}, partition={}, store={}, endingOffset={}", topicPartition.topic(), topicPartition.partition(), storeName, endingOffset);
      }

      @Override
      public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        log.debug("State restoration in progress: topic={}, partition={}, store={}, numRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, numRestored);
      }

      @Override
      public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        log.debug("State restoration ended: topic={}, partition={}, store={}, totalRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
      }
    });
  }
}
