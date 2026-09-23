package io.github.alikelleci.eventify.core.plugin;

import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;

/**
 * Hooks into Eventify; every method is optional. Listeners run on Kafka Streams' threads, so don't block.
 * A plugin's exception is logged and goes no further.
 */
public interface EventifyPlugin {

  /** Eventify has started; Kafka Streams is running. */
  default void onStart(PluginContext context) {
  }

  /** Eventify is stopping. */
  default void onStop(PluginContext context) {
  }

  /** Told when Kafka Streams changes state, e.g. to REBALANCING or ERROR; {@code null} to not be told. */
  default StateListener stateListener() {
    return null;
  }

  /** Told about the state stores being restored and how far they are; {@code null} to not be told. */
  default StateRestoreListener stateRestoreListener() {
    return null;
  }
}
