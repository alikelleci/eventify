package io.github.alikelleci.eventify.core.plugin;

import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;

/**
 * Something that runs along with Eventify. Everything a plugin can hook into is on this interface, and all of it is
 * optional: implement what you need.
 *
 * <p>The listeners are called on Kafka Streams' own threads, so keep them short: remember something, don't block.
 * An exception from a plugin is logged and reaches neither Kafka Streams nor the other plugins.
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
