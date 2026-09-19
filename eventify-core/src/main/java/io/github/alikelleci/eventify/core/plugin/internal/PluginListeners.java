package io.github.alikelleci.eventify.core.plugin.internal;

import io.github.alikelleci.eventify.core.plugin.EventifyPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Tells the plugins what happens. Kafka Streams takes one listener of each kind, so one listener passes everything on
 * to the plugins that asked. Each is told on the calling thread; one that throws is logged and skipped.
 */
@Slf4j
public class PluginListeners {

  private final List<EventifyPlugin> plugins;

  public PluginListeners(List<EventifyPlugin> plugins) {
    this.plugins = List.copyOf(plugins);
  }

  /** Tells every plugin, e.g. {@code plugin -> plugin.onStart(eventify)}. */
  public void notifyPlugins(String hook, Consumer<EventifyPlugin> call) {
    tellAll(plugins, hook, call);
  }

  public StateListener stateListener() {
    List<StateListener> listeners = listenersOf(EventifyPlugin::stateListener);
    return (newState, oldState) ->
        tellAll(listeners, "onChange", listener -> listener.onChange(newState, oldState));
  }

  public StateRestoreListener stateRestoreListener() {
    List<StateRestoreListener> listeners = listenersOf(EventifyPlugin::stateRestoreListener);
    return new StateRestoreListener() {
      @Override
      public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        tellAll(listeners, "onRestoreStart", listener -> listener.onRestoreStart(topicPartition, storeName, startingOffset, endingOffset));
      }

      @Override
      public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        tellAll(listeners, "onBatchRestored", listener -> listener.onBatchRestored(topicPartition, storeName, batchEndOffset, numRestored));
      }

      @Override
      public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        tellAll(listeners, "onRestoreEnd", listener -> listener.onRestoreEnd(topicPartition, storeName, totalRestored));
      }

      @Override
      public void onRestoreSuspended(TopicPartition topicPartition, String storeName, long totalRestored) {
        tellAll(listeners, "onRestoreSuspended", listener -> listener.onRestoreSuspended(topicPartition, storeName, totalRestored));
      }
    };
  }

  private <T> List<T> listenersOf(Function<EventifyPlugin, T> listener) {
    return plugins.stream().map(listener).filter(Objects::nonNull).toList();
  }

  private static <T> void tellAll(List<T> listeners, String hook, Consumer<T> call) {
    listeners.forEach(listener -> {
      try {
        call.accept(listener);
      } catch (Exception e) {
        log.warn("Plugin {} failed in {}", listener.getClass().getName(), hook, e);
      }
    });
  }
}
