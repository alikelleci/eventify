package io.github.alikelleci.eventify.core.plugin.internal;

import io.github.alikelleci.eventify.core.plugin.EventifyPlugin;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/** Fans each Kafka Streams listener out to the plugins; one that throws is logged and skipped. */
@Slf4j
public class PluginListeners {

  private final List<EventifyPlugin> plugins;

  public PluginListeners(List<EventifyPlugin> plugins) {
    this.plugins = List.copyOf(plugins);
  }

  /** Tells every plugin, e.g. {@code plugin -> plugin.onStart(context)}. */
  public void notifyPlugins(String hook, Consumer<EventifyPlugin> call) {
    tellAll(plugins, hook, call);
  }

  public StateListener stateListener() {
    List<StateListener> listeners = listenersOf("stateListener", EventifyPlugin::stateListener);
    return (newState, oldState) ->
        tellAll(listeners, "onChange", listener -> listener.onChange(newState, oldState));
  }

  public StateRestoreListener stateRestoreListener() {
    List<StateRestoreListener> listeners = listenersOf("stateRestoreListener", EventifyPlugin::stateRestoreListener);
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

  /** A plugin that fails to give its listener is logged and skipped, like one whose callback fails. */
  private <T> List<T> listenersOf(String hook, Function<EventifyPlugin, T> listener) {
    List<T> listeners = new ArrayList<>();
    for (EventifyPlugin plugin : plugins) {
      try {
        T given = listener.apply(plugin);
        if (given != null) {
          listeners.add(given);
        }
      } catch (Exception e) {
        log.warn("Plugin callback failed: targetClass={}, callback={}", plugin.getClass().getName(), hook, e);
      }
    }
    return listeners;
  }

  private static <T> void tellAll(List<T> listeners, String hook, Consumer<T> call) {
    listeners.forEach(listener -> {
      try {
        call.accept(listener);
      } catch (Exception e) {
        log.warn("Plugin callback failed: targetClass={}, callback={}", listener.getClass().getName(), hook, e);
      }
    });
  }
}
