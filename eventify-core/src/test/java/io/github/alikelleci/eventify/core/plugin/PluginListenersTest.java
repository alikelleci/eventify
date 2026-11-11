package io.github.alikelleci.eventify.core.plugin;

import io.github.alikelleci.eventify.core.plugin.internal.PluginListeners;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/** A plugin's exception is logged and goes no further: also when it fails to give its listener. */
@DisplayName("Plugin listeners")
class PluginListenersTest {

  private final List<String> told = new ArrayList<>();

  private final EventifyPlugin failing = new EventifyPlugin() {
    @Override
    public StateListener stateListener() {
      throw new IllegalStateException("no state listener");
    }

    @Override
    public StateRestoreListener stateRestoreListener() {
      throw new IllegalStateException("no restore listener");
    }
  };

  private final EventifyPlugin listening = new EventifyPlugin() {
    @Override
    public StateListener stateListener() {
      return (newState, oldState) -> told.add(oldState + " -> " + newState);
    }

    @Override
    public StateRestoreListener stateRestoreListener() {
      return new StateRestoreListener() {
        @Override
        public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
          told.add("restoring " + storeName);
        }

        @Override
        public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        }

        @Override
        public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        }
      };
    }
  };

  @Test
  @DisplayName("Should skip a plugin that fails to give its state listener, and still tell the others")
  void aPluginThatFailsToGiveItsStateListenerIsSkipped() {
    StateListener listener = new PluginListeners(List.of(failing, listening)).stateListener();

    listener.onChange(KafkaStreams.State.RUNNING, KafkaStreams.State.REBALANCING);

    assertThat(told).containsExactly("REBALANCING -> RUNNING");
  }

  @Test
  @DisplayName("Should skip a plugin that fails to give its restore listener, and still tell the others")
  void aPluginThatFailsToGiveItsRestoreListenerIsSkipped() {
    StateRestoreListener listener = new PluginListeners(List.of(failing, listening)).stateRestoreListener();

    listener.onRestoreStart(new TopicPartition("changelog", 0), "event-store", 0, 10);

    assertThat(told).containsExactly("restoring event-store");
  }
}
