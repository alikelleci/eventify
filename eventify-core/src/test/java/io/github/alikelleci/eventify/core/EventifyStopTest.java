package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.plugin.EventifyPlugin;
import io.github.alikelleci.eventify.core.plugin.PluginContext;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("Stopping Eventify")
class EventifyStopTest {

  /** Only there so Eventify has a topology to start. */
  @Topic("commands.ping")
  public record Ping(@AggregateId String id) {
  }

  @AggregateRoot("pinged")
  public record Pinged(@AggregateId String id) {
  }

  public static class PingHandler {
    @HandleCommand
    public Object handle(Ping command, Pinged state) {
      return null;
    }
  }

  @TempDir
  Path stateDir;

  @Test
  @DisplayName("Should stop plugins once, also when Kafka Streams stopped by itself")
  void pluginsAreStoppedOnceAlsoWhenKafkaStreamsStoppedByItself() {
    AtomicInteger stops = new AtomicInteger();
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stop-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

    Eventify eventify = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new PingHandler())
        .registerPlugin(new EventifyPlugin() {
          @Override
          public void onStop(PluginContext context) {
            stops.incrementAndGet();
          }
        })
        .build();
    eventify.start();

    // Kafka Streams is no longer running, like after an ERROR: Eventify still stops, and the plugins with it.
    eventify.getKafkaStreams().close();
    eventify.stop();
    eventify.stop();

    assertThat(stops).hasValue(1);
  }

  @Test
  @DisplayName("Should still stop the other plugins when one fails to stop")
  void aPluginThatFailsToStopDoesNotKeepTheOthersRunning() {
    AtomicInteger stops = new AtomicInteger();
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stop-failing-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

    Eventify eventify = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new PingHandler())
        .registerPlugin(new EventifyPlugin() {
          @Override
          public void onStop(PluginContext context) {
            throw new IllegalStateException("fails");
          }
        })
        .registerPlugin(new EventifyPlugin() {
          @Override
          public void onStop(PluginContext context) {
            stops.incrementAndGet();
          }
        })
        .build();
    eventify.start();
    eventify.stop();

    assertThat(stops).hasValue(1);
  }

  /** E.g. Spring closing its context while the JVM's shutdown hook stops Eventify too. */
  @Test
  @DisplayName("Should not return from stop while another thread is still stopping")
  void stopWaitsForAStopInProgress() throws Exception {
    CountDownLatch stopping = new CountDownLatch(1);
    AtomicBoolean firstStopDone = new AtomicBoolean();
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "stop-concurrent-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:1");
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.toString());

    Eventify eventify = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new PingHandler())
        .registerPlugin(new EventifyPlugin() {
          @Override
          public void onStop(PluginContext context) {
            stopping.countDown();
            try {
              Thread.sleep(500);
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            firstStopDone.set(true);
          }
        })
        .build();
    eventify.start();

    Thread first = new Thread(eventify::stop);
    first.start();
    assertThat(stopping.await(30, TimeUnit.SECONDS)).isTrue();

    eventify.stop();

    assertThat(firstStopDone).isTrue();
    first.join();
  }
}
