package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * An event sourcing handler that returns {@code null} removes the aggregate: the commands after it get no state. The
 * {@code Probe} command answers whether its handler got a state.
 */
@DisplayName("Aggregate removal")
class AggregateRemovalTest {

  @Topic("commands.tab")
  public interface TabCommand {
  }

  public record Open(@AggregateId String id) implements TabCommand {
  }

  public record Close(@AggregateId String id) implements TabCommand {
  }

  public record Probe(@AggregateId String id) implements TabCommand {
  }

  @Topic("events.tab")
  public interface TabEvent {
  }

  public record Opened(@AggregateId String id) implements TabEvent {
  }

  public record Closed(@AggregateId String id) implements TabEvent {
  }

  public record Probed(@AggregateId String id, boolean hadState) implements TabEvent {
  }

  @AggregateRoot("tab")
  public record Tab(@AggregateId String id) {
  }

  public static class TabHandler {
    @HandleCommand
    public TabEvent handle(Open command, Tab state) {
      return new Opened(command.id());
    }

    @HandleCommand
    public TabEvent handle(Close command, Tab state) {
      return new Closed(command.id());
    }

    @HandleCommand
    public TabEvent handle(Probe command, Tab state) {
      return new Probed(command.id(), state != null);
    }

    @ApplyEvent
    public Tab apply(Opened event, Tab state) {
      return new Tab(event.id());
    }

    @ApplyEvent
    public Tab apply(Closed event, Tab state) {
      return null;
    }
  }

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, Event> events;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-removal-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(properties).registerHandler(new TabHandler()).build().topology());
    commands = driver.createInputTopic("commands.tab", new StringSerializer(), new CommandSerde().serializer());
    events = driver.createOutputTopic("events.tab", new StringDeserializer(), new EventSerde().deserializer());
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("Should give the next command no state once the aggregate is removed")
  void aRemovedAggregateHasNoState() {
    send(new Open("tab-1"));
    send(new Probe("tab-1"));
    send(new Close("tab-1"));
    send(new Probe("tab-1"));

    assertThat(events.readValuesToList())
        .extracting(event -> event.getPayload() instanceof Probed probed ? "probed, had state: " + probed.hadState() : event.getType())
        .containsExactly("Opened", "probed, had state: true", "Closed", "probed, had state: false");
  }

  private void send(Object payload) {
    Command command = Command.builder().payload(payload).build();
    commands.pipeInput(command.getAggregateId(), command);
  }
}
