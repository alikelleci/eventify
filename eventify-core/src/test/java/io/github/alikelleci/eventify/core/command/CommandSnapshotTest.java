package io.github.alikelleci.eventify.core.command;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.internal.StoreKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import jakarta.validation.constraints.NotBlank;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** A snapshot is made only after an accepted command, of the state with its events applied. */
@DisplayName("Command snapshots")
class CommandSnapshotTest {
  @AggregateRoot("counter")
  @EnableSnapshotting(threshold = 2, deleteEvents = true)
  public static class Counter {
    @AggregateId public String id;
    public int value;
    public Counter() {}
    public Counter(String id, int value) { this.id = id; this.value = value; }
  }

  @Topic("commands.counter")
  public record Change(@AggregateId String id, @NotBlank String mode) {}
  @Topic("events.counter")
  public record Created(@AggregateId String id) {}
  @Topic("events.counter")
  public record Added(@AggregateId String id, int amount) {}
  @Topic("events.counter")
  public record Broken(@AggregateId String id) {}
  @Topic("events.counter")
  public record Removed(@AggregateId String id) {}

  public static class Handler {
    @CommandHandler
    public List<Object> handle(Change command, Counter state) {
      return switch (command.mode()) {
        case "command-failure" -> throw new IllegalStateException("command rejected");
        case "apply-failure" -> List.of(new Added(command.id(), 100), new Broken(command.id()));
        case "remove" -> List.of(new Removed(command.id()));
        case "no-events" -> List.of();
        default -> List.of(new Added(command.id(), 5));
      };
    }
    @EventSourcingHandler public Counter apply(Created event, Counter state) { return new Counter(event.id(), 1); }
    @EventSourcingHandler public Counter apply(Added event, Counter state) { return new Counter(event.id(), state.value + event.amount()); }
    @EventSourcingHandler public Counter apply(Broken event, Counter state) { throw new IllegalStateException("event rejected"); }
    @EventSourcingHandler public Counter apply(Removed event, Counter state) { return null; }
  }

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, CommandResult> results;
  private KeyValueStore<String, Event> events;
  private KeyValueStore<String, AggregateState> snapshots;

  @BeforeEach
  void setUp() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "command-snapshot-test");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(config).registerHandler(new Handler()).build().topology());
    commands = driver.createInputTopic("commands.counter", new StringSerializer(), new JsonSerializer<>());
    results = driver.createOutputTopic("commands.counter.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    events = driver.getKeyValueStore("event-store");
    snapshots = driver.getKeyValueStore("snapshot-store");
    // Existing history without a snapshot, as when snapshotting is enabled for an existing aggregate.
    store(new Created("one"), 1);
    store(new Added("one", 1), 2);
  }

  @AfterEach void close() { driver.close(); }

  @ParameterizedTest
  @ValueSource(strings = {"command-failure", "apply-failure", ""})
  @DisplayName("Should not snapshot when a command is rejected")
  void aRejectedCommandIsNotSnapshotted(String mode) {
    send(mode);
    assertThat(results.readValue()).isInstanceOf(CommandResult.Failure.class);
    assertThat(snapshot()).isNull();
    assertThat(events.get(key(1))).isNotNull();
    assertThat(events.get(key(2))).isNotNull();
    assertThat(events.get(key(3))).isNull();

    send("success"); // the next commands see nothing of the rejected one
    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
    send("success");
    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
    assertThat(((Counter) snapshot().getPayload()).value).isEqualTo(12);
    assertThat(snapshot().getVersion()).isEqualTo(4);
  }

  @Test
  @DisplayName("Should snapshot the state after an accepted command right away")
  void successSnapshotsTheResultWithoutWaitingForAnotherCommand() {
    send("success");
    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
    assertThat(snapshot().getVersion()).isEqualTo(3);
    assertThat(((Counter) snapshot().getPayload()).value).isEqualTo(7);
    assertThat(events.get(key(2))).isNull();
    assertThat(events.get(key(3))).isNotNull();
  }

  @Test
  @DisplayName("Should snapshot the history also for an accepted command without events")
  void acceptedCommandWithoutEventsStillCheckpointsHistory() {
    send("no-events");
    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
    assertThat(snapshot().getVersion()).isEqualTo(2);
    assertThat(((Counter) snapshot().getPayload()).value).isEqualTo(2);
  }

  @Test
  @DisplayName("Should snapshot a removed aggregate as a version without payload")
  void deletionSnapshotsVersionWithoutPayload() {
    send("remove");
    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
    assertThat(snapshot().getPayload()).isNull();
    assertThat(snapshot().getVersion()).isEqualTo(3);
    assertThat(snapshot().getAggregateId()).isEqualTo("one");
    assertThat(events.get(key(2))).isNull();
    assertThat(events.get(key(3))).isNotNull();
  }

  @AggregateRoot("unwritable")
  @EnableSnapshotting(threshold = 1)
  public static class Unwritable {
    @AggregateId public String id;
    public Unwritable() {}
    public Unwritable(String id) { this.id = id; }
    public String getBroken() { throw new IllegalStateException("cannot be written"); }
  }

  @Topic("commands.unwritable")
  public record Touch(@AggregateId String id) {}
  @Topic("events.unwritable")
  public record Touched(@AggregateId String id) {}

  public static class UnwritableHandler {
    @CommandHandler public Touched handle(Touch command, Unwritable state) { return new Touched(command.id()); }
    @EventSourcingHandler public Unwritable apply(Touched event, Unwritable state) { return new Unwritable(event.id()); }
  }

  @Test
  @DisplayName("Should skip the snapshot of a state that can't be written as JSON, and accept the command")
  void aStateThatCannotBeWrittenAsJsonIsNotSnapshotted() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "unwritable-snapshot-test");
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    try (TopologyTestDriver unwritable = new TopologyTestDriver(Eventify.builder().streamsConfig(config).registerHandler(new UnwritableHandler()).build().topology())) {
      TestInputTopic<String, Command> touches = unwritable.createInputTopic("commands.unwritable", new StringSerializer(), new JsonSerializer<>());
      TestOutputTopic<String, CommandResult> touched = unwritable.createOutputTopic("commands.unwritable.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));

      touches.pipeInput("one", Command.builder().payload(new Touch("one")).build());

      assertThat(touched.readValue()).isInstanceOf(CommandResult.Success.class);
      KeyValueStore<String, AggregateState> unwritableSnapshots = unwritable.getKeyValueStore("snapshot-store");
      assertThat(unwritableSnapshots.get(StoreKeys.aggregate("unwritable", "one"))).isNull();
    }
  }

  private void send(String mode) { commands.pipeInput("one", Command.builder().payload(new Change("one", mode)).build()); }
  private AggregateState snapshot() { return snapshots.get(StoreKeys.aggregate("counter", "one")); }
  private String key(long sequence) { return StoreKeys.event("counter", "one", sequence); }
  private void store(Object payload, long sequence) {
    events.put(key(sequence), Event.builder().aggregateType("counter").payload(payload).sequence(sequence).build());
  }
}
