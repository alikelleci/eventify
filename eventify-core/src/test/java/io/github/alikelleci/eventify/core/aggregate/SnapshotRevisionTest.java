package io.github.alikelleci.eventify.core.aggregate;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Revision;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.AccountHandler;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.Deposit;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.OpenAccount;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** A snapshot of another {@code @Revision} is not used; a count the events can't give (999) shows whether it was. */
@DisplayName("Snapshot revision")
class SnapshotRevisionTest {

  @Topic("commands.counter")
  public record Increment(@AggregateId String id) {
  }

  @Topic("events.counter")
  public record Incremented(@AggregateId String id) {
  }

  @AggregateRoot("counter")
  @Revision(2)
  @EnableSnapshotting(threshold = 2)
  public record Counter(@AggregateId String id, int count) {
  }

  public static class CounterHandler {
    @HandleCommand
    public Incremented handle(Increment command, Counter state) {
      return new Incremented(command.id());
    }

    @ApplyEvent
    public Counter apply(Incremented event, Counter state) {
      return new Counter(event.id(), state == null ? 1 : state.count() + 1);
    }
  }

  private final ObjectMapper objectMapper = EventifyObjectMapper.create();
  private TopologyTestDriver driver;

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("Should give a snapshot the @Revision of its aggregate class")
  void aSnapshotHasTheRevisionOfItsAggregate() {
    TestInputTopic<String, Command> commands = counters();
    KeyValueStore<String, AggregateState> snapshots = driver.getKeyValueStore("snapshot-store");

    increment(commands, 3); // the second command snapshots version 2

    assertThat(snapshots.get(StoreKeys.aggregate("counter", "c-1")).getRevision()).isEqualTo(2);
  }

  @Test
  @DisplayName("Should not use a snapshot made with another revision, and rebuild the aggregate from its events")
  void anOutdatedSnapshotIsNotUsed() {
    TestInputTopic<String, Command> commands = counters();
    KeyValueStore<String, AggregateState> snapshots = driver.getKeyValueStore("snapshot-store");
    increment(commands, 3);
    snapshots.put(StoreKeys.aggregate("counter", "c-1"), changed(snapshots.get(StoreKeys.aggregate("counter", "c-1")), 1, 999));

    increment(commands, 1); // rebuilds version 3 and snapshots the successful version 4

    AggregateState snapshot = snapshots.get(StoreKeys.aggregate("counter", "c-1"));
    assertThat(((Counter) snapshot.getPayload()).count()).isEqualTo(4); // from the events, not 999
    assertThat(snapshot.getRevision()).isEqualTo(2);
  }

  @Test
  @DisplayName("Should not use a snapshot of a removed aggregate made with another revision")
  void anOutdatedSnapshotOfARemovedAggregateIsNotUsed() {
    TestInputTopic<String, Command> commands = counters();
    KeyValueStore<String, AggregateState> snapshots = driver.getKeyValueStore("snapshot-store");
    increment(commands, 3);
    // A removal made with revision 1: no payload and no type.
    ObjectNode removed = objectMapper.valueToTree(snapshots.get(StoreKeys.aggregate("counter", "c-1")));
    removed.remove(List.of("payload", "type"));
    removed.put("revision", 1);
    snapshots.put(StoreKeys.aggregate("counter", "c-1"), objectMapper.convertValue(removed, AggregateState.class));

    increment(commands, 1); // rebuilds version 3 and snapshots the successful version 4

    AggregateState snapshot = snapshots.get(StoreKeys.aggregate("counter", "c-1"));
    assertThat(((Counter) snapshot.getPayload()).count()).isEqualTo(4); // from the events, not a new counter at 1
    assertThat(snapshot.getRevision()).isEqualTo(2);
  }

  @Test
  @DisplayName("Should use a snapshot made with the current revision")
  void aCurrentSnapshotIsUsed() {
    TestInputTopic<String, Command> commands = counters();
    KeyValueStore<String, AggregateState> snapshots = driver.getKeyValueStore("snapshot-store");
    increment(commands, 3);
    snapshots.put(StoreKeys.aggregate("counter", "c-1"), changed(snapshots.get(StoreKeys.aggregate("counter", "c-1")), 2, 999));

    increment(commands, 2); // the first command snapshots version 4

    assertThat(((Counter) snapshots.get(StoreKeys.aggregate("counter", "c-1")).getPayload()).count()).isEqualTo(1001);
  }

  @Test
  @DisplayName("Should fail the command when an outdated snapshot can't be rebuilt: the events before it were deleted")
  void anOutdatedSnapshotWithoutItsEventsFailsTheCommand() {
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(config()).registerHandler(new AccountHandler()).build().topology());
    TestInputTopic<String, Command> commands = driver.createInputTopic("commands.account", new StringSerializer(), new JsonSerializer<>());
    TestOutputTopic<String, CommandResult> results = driver.createOutputTopic("commands.account.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    KeyValueStore<String, AggregateState> snapshots = driver.getKeyValueStore("snapshot-store");
    send(commands, OpenAccount.builder().id("ada").build());
    send(commands, Deposit.builder().id("ada").amount(5).build());
    send(commands, Deposit.builder().id("ada").amount(7).build()); // loads the version 2 snapshot; earlier events were already deleted
    snapshots.put(StoreKeys.aggregate("account", "ada"), changed(snapshots.get(StoreKeys.aggregate("account", "ada")), 5, null));
    results.readValuesToList();

    send(commands, Deposit.builder().id("ada").amount(1).build());

    assertThat(results.readValue()).isInstanceOfSatisfying(CommandResult.Failure.class, failure ->
        assertThat(failure.cause()).contains("snapshot of aggregate account ada can't be used", "revision 5", "the events before it were deleted"));
  }

  private TestInputTopic<String, Command> counters() {
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(config()).registerHandler(new CounterHandler()).build().topology());
    return driver.createInputTopic("commands.counter", new StringSerializer(), new JsonSerializer<>());
  }

  private static void increment(TestInputTopic<String, Command> commands, int times) {
    for (int i = 0; i < times; i++) {
      send(commands, new Increment("c-1"));
    }
  }

  private static void send(TestInputTopic<String, Command> commands, Object payload) {
    Command command = Command.builder().payload(payload).build();
    commands.pipeInput(command.getAggregateId(), command);
  }

  /** The snapshot as another revision would have stored it, with a count the events can't give. */
  private AggregateState changed(AggregateState snapshot, int revision, Integer count) {
    ObjectNode json = objectMapper.valueToTree(snapshot);
    json.put("revision", revision);
    if (count != null) {
      ((ObjectNode) json.get("payload")).put("count", count);
    }
    return objectMapper.convertValue(json, AggregateState.class);
  }

  private static Properties config() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "snapshot-revision-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return properties;
  }
}
