package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.MetadataKeys;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import io.github.alikelleci.eventify.core.testdomain.account.Account;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.AccountHandler;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.Deposit;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.DepositEach;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.Deposited;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.OpenAccount;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyDescription;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** When snapshots are taken and events deleted, for an aggregate with a snapshot at every second event that deletes the events before it. */
@DisplayName("Snapshotting and event order")
class SnapshottingTest {

  private TopologyTestDriver driver;

  @AfterEach
  void tearDown() {
    if (driver != null) driver.close();
  }

  @Test
  @DisplayName("Should take a snapshot when a command with several events steps over the threshold")
  void aCommandWithSeveralEventsDoesNotStepOverTheSnapshot() {
    driver = new TopologyTestDriver(accounts());
    TestInputTopic<String, Command> commands = driver.createInputTopic("commands.account", new StringSerializer(), new JsonSerializer<>());
    KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
    KeyValueStore<String, AggregateState> snapshotStore = driver.getKeyValueStore("snapshot-store");

    send(commands, OpenAccount.builder().id("ada").build());                              // version 1
    send(commands, DepositEach.builder().id("ada").amounts(List.of(5, 7)).build());       // version 3: snapshot
    send(commands, Deposit.builder().id("ada").amount(1).build());                        // version 4: next snapshot

    AggregateState snapshot = snapshotStore.get(StoreKeys.aggregate("account", "ada"));
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.getVersion()).isEqualTo(4);
    assertThat(((Account) snapshot.getPayload()).getBalance()).isEqualTo(13);
    assertThat(eventStore.get(StoreKeys.event("account", "ada", snapshot.getVersion()))).isNotNull();
    assertThat(IteratorUtils.toList(eventStore.all())).hasSize(1); // the snapshot's event
  }

  /** Sequences have no gaps or repeats, across multi-event commands, rejections and pruning snapshots. */
  @Test
  @DisplayName("Should number the events of an aggregate without gaps, across rejected commands and deleted events")
  void sequencesContinueAcrossRejectionsAndDeletedEvents() {
    driver = new TopologyTestDriver(accounts());
    TestInputTopic<String, Command> commands = driver.createInputTopic("commands.account", new StringSerializer(), new JsonSerializer<>());
    TestOutputTopic<String, Event> events = driver.createOutputTopic("events.account", new StringDeserializer(), new JsonDeserializer<>(Event.class));
    KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");

    Command open = send(commands, OpenAccount.builder().id("ada").build());                     // 1
    Command both = send(commands, DepositEach.builder().id("ada").amounts(List.of(5, 7)).build()); // 2, 3: snapshot, 1 and 2 deleted
    send(commands, OpenAccount.builder().id("ada").build());                                      // rejected: no number
    Command one = send(commands, Deposit.builder().id("ada").amount(1).build());                   // 4
    Command more = send(commands, DepositEach.builder().id("ada").amounts(List.of(2, 3)).build()); // 5, 6

    List<Event> sent = events.readValuesToList();
    assertThat(sent)
        .extracting(event -> event.getAggregateType() + " " + event.getSequence() + " " + event.getMetadata().get(MetadataKeys.CAUSATION_ID))
        .containsExactly(
            "account 1 " + open.getId(),
            "account 2 " + both.getId(), "account 3 " + both.getId(),
            "account 4 " + one.getId(),
            "account 5 " + more.getId(), "account 6 " + more.getId());
    // Stored under the same number, as the same event: the last one, at the snapshot of version 6.
    assertThat(IteratorUtils.toList(eventStore.all()))
        .extracting(entry -> entry.key + " " + entry.value.getId())
        .containsExactly(StoreKeys.event("account", "ada", 6) + " " + sent.get(5).getId());
  }

  /** An event with an older timestamp is still stored after the earlier ones: order is by sequence, not time. */
  @Test
  @DisplayName("Should store events in the order they were handled, not by the command's timestamp")
  void anEventIsStoredInTheOrderItWasHandledNotByTheCommandsTimestamp() {
    driver = new TopologyTestDriver(accounts());
    TestInputTopic<String, Command> commands = driver.createInputTopic("commands.account", new StringSerializer(), new JsonSerializer<>());
    KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
    KeyValueStore<String, AggregateState> snapshotStore = driver.getKeyValueStore("snapshot-store");
    Instant now = Instant.now();

    send(commands, OpenAccount.builder().id("ada").build(), now);                          // version 1
    send(commands, Deposit.builder().id("ada").amount(5).build(), now.plusMillis(1));      // version 2
    send(commands, Deposit.builder().id("ada").amount(7).build(), now.plusMillis(2));      // version 3
    send(commands, Deposit.builder().id("ada").amount(100).build(), now.minusSeconds(60)); // version 4, older timestamp
    send(commands, Deposit.builder().id("ada").amount(1).build(), now.plusMillis(3));      // version 5
    send(commands, Deposit.builder().id("ada").amount(1).build(), now.plusMillis(4));      // version 6
    send(commands, Deposit.builder().id("ada").amount(1).build(), now.plusMillis(5));      // version 7, snapshot remains at 6

    AggregateState snapshot = snapshotStore.get(StoreKeys.aggregate("account", "ada"));
    assertThat(snapshot.getVersion()).isEqualTo(6);
    assertThat(((Account) snapshot.getPayload()).getBalance()).isEqualTo(114);
    assertThat(IteratorUtils.toList(eventStore.all()))
        .extracting(entry -> ((Deposited) entry.value.getPayload()).getAmount())
        .containsExactly(1, 1); // the snapshot's event and the last deposit
  }

  @Test
  @DisplayName("Should store the events of one command in the order the handler returned them")
  void theEventsOfOneCommandAreStoredInTheOrderTheHandlerReturnedThem() {
    driver = new TopologyTestDriver(accounts());
    TestInputTopic<String, Command> commands = driver.createInputTopic("commands.account", new StringSerializer(), new JsonSerializer<>());
    KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
    var emitted = driver.createOutputTopic("events.account", new StringDeserializer(), new JsonDeserializer<>(Event.class));
    Instant now = Instant.now();

    send(commands, OpenAccount.builder().id("ada").build(), now);
    send(commands, DepositEach.builder().id("ada").amounts(List.of(3, 2, 1)).build(), now.minusSeconds(60));

    // Pruning retains only the last stored event, so verify the full ordered sequence on the emitted events too.
    List<Event> recorded = emitted.readValuesToList();
    assertThat(recorded).extracting(Event::getSequence).containsExactly(1L, 2L, 3L, 4L);
    assertThat(recorded).extracting(event -> event.getPayload() instanceof Deposited deposited ? deposited.getAmount() : 0)
        .containsExactly(0, 3, 2, 1);

    assertThat(IteratorUtils.toList(eventStore.all()))
        .extracting(entry -> entry.value.getPayload() instanceof Deposited deposited ? deposited.getAmount() : 0)
        .containsExactly(1); // the command crossed a snapshot threshold, so older events were pruned
  }

  @Test
  @DisplayName("Should only store events from commands, not from the event topic")
  void anAggregateIsOnlyStoredFromItsCommandsNotFromItsEventTopic() {
    // Stored only when handled: storing them again from the event topic would bring back pruned events.
    assertThat(sourceTopics(accounts())).containsExactly("commands.account");
  }

  private static Command send(TestInputTopic<String, Command> commands, Object payload) {
    return send(commands, payload, Instant.now());
  }

  /** The timestamp is the record's: the clock of the host that sent the command. */
  private static Command send(TestInputTopic<String, Command> commands, Object payload, Instant timestamp) {
    Command command = Command.builder().payload(payload).build();
    commands.pipeInput(command.getAggregateId(), command, timestamp);
    return command;
  }

  private static List<String> sourceTopics(Topology topology) {
    return topology.describe().subtopologies().stream()
        .flatMap(subtopology -> subtopology.nodes().stream())
        .filter(node -> node instanceof TopologyDescription.Source)
        .flatMap(node -> ((TopologyDescription.Source) node).topicSet().stream())
        .toList();
  }

  private static Topology accounts() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "snapshotting-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return Eventify.builder().streamsConfig(properties).registerHandler(new AccountHandler()).build().topology();
  }
}
