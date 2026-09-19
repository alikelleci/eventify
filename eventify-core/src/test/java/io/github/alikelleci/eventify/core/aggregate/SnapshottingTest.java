package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import io.github.alikelleci.eventify.core.store.StoreKeys;
import io.github.alikelleci.eventify.core.testdomain.account.Account;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.AccountHandler;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.Deposit;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.DepositEach;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.Deposited;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.OpenAccount;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
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
    send(commands, DepositEach.builder().id("ada").amounts(List.of(5, 7)).build());       // version 3: past 2
    send(commands, Deposit.builder().id("ada").amount(1).build());                        // loads version 3: snapshot

    AggregateState snapshot = snapshotStore.get("ada");
    assertThat(snapshot).isNotNull();
    assertThat(snapshot.getVersion()).isEqualTo(3);
    assertThat(((Account) snapshot.getPayload()).getBalance()).isEqualTo(12);
    assertThat(eventStore.get(StoreKeys.of("ada", snapshot.getVersion()))).isNotNull();
    assertThat(IteratorUtils.toList(eventStore.all())).hasSize(2); // the snapshot's event and the last deposit
  }

  /**
   * A command handled after others, with an older timestamp (a producer's clock behind, a command sent late, or set
   * explicitly): its event is stored after theirs. Stored by its timestamp, it would come before the snapshot's event,
   * be skipped by every replay from the snapshot, and be deleted at the next one.
   */
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
    send(commands, Deposit.builder().id("ada").amount(7).build(), now.plusMillis(2));      // loads version 2: snapshot
    send(commands, Deposit.builder().id("ada").amount(100).build(), now.minusSeconds(60)); // version 4, older timestamp
    send(commands, Deposit.builder().id("ada").amount(1).build(), now.plusMillis(3));      // loads version 4: snapshot
    send(commands, Deposit.builder().id("ada").amount(1).build(), now.plusMillis(4));      // version 6
    send(commands, Deposit.builder().id("ada").amount(1).build(), now.plusMillis(5));      // loads version 6: snapshot

    AggregateState snapshot = snapshotStore.get("ada");
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
    Instant now = Instant.now();

    send(commands, OpenAccount.builder().id("ada").build(), now);
    send(commands, DepositEach.builder().id("ada").amounts(List.of(3, 2, 1)).build(), now.minusSeconds(60));

    assertThat(IteratorUtils.toList(eventStore.all()))
        .extracting(entry -> entry.value.getPayload() instanceof Deposited deposited ? deposited.getAmount() : 0)
        .containsExactly(0, 3, 2, 1);
  }

  @Test
  @DisplayName("Should only store events from commands, not from the event topic")
  void anAggregateIsOnlyStoredFromItsCommandsNotFromItsEventTopic() {
    // The events are stored when the command is handled. Stored again from the event topic, events deleted at a
    // snapshot in the meantime would come back.
    assertThat(sourceTopics(accounts())).containsExactly("commands.account");
  }

  private static void send(TestInputTopic<String, Command> commands, Object payload) {
    send(commands, payload, Instant.now());
  }

  /** The timestamp is the record's: the clock of the host that sent the command. */
  private static void send(TestInputTopic<String, Command> commands, Object payload, Instant timestamp) {
    Command command = Command.builder().payload(payload).build();
    commands.pipeInput(command.getAggregateId(), command, timestamp);
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
