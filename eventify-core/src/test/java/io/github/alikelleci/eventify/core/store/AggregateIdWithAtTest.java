package io.github.alikelleci.eventify.core.store;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import io.github.alikelleci.eventify.core.support.Matchers;
import io.github.alikelleci.eventify.core.testdomain.account.Account;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.AccountHandler;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.Deposit;
import io.github.alikelleci.eventify.core.testdomain.account.AccountMessages.OpenAccount;
import org.apache.commons.collections4.IteratorUtils;
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

/**
 * Ids like "ada" and "ada@example.com": with "@" as separator their key ranges overlapped.
 * Each aggregate must only see and delete its own events.
 */
@DisplayName("Aggregate ids that start with another id and '@'")
class AggregateIdWithAtTest {

  private TopologyTestDriver driver;

  @AfterEach
  void tearDown() {
    if (driver != null) driver.close();
  }

  @Test
  @DisplayName("Should not load the events of an aggregate whose id starts with its id and '@'")
  void anAggregateDoesNotLoadTheEventsOfAnAggregateWhoseIdStartsWithItsIdAndAt() {
    driver = new TopologyTestDriver(accounts());
    TestInputTopic<String, Command> commands = driver.createInputTopic("commands.account", new StringSerializer(), new JsonSerializer<>());
    TestOutputTopic<String, CommandResult> results = driver.createOutputTopic("commands.account.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));

    send(commands, Command.builder().payload(OpenAccount.builder().id("ada@example.com").build()).build());
    send(commands, Command.builder().payload(OpenAccount.builder().id("ada@1").build()).build());
    send(commands, Command.builder().payload(OpenAccount.builder().id("ada").build()).build());

    // Opening "ada" fails with "Account already exists." when it loads the account of another aggregate.
    assertThat(results.readValuesToList())
        .extracting(result -> result.command().getAggregateId() + " " + Matchers.outcome(result) + " " + Matchers.causeOf(result))
        .containsExactly("ada@example.com success null", "ada@1 success null", "ada success null");
  }

  @Test
  @DisplayName("Should only delete the aggregate's own events at a snapshot")
  void aSnapshotThatDeletesEventsOnlyDeletesTheAggregatesOwnEvents() {
    driver = new TopologyTestDriver(accounts());
    TestInputTopic<String, Command> commands = driver.createInputTopic("commands.account", new StringSerializer(), new JsonSerializer<>());
    TestOutputTopic<String, CommandResult> results = driver.createOutputTopic("commands.account.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
    KeyValueStore<String, AggregateState> snapshotStore = driver.getKeyValueStore("snapshot-store");

    // An id whose keys sort right after the keys of "ada": next to the range that is deleted.
    String neighbour = "ada@1";
    send(commands, Command.builder().payload(OpenAccount.builder().id(neighbour).build()).build());
    send(commands, Command.builder().payload(Deposit.builder().id(neighbour).amount(100).build()).build());

    // "ada": the third command loads two events, takes a snapshot and deletes the events before it.
    send(commands, Command.builder().payload(OpenAccount.builder().id("ada").build()).build());
    send(commands, Command.builder().payload(Deposit.builder().id("ada").amount(5).build()).build());
    send(commands, Command.builder().payload(Deposit.builder().id("ada").amount(7).build()).build());

    assertThat(results.readValuesToList())
        .extracting(result -> result.command().getAggregateId() + " " + Matchers.outcome(result) + " " + Matchers.causeOf(result))
        .containsExactly(
            neighbour + " success null", neighbour + " success null",
            "ada success null", "ada success null", "ada success null");

    AggregateState snapshot = snapshotStore.get(StoreKeys.snapshot("account", "ada"));
    assertThat(snapshot).isNotNull();
    assertThat(((Account) snapshot.getPayload()).getBalance()).isEqualTo(5);
    assertThat(snapshot.getVersion()).isEqualTo(2);

    // The other aggregate keeps all its events.
    assertThat(StoreKeys.of("account", neighbour, 1)).isGreaterThan(StoreKeys.last("account", "ada"));
    assertThat(eventsOf(eventStore, neighbour)).hasSize(1); // its second event was snapshotted immediately
    assertThat(eventsOf(eventStore, "ada")).hasSize(2); // its first event was deleted at the snapshot
  }

  /** The separator is the one character an identifier cannot hold: the command is refused, the application goes on. */
  @Test
  @DisplayName("Should refuse an aggregate whose id contains the separator, and say so")
  void anIdThatContainsTheSeparatorIsRefused() {
    driver = new TopologyTestDriver(accounts());
    TestInputTopic<String, Command> commands = driver.createInputTopic("commands.account", new StringSerializer(), new JsonSerializer<>());
    TestOutputTopic<String, CommandResult> results = driver.createOutputTopic("commands.account.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));

    send(commands, Command.builder().payload(OpenAccount.builder().id("ada\u00001").build()).build());
    send(commands, Command.builder().payload(OpenAccount.builder().id("ada").build()).build());

    List<CommandResult> answers = results.readValuesToList();
    assertThat(Matchers.outcome(answers.get(0))).isEqualTo("failure");
    assertThat(Matchers.causeOf(answers.get(0))).contains("NUL");
    assertThat(Matchers.outcome(answers.get(1))).isEqualTo("success");
  }

  private static void send(TestInputTopic<String, Command> commands, Command command) {
    commands.pipeInput(command.getAggregateId(), command);
  }

  private static List<Event> eventsOf(KeyValueStore<String, Event> eventStore, String aggregateId) {
    return IteratorUtils.toList(eventStore.all()).stream()
        .map(keyValue -> keyValue.value)
        .filter(event -> event.getAggregateId().equals(aggregateId))
        .toList();
  }

  private static org.apache.kafka.streams.Topology accounts() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "account-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return Eventify.builder().streamsConfig(properties).registerHandler(new AccountHandler()).build().topology();
  }
}
