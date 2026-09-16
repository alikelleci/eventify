package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.account.Account;
import io.github.alikelleci.eventify.core.account.AccountMessages.AccountHandler;
import io.github.alikelleci.eventify.core.account.AccountMessages.Deposit;
import io.github.alikelleci.eventify.core.account.AccountMessages.OpenAccount;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import org.apache.commons.collections4.IteratorUtils;
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

import java.util.List;
import java.util.Properties;

import static io.github.alikelleci.eventify.core.EventifyTest.buildPlaceOrderCommandFor;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * An aggregate id that is another aggregate id followed by "@", like "ada" and "ada@example.com". Events are stored
 * under aggregateId@ULID, so the keys of the second start with the keys' prefix of the first: each aggregate must
 * still only see, and delete, its own events.
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
    driver = new TopologyTestDriver(EventifyTest.baseBuilder().build().topology());
    TestInputTopic<String, Command> commands = EventifyTest.commandsTopic(driver);
    TestOutputTopic<String, Command> results = EventifyTest.commandResultsTopic(driver);

    send(commands, buildPlaceOrderCommandFor("ada@example.com"));
    send(commands, buildPlaceOrderCommandFor("ada"));

    // Placing "ada" fails with "Order already exists." when it loads the order of "ada@example.com".
    assertThat(results.readValuesToList())
        .extracting(result -> result.getAggregateId() + " " + result.getMetadata().get(Metadata.RESULT) + " " + result.getMetadata().get(Metadata.CAUSE))
        .containsExactly("ada@example.com success null", "ada success null");
  }

  @Test
  @DisplayName("Should only delete the aggregate's own events at a snapshot")
  void aSnapshotThatDeletesEventsOnlyDeletesTheAggregatesOwnEvents() {
    driver = new TopologyTestDriver(accounts());
    TestInputTopic<String, Command> commands = driver.createInputTopic("commands.account", new StringSerializer(), new JsonSerializer<>());
    TestOutputTopic<String, Command> results = driver.createOutputTopic("commands.account.results", new StringDeserializer(), new JsonDeserializer<>(Command.class));
    KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
    KeyValueStore<String, AggregateState> snapshotStore = driver.getKeyValueStore("snapshot-store");

    // "ada-team"-like ids with a character that sorts before every ULID ("-" before "0"): their keys come first.
    send(commands, Command.builder().payload(OpenAccount.builder().id("ada@-team").build()).build());
    send(commands, Command.builder().payload(Deposit.builder().id("ada@-team").amount(100).build()).build());

    // "ada": the third command loads two events, takes a snapshot and deletes the events before it.
    send(commands, Command.builder().payload(OpenAccount.builder().id("ada").build()).build());
    send(commands, Command.builder().payload(Deposit.builder().id("ada").amount(5).build()).build());
    send(commands, Command.builder().payload(Deposit.builder().id("ada").amount(7).build()).build());

    assertThat(results.readValuesToList())
        .extracting(result -> result.getAggregateId() + " " + result.getMetadata().get(Metadata.RESULT) + " " + result.getMetadata().get(Metadata.CAUSE))
        .containsExactly(
            "ada@-team success null", "ada@-team success null",
            "ada success null", "ada success null", "ada success null");

    AggregateState snapshot = snapshotStore.get("ada");
    assertThat(snapshot).isNotNull();
    assertThat(((Account) snapshot.getPayload()).getBalance()).isEqualTo(5);
    assertThat(snapshot.getVersion()).isEqualTo(2);

    // The other aggregate keeps all its events.
    assertThat(eventsOf(eventStore, "ada@-team")).hasSize(2);
    assertThat(eventsOf(eventStore, "ada")).hasSize(2); // its first event was deleted at the snapshot
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
