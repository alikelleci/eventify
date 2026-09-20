package io.github.alikelleci.eventify.core.command;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.aggregate.annotation.EnableSnapshotting;
import io.github.alikelleci.eventify.core.command.annotation.HandleCommand;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import io.github.alikelleci.eventify.core.support.Matchers;
import lombok.Builder;
import lombok.Value;
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

import static org.assertj.core.api.Assertions.assertThat;

/** An aggregate is only ever loaded and stored under its own id: not under the record key of another, nor the id a handler returns. */
@DisplayName("Aggregate id mismatch")
class AggregateIdMismatchTest {

  @Value
  @Builder
  @AggregateRoot("counter")
  @EnableSnapshotting(threshold = 2)
  public static class Counter {
    @AggregateId
    String id;
    int value;
  }

  @Topic("commands.counter")
  public interface CounterCommand {
  }

  @Topic("events.counter")
  public interface CounterEvent {
  }

  @Value
  @Builder
  public static class Increment implements CounterCommand {
    @AggregateId
    String id;
    /** The id the event sourcing handler gives the state: a bug in the handler when it isn't {@link #id}. */
    String stateId;
  }

  @Value
  @Builder
  public static class Incremented implements CounterEvent {
    @AggregateId
    String id;
    String stateId;
  }

  public static class CounterHandler {
    final List<String> handled = new java.util.ArrayList<>();

    @HandleCommand
    public CounterEvent handle(Increment command, Counter state) {
      handled.add(command.getId());
      return Incremented.builder().id(command.getId()).stateId(command.getStateId()).build();
    }

    @ApplyEvent
    public Counter apply(Incremented event, Counter state) {
      return Counter.builder().id(event.getStateId()).value(state != null ? state.getValue() + 1 : 1).build();
    }
  }

  private TopologyTestDriver driver;
  private CounterHandler handler;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, CommandResult> results;
  private KeyValueStore<String, Event> eventStore;
  private KeyValueStore<String, AggregateState> snapshotStore;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-id-mismatch-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    handler = new CounterHandler();
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(properties).registerHandler(handler).build().topology());
    commands = driver.createInputTopic("commands.counter", new StringSerializer(), new JsonSerializer<>());
    results = driver.createOutputTopic("commands.counter.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    eventStore = driver.getKeyValueStore("event-store");
    snapshotStore = driver.getKeyValueStore("snapshot-store");
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("Should reject a state with another aggregate id, and never store it as that aggregate's snapshot")
  void aStateWithAnotherAggregateIdIsNotStoredAsThatAggregatesSnapshot() {
    send("ad", Increment.builder().id("ad").stateId("ad").build());
    send("ada", Increment.builder().id("ada").stateId("ada").build());
    send("ada", Increment.builder().id("ada").stateId("ad").build()); // the handler gives "ada" the id "ad": rejected, not stored
    send("ada", Increment.builder().id("ada").stateId("ada").build()); // "ada" is still at version 1, and goes on

    assertThat(results.readValuesToList())
        .extracting(result -> result.command().getAggregateId() + " " + Matchers.outcome(result))
        .containsExactly("ad success", "ada success", "ada failure", "ada success");
    assertThat(IteratorUtils.toList(eventStore.all())).hasSize(3);
    assertThat(snapshotStore.get(StoreKeys.snapshot("counter", "ad"))).isNull();
    assertThat(((Counter) snapshotStore.get(StoreKeys.snapshot("counter", "ada")).getPayload()).getValue()).isEqualTo(2);

    // "ad" is untouched: its next command loads its own state.
    send("ad", Increment.builder().id("ad").stateId("ad").build());
    assertThat(Matchers.outcome(results.readValue())).isEqualTo("success");
  }

  @Test
  @DisplayName("Should not handle a command whose record key is another aggregate id")
  void aCommandUnderAnotherRecordKeyIsNotHandled() {
    send("ada", Increment.builder().id("ada").stateId("ada").build());
    send("ada", Increment.builder().id("bob").stateId("bob").build()); // record key "ada", command for "bob"

    assertThat(results.readValuesToList())
        .extracting(result -> result.command().getAggregateId() + " " + Matchers.outcome(result) + " " + Matchers.causeOf(result))
        .containsExactly(
            "ada success null",
            "bob failure AggregateIdMismatchException: Aggregate identifier does not match for command Increment. Expected bob, but was ada");
    assertThat(handler.handled).containsExactly("ada");
    assertThat(IteratorUtils.toList(eventStore.all())).hasSize(1);
  }

  private void send(String key, Object payload) {
    commands.pipeInput(key, Command.builder().payload(payload).build());
  }
}
