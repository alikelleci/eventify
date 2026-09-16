package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.AggregateRoot;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
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
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A command is rejected before any of its events is stored, also when the mistake is in an event: nothing of it is
 * stored or sent, and the aggregate goes on as it was. What fails after the command is accepted is not its failure.
 */
class CommandRejectionTest {

  @Value
  @Builder(toBuilder = true)
  @AggregateRoot
  public static class Counter {
    @AggregateId
    String id;
    int value;
  }

  @TopicInfo("commands.counter")
  public interface CounterCommand {
  }

  @TopicInfo("events.counter")
  public interface CounterEvent {
  }

  @Value
  @Builder
  public static class Create implements CounterCommand {
    @AggregateId
    String id;
  }

  /** Its event can't be applied. */
  @Value
  @Builder
  public static class Break implements CounterCommand {
    @AggregateId
    String id;
  }

  /** Its event has no topic. */
  @Value
  @Builder
  public static class Misplace implements CounterCommand {
    @AggregateId
    String id;
  }

  /** Its event can't be written as JSON. */
  @Value
  @Builder
  public static class Corrupt implements CounterCommand {
    @AggregateId
    String id;
  }

  /** Its event can be written as JSON once, and not after that. */
  @Value
  @Builder
  public static class Flake implements CounterCommand {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class Created implements CounterEvent {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class Broken implements CounterEvent {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class Misplaced {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  public static class Corrupted implements CounterEvent {
    @AggregateId
    String id;

    public String getContent() {
      throw new IllegalStateException("not writable");
    }
  }

  @Value
  @Builder
  public static class Flaked implements CounterEvent {
    static final AtomicInteger WRITES = new AtomicInteger();

    @AggregateId
    String id;

    public String getContent() {
      if (WRITES.incrementAndGet() > 1) {
        throw new IllegalStateException("not writable anymore");
      }
      return "written";
    }
  }

  public static class CounterHandler {
    @HandleCommand
    public Object handle(Create command, Counter state) {
      return Created.builder().id(command.getId()).build();
    }

    @HandleCommand
    public Object handle(Break command, Counter state) {
      return Broken.builder().id(command.getId()).build();
    }

    @HandleCommand
    public Object handle(Misplace command, Counter state) {
      return Misplaced.builder().id(command.getId()).build();
    }

    @HandleCommand
    public Object handle(Corrupt command, Counter state) {
      return Corrupted.builder().id(command.getId()).build();
    }

    @HandleCommand
    public Object handle(Flake command, Counter state) {
      return Flaked.builder().id(command.getId()).build();
    }

    @ApplyEvent
    public Counter apply(Created event, Counter state) {
      return Counter.builder().id(event.getId()).value(state != null ? state.getValue() + 1 : 1).build();
    }

    @ApplyEvent
    public Counter apply(Broken event, Counter state) {
      throw new IllegalStateException("cannot apply");
    }

    @ApplyEvent
    public Counter apply(Misplaced event, Counter state) {
      return state;
    }

    @ApplyEvent
    public Counter apply(Corrupted event, Counter state) {
      return state;
    }

    @ApplyEvent
    public Counter apply(Flaked event, Counter state) {
      return state;
    }
  }

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, Command> results;
  private TestOutputTopic<String, Event> events;
  private KeyValueStore<String, Event> eventStore;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "command-rejection-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(properties).registerHandler(new CounterHandler()).build().topology());
    commands = driver.createInputTopic("commands.counter", new StringSerializer(), new JsonSerializer<>());
    results = driver.createOutputTopic("commands.counter.results", new StringDeserializer(), new JsonDeserializer<>(Command.class));
    events = driver.createOutputTopic("events.counter", new StringDeserializer(), new JsonDeserializer<>(Event.class));
    eventStore = driver.getKeyValueStore("event-store");
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  /** Stored, the event would be replayed at every load, and every next command of the aggregate would fail. */
  @Test
  void anEventItsHandlerCannotApplyIsNotStored() {
    send(Create.builder().id("ada").build());
    send(Break.builder().id("ada").build());
    send(Create.builder().id("ada").build());

    assertThat(results()).containsExactly(
        "Create success null",
        "Break failure IllegalStateException: cannot apply",
        "Create success null");
    assertThat(storedTypes()).containsExactly("Created", "Created");
    assertThat(events.readValuesToList()).extracting(Event::getType).containsExactly("Created", "Created");
  }

  /** Its topic was only looked up when sending it: the event was stored, and the command reported both as done and as failed. */
  @Test
  void anEventWithoutATopicIsNotStored() {
    send(Create.builder().id("ada").build());
    send(Misplace.builder().id("ada").build());

    assertThat(results()).containsExactly(
        "Create success null",
        "Misplace failure TopicInfoMissingException: Event Misplaced has no topic. Please annotate its class, or an interface it implements, with @TopicInfo.");
    assertThat(storedTypes()).containsExactly("Created");
    assertThat(events.readValuesToList()).extracting(Event::getType).containsExactly("Created");
  }

  /** Found only when stored, it would stop the application, and every instance the command moves to after it. */
  @Test
  void anEventThatCannotBeWrittenAsJsonIsNotStored() {
    send(Create.builder().id("ada").build());
    send(Corrupt.builder().id("ada").build());
    send(Create.builder().id("ada").build());

    assertThat(results()).containsExactly(
        "Create success null",
        "Corrupt failure IllegalStateException: not writable",
        "Create success null");
    assertThat(storedTypes()).containsExactly("Created", "Created");
  }

  /** Not the command's failure: it fails the task, so exactly-once aborts what was written for the command. */
  @Test
  void aFailureAfterTheCommandIsAcceptedIsNotReportedAsItsFailure() {
    send(Create.builder().id("ada").build());
    results.readValuesToList();
    Flaked.WRITES.set(0);

    assertThatThrownBy(() -> send(Flake.builder().id("ada").build())).hasStackTraceContaining("not writable anymore");
    assertThat(results.isEmpty()).isTrue();
  }

  private void send(Object payload) {
    Command command = Command.builder().payload(payload).build();
    commands.pipeInput(command.getAggregateId(), command);
  }

  private java.util.List<String> results() {
    return results.readValuesToList().stream()
        .map(result -> result.getType() + " " + result.getMetadata().get(Metadata.RESULT) + " " + result.getMetadata().get(Metadata.CAUSE))
        .toList();
  }

  private java.util.List<String> storedTypes() {
    return IteratorUtils.toList(eventStore.all()).stream().map(entry -> entry.value.getType()).toList();
  }
}
