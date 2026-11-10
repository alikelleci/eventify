package io.github.alikelleci.eventify.core.command;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import io.github.alikelleci.eventify.core.support.Matchers;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
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

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * A command is rejected before anything of it is stored or sent, also for a mistake in an event.
 * What fails after it is accepted is not the command's failure.
 */
@DisplayName("Command rejection")
class CommandRejectionTest {

  @Value
  @Builder(toBuilder = true)
  @AggregateRoot("counter")
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

  /** Its event has a topic name Kafka refuses. */
  @Value
  @Builder
  public static class Misname implements CounterCommand {
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

  /** Its event loses a field when written as JSON. */
  @Value
  @Builder
  public static class Lose implements CounterCommand {
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

  /** Its reason has no getter, so it is not written as JSON: stored, the event has no reason. */
  @Value
  @Builder
  public static class Lost implements CounterEvent {
    @AggregateId
    String id;
    @Getter(AccessLevel.NONE)
    String reason;

    public String reason() {
      return reason;
    }
  }

  @Value
  @Builder
  public static class Misplaced {
    @AggregateId
    String id;
  }

  @Value
  @Builder
  @Topic("events counter!")
  public static class Misnamed {
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
      // Written three times before the command is accepted: twice for its JSON copy, once for its size.
      if (WRITES.incrementAndGet() > 3) {
        throw new IllegalStateException("not writable anymore");
      }
      return "written";
    }
  }

  public static class CounterHandler {
    @CommandHandler
    public Object handle(Create command, Counter state) {
      return Created.builder().id(command.getId()).build();
    }

    @CommandHandler
    public Object handle(Break command, Counter state) {
      return Broken.builder().id(command.getId()).build();
    }

    @CommandHandler
    public Object handle(Lose command, Counter state) {
      return Lost.builder().id(command.getId()).reason("gone").build();
    }

    @CommandHandler
    public Object handle(Misplace command, Counter state) {
      return Misplaced.builder().id(command.getId()).build();
    }

    @CommandHandler
    public Object handle(Misname command, Counter state) {
      return Misnamed.builder().id(command.getId()).build();
    }

    @CommandHandler
    public Object handle(Corrupt command, Counter state) {
      return Corrupted.builder().id(command.getId()).build();
    }

    @CommandHandler
    public Object handle(Flake command, Counter state) {
      return Flaked.builder().id(command.getId()).build();
    }

    @EventSourcingHandler
    public Counter handle(Created event, Counter state) {
      return Counter.builder().id(event.getId()).value(state != null ? state.getValue() + 1 : 1).build();
    }

    @EventSourcingHandler
    public Counter handle(Broken event, Counter state) {
      throw new IllegalStateException("cannot apply");
    }

    @EventSourcingHandler
    public Counter handle(Lost event, Counter state) {
      if (event.reason() == null) {
        throw new IllegalStateException("no reason");
      }
      return state;
    }

    @EventSourcingHandler
    public Counter handle(Misplaced event, Counter state) {
      return state;
    }

    @EventSourcingHandler
    public Counter handle(Corrupted event, Counter state) {
      return state;
    }

    @EventSourcingHandler
    public Counter handle(Flaked event, Counter state) {
      return state;
    }
  }

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, CommandResult> results;
  private TestOutputTopic<String, Event> events;
  private KeyValueStore<String, Event> eventStore;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "command-rejection-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(properties).registerHandler(new CounterHandler()).build().topology());
    commands = driver.createInputTopic("commands.counter", new StringSerializer(), new JsonSerializer<>());
    results = driver.createOutputTopic("commands.counter.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    events = driver.createOutputTopic("events.counter", new StringDeserializer(), new JsonDeserializer<>(Event.class));
    eventStore = driver.getKeyValueStore("event-store");
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  /** Stored, the event would be replayed at every load, and every next command of the aggregate would fail. */
  @Test
  @DisplayName("Should fail the command and store nothing when an event cannot be applied")
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

  /** Every load replays the stored JSON, not the returned object, so that is what must apply. */
  @Test
  @DisplayName("Should fail the command and store nothing when an event cannot be applied as it is stored")
  void anEventThatCannotBeAppliedAsStoredIsNotStored() {
    send(Create.builder().id("ada").build());
    send(Lose.builder().id("ada").build());
    send(Create.builder().id("ada").build());

    assertThat(results()).containsExactly(
        "Create success null",
        "Lose failure IllegalStateException: no reason",
        "Create success null");
    assertThat(storedTypes()).containsExactly("Created", "Created");
  }

  @Test
  @DisplayName("Should fail the command and store nothing when an event has no @Topic")
  void anEventWithoutATopicIsNotStored() {
    send(Create.builder().id("ada").build());
    send(Misplace.builder().id("ada").build());

    assertThat(results()).containsExactly(
        "Create success null",
        "Misplace failure TopicMissingException: Event Misplaced has no @Topic.");
    assertThat(storedTypes()).containsExactly("Created");
    assertThat(events.readValuesToList()).extracting(Event::getType).containsExactly("Created");
  }

  @Test
  @DisplayName("Should fail the command and store nothing when an event has an invalid @Topic")
  void anEventWithAnInvalidTopicIsNotStored() {
    send(Create.builder().id("ada").build());
    send(Misname.builder().id("ada").build());

    assertThat(results()).element(1).asString()
        .startsWith("Misname failure InvalidTopicException: Topic name is invalid: 'events counter!'");
    assertThat(storedTypes()).containsExactly("Created");
    assertThat(events.readValuesToList()).extracting(Event::getType).containsExactly("Created");
  }

  /** Found only when stored, it would stop the application, and every instance the command moves to after it. */
  @Test
  @DisplayName("Should fail the command and store nothing when an event cannot be written as JSON")
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

  /** Found only when sent, it would stop the application, again after every restart. */
  @Test
  @DisplayName("Should fail the command and store nothing when its events are larger than max.request.size")
  void eventsTooLargeForKafkaAreNotStored() {
    restartWithMaxRequestSize(3600);

    // The failure, with only the command, fits; the success, with the event too, doesn't.
    send(Create.builder().id("a".repeat(600)).build());

    assertThat(results()).singleElement().asString()
        .startsWith("Create failure RecordTooLargeException: The result of command Create is ")
        .endsWith(" bytes, more than Kafka accepts: max.request.size is 3600.");
    assertThat(storedTypes()).isEmpty();
  }

  /** Its failure holds the command, so that can't be sent either: sending it would stop the application. */
  @Test
  @DisplayName("Should skip a command that is itself larger than max.request.size, and handle the next one")
  void aCommandTooLargeForKafkaGetsNoResult() {
    restartWithMaxRequestSize(3000);

    send(Create.builder().id("a".repeat(3000)).build());
    send(Create.builder().id("ada").build());

    assertThat(results()).containsExactly("Create success null");
    assertThat(storedTypes()).containsExactly("Created");
  }

  private void restartWithMaxRequestSize(int maxRequestSize) {
    driver.close();
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "command-rejection-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    properties.put(StreamsConfig.producerPrefix(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), maxRequestSize);
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(properties).registerHandler(new CounterHandler()).build().topology(), properties);
    commands = driver.createInputTopic("commands.counter", new StringSerializer(), new JsonSerializer<>());
    results = driver.createOutputTopic("commands.counter.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    eventStore = driver.getKeyValueStore("event-store");
  }

  /** Not the command's failure: it fails the task, so exactly-once aborts what was written for the command. */
  @Test
  @DisplayName("Should not report a failure after the command was accepted as the command's failure")
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
        .map(result -> result.command().getType() + " " + Matchers.outcome(result) + " " + Matchers.causeOf(result))
        .toList();
  }

  private java.util.List<String> storedTypes() {
    return IteratorUtils.toList(eventStore.all()).stream().map(entry -> entry.value.getType()).toList();
  }
}
