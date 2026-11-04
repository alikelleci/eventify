package io.github.alikelleci.eventify.core.command;

import com.fasterxml.jackson.annotation.JsonIgnore;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.command.internal.HeaderNames;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** A scheduled command waits in its own store and is handled like any other command once it is due. */
@DisplayName("Scheduled commands")
class ScheduledCommandTest {

  private static final Instant START = Instant.parse("2026-11-04T12:00:00Z");

  @Value
  @Builder(toBuilder = true)
  @AggregateRoot("counter")
  public static class Counter {
    @AggregateId
    String id;
  }

  @Topic("commands.counter")
  public interface CounterCommand {
  }

  @Topic("events.counter")
  public interface CounterEvent {
  }

  @Value
  @Builder
  public static class Tick implements CounterCommand {
    @AggregateId
    String id;
  }

  /** No handler in this application. */
  @Value
  @Builder
  public static class Reset implements CounterCommand {
    @AggregateId
    String id;
  }

  /** Can be written as JSON once: read back, it has no note. */
  @Value
  @Builder
  public static class Annotate implements CounterCommand {
    @AggregateId
    String id;
    @JsonIgnore
    String note;

    public int getNoteLength() {
      return note.length();
    }
  }

  @Value
  @Builder
  public static class Ticked implements CounterEvent {
    @AggregateId
    String id;
  }

  public static class CounterHandler {
    @CommandHandler
    public Object handle(Tick command, Counter state) {
      return Ticked.builder().id(command.getId()).build();
    }

    @CommandHandler
    public Object handle(Annotate command, Counter state) {
      return Ticked.builder().id(command.getId()).build();
    }

    @EventSourcingHandler
    public Counter handle(Ticked event, Counter state) {
      return Counter.builder().id(event.getId()).build();
    }
  }

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> scheduled;
  private TestOutputTopic<String, Command> commands;
  private TestOutputTopic<String, CommandResult> results;
  private TestOutputTopic<String, Event> events;
  private KeyValueStore<String, Command> store;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "scheduled-command-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(properties).registerHandler(new CounterHandler()).build().topology(), properties, START);
    scheduled = driver.createInputTopic("commands.counter.scheduled", new StringSerializer(), new JsonSerializer<>());
    commands = driver.createOutputTopic("commands.counter", new StringDeserializer(), new JsonDeserializer<>(Command.class));
    results = driver.createOutputTopic("commands.counter.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    events = driver.createOutputTopic("events.counter", new StringDeserializer(), new JsonDeserializer<>(Event.class));
    store = driver.getKeyValueStore("scheduled-commands");
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("Should deliver a scheduled command once at its time, not earlier, and then handle it")
  void deliversAtItsTime() {
    Command command = schedule(Tick.builder().id("ada").build(), START.plusSeconds(60));

    driver.advanceWallClockTime(Duration.ofSeconds(59));
    assertThat(commands.readRecordsToList()).as("delivered before its time").isEmpty();
    assertThat(stored()).hasSize(1);

    driver.advanceWallClockTime(Duration.ofSeconds(1));
    List<TestRecord<String, Command>> delivered = commands.readRecordsToList();
    assertThat(delivered).hasSize(1);
    assertThat(delivered.get(0).key()).isEqualTo("ada");
    assertThat(delivered.get(0).value().getId()).isEqualTo(command.getId());
    assertThat(delivered.get(0).timestamp()).as("Kafka timestamp = delivery").isEqualTo(START.plusSeconds(60).toEpochMilli());
    assertThat(delivered.get(0).value().getTimestamp()).as("command keeps its own timestamp").isEqualTo(command.getTimestamp());
    assertThat(delivered.get(0).headers().lastHeader(HeaderNames.REPLY_TO)).as("nobody waits for it").isNull();
    assertThat(stored()).isEmpty();

    assertThat(results.readValuesToList()).extracting(result -> result.command().getId()).containsExactly(command.getId());
    assertThat(events.readValuesToList()).extracting(Event::getType).containsExactly("Ticked");

    driver.advanceWallClockTime(Duration.ofMinutes(5));
    assertThat(commands.readRecordsToList()).as("delivered again").isEmpty();
  }

  @Test
  @DisplayName("Should deliver scheduled commands in the order of their time")
  void deliversInTimeOrder() {
    Command later = schedule(Tick.builder().id("ada").build(), START.plusSeconds(20));
    Command sooner = schedule(Tick.builder().id("bob").build(), START.plusSeconds(10));

    driver.advanceWallClockTime(Duration.ofSeconds(30));

    assertThat(commands.readValuesToList()).extracting(Command::getId).containsExactly(sooner.getId(), later.getId());
  }

  @Test
  @DisplayName("Should deliver a command scheduled in the past at the next delivery round")
  void deliversAPastTimeRightAway() {
    Command command = schedule(Tick.builder().id("ada").build(), START.minusSeconds(60));

    driver.advanceWallClockTime(Duration.ofSeconds(1));

    assertThat(commands.readValuesToList()).extracting(Command::getId).containsExactly(command.getId());
  }

  @Test
  @DisplayName("Should deliver a command scheduled twice for the same time once")
  void deliversADuplicateOnce() {
    Command command = Command.builder().payload(Tick.builder().id("ada").build()).build();
    pipe(command, header(START.plusSeconds(10)));
    pipe(command, header(START.plusSeconds(10)));

    driver.advanceWallClockTime(Duration.ofSeconds(10));

    assertThat(commands.readValuesToList()).extracting(Command::getId).containsExactly(command.getId());
  }

  @Test
  @DisplayName("Should skip a scheduled command without a valid delivery time")
  void skipsWithoutAValidTime() {
    pipe(Command.builder().payload(Tick.builder().id("ada").build()).build(), new RecordHeaders());
    pipe(Command.builder().payload(Tick.builder().id("bob").build()).build(),
        new RecordHeaders().add(new RecordHeader(HeaderNames.DELIVER_AT, "soon".getBytes(StandardCharsets.UTF_8))));
    pipe(Command.builder().payload(Tick.builder().id("cyd").build()).build(),
        new RecordHeaders().add(new RecordHeader(HeaderNames.DELIVER_AT, "-1".getBytes(StandardCharsets.UTF_8))));

    driver.advanceWallClockTime(Duration.ofMinutes(1));

    assertThat(stored()).isEmpty();
    assertThat(commands.readValuesToList()).isEmpty();
  }

  /** Another application with handlers on the same topic delivers it; delivered here too, it would be handled twice. */
  @Test
  @DisplayName("Should skip a scheduled command this application has no handler for")
  void skipsACommandWithoutAHandler() {
    schedule(Reset.builder().id("ada").build(), START.plusSeconds(10));

    driver.advanceWallClockTime(Duration.ofSeconds(10));

    assertThat(stored()).isEmpty();
    assertThat(commands.readValuesToList()).isEmpty();
  }

  /** Stored, it could not be written to the store: Eventify would stop. */
  @Test
  @DisplayName("Should skip a scheduled command that can't be written as JSON, and deliver the next one")
  void skipsACommandThatCantBeWrittenAgain() {
    schedule(Annotate.builder().id("ada").note("not in the JSON").build(), START.plusSeconds(10));
    Command next = schedule(Tick.builder().id("bob").build(), START.plusSeconds(10));

    driver.advanceWallClockTime(Duration.ofSeconds(10));

    assertThat(commands.readValuesToList()).extracting(Command::getId).containsExactly(next.getId());
  }

  private Command schedule(Object payload, Instant deliverAt) {
    Command command = Command.builder().payload(payload).build();
    pipe(command, header(deliverAt));
    return command;
  }

  private void pipe(Command command, Headers headers) {
    scheduled.pipeInput(new TestRecord<>(command.getAggregateId(), command, headers, command.getTimestamp()));
  }

  private static Headers header(Instant deliverAt) {
    return new RecordHeaders().add(new RecordHeader(HeaderNames.DELIVER_AT, Long.toString(deliverAt.toEpochMilli()).getBytes(StandardCharsets.UTF_8)));
  }

  private List<?> stored() {
    return IteratorUtils.toList(store.all());
  }
}
