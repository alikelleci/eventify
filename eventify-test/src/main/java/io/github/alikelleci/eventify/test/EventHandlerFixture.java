package io.github.alikelleci.eventify.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.Scheduled;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Tests event handlers that send commands: when an event arrives, then expect the commands they send now or
 * schedule, or their failure. Every scenario runs Eventify on its own {@link TopologyTestDriver}, without a broker.
 *
 * <pre>{@code
 * EventHandlerFixture fixture = EventHandlerFixture.of(new ShippingHandler());
 *
 * fixture.when(new OrderPaid("order-1"))
 *     .expectCommands(new ShipOrder("order-1"));
 * }</pre>
 */
public final class EventHandlerFixture {

  /** On a scheduled command's record: when to deliver it, in epoch milliseconds. */
  private static final String DELIVER_AT = "eventify-deliver-at";
  /** Event handlers don't see an event's aggregate type. */
  private static final String AGGREGATE_TYPE = "eventify-test";

  private final List<Object> handlers;
  /** {@code null} for Eventify's default. */
  private final ObjectMapper objectMapper;

  private EventHandlerFixture(List<Object> handlers, ObjectMapper objectMapper) {
    this.handlers = handlers;
    this.objectMapper = objectMapper;
  }

  /** A fixture for these event handlers. */
  public static EventHandlerFixture of(Object... handlers) {
    return new EventHandlerFixture(List.of(handlers), null);
  }

  /** This fixture with the ObjectMapper of your application, so events and commands are written as there. */
  public EventHandlerFixture withObjectMapper(ObjectMapper objectMapper) {
    return new EventHandlerFixture(handlers, objectMapper);
  }

  /** For {@link Then#expectScheduled}: this command, scheduled this long after the event's timestamp. */
  public static After after(Duration delay, Object command) {
    if (delay == null || command == null) {
      throw new IllegalArgumentException("A scheduled command needs a delay and a command.");
    }
    if (delay.isNegative()) {
      throw new IllegalArgumentException("A delay can't be negative, not " + delay + ".");
    }
    return new After(delay, command);
  }

  /** A command scheduled a delay after the event's timestamp; made with {@link #after}. */
  public record After(Duration delay, Object command) {
  }

  /** Handles the event, a payload or an {@link Event}, and returns what its handlers sent. */
  public Then when(Object event) {
    Event message = event instanceof Event e ? e : Event.builder().aggregateType(AGGREGATE_TYPE).payload(event).sequence(1).build();
    Eventify eventify = eventify();
    if (!eventify.getCommandClasses().isEmpty()) {
      throw new IllegalArgumentException("An EventHandlerFixture takes no command handlers: test them with an AggregateFixture.");
    }
    if (!eventify.getEventClasses().contains(message.getPayload().getClass())) {
      throw new IllegalArgumentException("No event handler is registered for " + message.getType() + ".");
    }

    try (TopologyTestDriver driver = new TopologyTestDriver(eventify.topology())) {
      String failure = null;
      try {
        driver.createInputTopic(message.getTopic().value(), new StringSerializer(), new EventSerde(eventify.getObjectMapper()).serializer())
            .pipeInput(message.getAggregateId(), message);
      } catch (RuntimeException e) {
        failure = Causes.of(e);
      }

      // Only commands: the fixture takes no command handlers, so nothing else is sent. Sorted for a stable order.
      List<Command> commands = new ArrayList<>();
      List<Scheduled> scheduled = new ArrayList<>();
      CommandSerde commandSerde = new CommandSerde(eventify.getObjectMapper());
      driver.producedTopicNames().stream().sorted().forEach(topic -> {
        for (TestRecord<String, Command> record : driver.createOutputTopic(topic, new StringDeserializer(), commandSerde.deserializer()).readRecordsToList()) {
          Header deliverAt = record.headers().lastHeader(DELIVER_AT);
          if (deliverAt == null) {
            commands.add(record.value());
          } else {
            scheduled.add(Scheduled.of(record.value(), Instant.ofEpochMilli(Long.parseLong(new String(deliverAt.value(), StandardCharsets.UTF_8)))));
          }
        }
      });
      return new Then(message, commands, scheduled, failure);
    }
  }

  private Eventify eventify() {
    Properties streamsConfig = new Properties();
    streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventify-test");
    streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
    Eventify.EventifyBuilder builder = Eventify.builder().streamsConfig(streamsConfig).objectMapper(objectMapper);
    handlers.forEach(builder::registerHandler);
    return builder.build();
  }

  /**
   * What the event handlers sent for an event. The expectations throw an {@link AssertionError}. Kafka keeps the
   * order of commands per topic, so they check the order per topic.
   */
  public static final class Then {

    private final Event event;
    private final List<Command> commands;
    /** With the commands as sent. */
    private final List<Scheduled> scheduled;
    private final String cause;

    private Then(Event event, List<Command> commands, List<Scheduled> scheduled, String cause) {
      this.event = event;
      this.commands = commands;
      this.scheduled = scheduled;
      this.cause = cause;
    }

    /** The event that was handled; the handlers get the same timestamp, the one to schedule from. */
    public Event event() {
      return event;
    }

    /** The payloads of the commands sent now, per topic in sent order. */
    public List<Object> commands() {
      return commands.stream().map(Command::getPayload).toList();
    }

    /** The scheduled commands with their payload and delivery time, per topic in sent order. */
    public List<Scheduled> scheduled() {
      return scheduled.stream().map(each -> Scheduled.of(((Command) each.command()).getPayload(), each.deliverAt())).toList();
    }

    /** Every command as sent, with its metadata: first the ones sent now, then the scheduled ones. */
    public List<Command> sent() {
      return Stream.concat(commands.stream(), scheduled.stream().map(each -> (Command) each.command())).toList();
    }

    /** Why handling the event failed, as "Exception: message" of the root cause; {@code null} when it didn't. */
    public String cause() {
      return cause;
    }

    /** The event was handled. */
    public Then expectSuccess() {
      if (cause != null) {
        throw new AssertionError("Expected " + event.getType() + " to be handled, but it failed: " + cause);
      }
      return this;
    }

    /** These commands, as payloads, were sent now, and no others; in this order per topic. */
    public Then expectCommands(Object... commands) {
      expectSuccess();
      List<Object> expected = Stream.of(commands).map(command -> payloadOf(command, "expectCommands")).toList();
      if (!byTopic(expected).equals(byTopic(commands()))) {
        throw new AssertionError("Expected " + event.getType() + " to send commands " + expected + ", but it sent " + commands() + ".");
      }
      return this;
    }

    /**
     * These commands were scheduled, and no others; in this order per topic, the time to the millisecond. Each is
     * {@link #after after(delay, command)}, timed from the event's timestamp, or {@code Scheduled.of(command, time)}.
     */
    public Then expectScheduled(Object... scheduled) {
      expectSuccess();
      List<Scheduled> expected = Stream.of(scheduled).map(this::resolve).map(Then::comparable).toList();
      List<Scheduled> actual = scheduled().stream().map(Then::comparable).toList();
      if (!byTopic(expected).equals(byTopic(actual))) {
        throw new AssertionError("Expected " + event.getType() + " to schedule " + expected + ", but it scheduled " + actual + ".");
      }
      return this;
    }

    /** The event was handled without sending or scheduling commands. */
    public Then expectNoCommands() {
      expectSuccess();
      if (!commands.isEmpty() || !scheduled.isEmpty()) {
        throw new AssertionError("Expected " + event.getType() + " to send no commands, but it sent " + commands()
            + " and scheduled " + scheduled() + ".");
      }
      return this;
    }

    /** Handling the event failed, and none of its commands were sent. */
    public Then expectFailure() {
      if (cause == null) {
        throw new AssertionError("Expected " + event.getType() + " to fail, but it was handled and sent " + sent().stream().map(Command::getPayload).toList() + ".");
      }
      return this;
    }

    /** Handling the event failed by this exception, as the root cause. */
    public Then expectFailure(Class<? extends Throwable> exception) {
      expectFailure();
      if (!Causes.isOf(cause, exception)) {
        throw new AssertionError("Expected " + event.getType() + " to fail with " + Causes.shortName(exception) + ", but it failed with: " + cause);
      }
      return this;
    }

    /** Handling the event failed by this exception with this message, as the root cause. */
    public Then expectFailure(Class<? extends Throwable> exception, String message) {
      expectFailure();
      String expected = Causes.of(exception, message);
      if (!expected.equals(cause)) {
        throw new AssertionError("Expected " + event.getType() + " to fail with \"" + expected + "\", but it failed with: " + cause);
      }
      return this;
    }

    /** An expected scheduled command with its time. */
    private Scheduled resolve(Object expected) {
      if (expected instanceof After after) {
        return Scheduled.of(payloadOf(after.command(), "expectScheduled"), event.getTimestamp().plus(after.delay()));
      }
      if (expected instanceof Scheduled each) {
        return Scheduled.of(payloadOf(each.command(), "expectScheduled"), each.deliverAt());
      }
      throw new IllegalArgumentException("expectScheduled takes after(delay, command) or Scheduled.of(command, time), not "
          + (expected == null ? "null" : expected.getClass().getSimpleName()) + ".");
    }

    /** An expected command is a payload: the metadata of a {@link Command} wouldn't be compared. */
    private static Object payloadOf(Object expected, String expectation) {
      if (expected instanceof Command) {
        throw new IllegalArgumentException(expectation + " takes the payloads of commands, not a Command: check its metadata with sent().");
      }
      return expected;
    }

    /** A scheduled command with its time to the millisecond, as Eventify stores it. */
    private static Scheduled comparable(Scheduled scheduled) {
      return Scheduled.of(scheduled.command(), scheduled.deliverAt().truncatedTo(ChronoUnit.MILLIS));
    }

    /** Payloads, or scheduled payloads, by the topic of their command, in their order. */
    private static Map<String, List<Object>> byTopic(List<?> values) {
      Map<String, List<Object>> byTopic = new LinkedHashMap<>();
      for (Object value : values) {
        Object payload = value instanceof Scheduled each ? each.command() : value;
        String topic = Command.builder().payload(payload).build().getTopic().value();
        byTopic.computeIfAbsent(topic, key -> new ArrayList<>()).add(value);
      }
      return byTopic;
    }
  }
}
