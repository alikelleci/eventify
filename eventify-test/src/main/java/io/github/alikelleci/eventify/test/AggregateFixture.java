package io.github.alikelleci.eventify.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.AggregateRepository;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.SnapshotStore;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandResult;
import io.github.alikelleci.eventify.core.command.CommandResultSerde;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.TopicNames;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventStore;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Tests the handlers of one aggregate: given its past events, when a command arrives, then expect its events, its
 * rejection or the new state. Every scenario runs Eventify on its own {@link TopologyTestDriver}, without a broker.
 *
 * <pre>{@code
 * AggregateFixture<Order> fixture = AggregateFixture.of(Order.class, new OrderHandler());
 *
 * fixture.given(new OrderPlaced("order-1", "Ada"))
 *     .when(new CancelOrder("order-1"))
 *     .expectEvents(new OrderCancelled("order-1"));
 * }</pre>
 */
public final class AggregateFixture<A> {

  /** The names of Eventify's stores. */
  private static final String EVENT_STORE = "event-store";
  private static final String SNAPSHOT_STORE = "snapshot-store";

  private final Class<A> aggregateClass;
  private final String aggregateType;
  private final List<Object> handlers;
  /** {@code null} for Eventify's default. */
  private final ObjectMapper objectMapper;

  private AggregateFixture(Class<A> aggregateClass, List<Object> handlers, ObjectMapper objectMapper) {
    AggregateRoot root = aggregateClass.getAnnotation(AggregateRoot.class);
    if (root == null) {
      throw new IllegalArgumentException(aggregateClass.getName() + " is no aggregate: annotate it with @AggregateRoot.");
    }
    this.aggregateClass = aggregateClass;
    this.aggregateType = root.value();
    this.handlers = handlers;
    this.objectMapper = objectMapper;
  }

  /** A fixture for this aggregate, with its command and event sourcing handlers. */
  public static <A> AggregateFixture<A> of(Class<A> aggregateClass, Object... handlers) {
    return new AggregateFixture<>(aggregateClass, List.of(handlers), null);
  }

  /** This fixture with the ObjectMapper of your application, so events and state are written as there. */
  public AggregateFixture<A> withObjectMapper(ObjectMapper objectMapper) {
    return new AggregateFixture<>(aggregateClass, handlers, objectMapper);
  }

  /** The aggregate's past events, oldest first; each aggregate id's sequence starts at 1. */
  public Given given(Object... events) {
    return new Given(List.of(events));
  }

  /** A command for an aggregate without past events. */
  public Then<A> when(Object command) {
    return given().when(command);
  }

  /** The past events of a scenario. */
  public final class Given {
    private final List<Object> events;

    private Given(List<Object> events) {
      this.events = events;
    }

    /** Handles the command, a payload or a {@link Command}, and returns its outcome. */
    public Then<A> when(Object command) {
      Command message = command instanceof Command c ? c : Command.builder().payload(command).build();
      Eventify eventify = eventify();
      if (!eventify.getAggregateTypes().contains(aggregateType)) {
        throw new IllegalArgumentException("No handler is registered for aggregate " + aggregateClass.getSimpleName() + ".");
      }
      if (!eventify.getCommandClasses().contains(message.getPayload().getClass())) {
        throw new IllegalArgumentException("No command handler is registered for " + message.getType() + ".");
      }
      String topic = message.getTopic().value();
      if (!eventify.getCommandTopics(aggregateType).contains(topic)) {
        throw new IllegalArgumentException(message.getType() + " is no command of aggregate " + aggregateClass.getSimpleName() + ".");
      }

      try (TopologyTestDriver driver = new TopologyTestDriver(eventify.topology())) {
        KeyValueStore<String, Event> eventStore = driver.getKeyValueStore(EVENT_STORE);
        KeyValueStore<String, AggregateState> snapshotStore = driver.getKeyValueStore(SNAPSHOT_STORE);
        new EventStore(eventStore).save(pastEvents());

        TestInputTopic<String, Command> commands = driver.createInputTopic(topic,
            new StringSerializer(), new CommandSerde(eventify.getObjectMapper()).serializer());
        TestOutputTopic<String, CommandResult> results = driver.createOutputTopic(TopicNames.resultTopicOf(topic),
            new StringDeserializer(), new CommandResultSerde(eventify.getObjectMapper()).deserializer());
        commands.pipeInput(message.getAggregateId(), message);
        if (results.isEmpty()) {
          throw new IllegalStateException("Command " + message.getType() + " has no result.");
        }
        CommandResult result = results.readValue();

        AggregateRepository repository = eventify.getAggregateRepository(aggregateType,
            new EventStore(eventStore), new SnapshotStore(snapshotStore));
        Object state = repository.replay(message.getAggregateId()).currentState().getPayload();
        return new Then<>(result, aggregateClass.cast(state));
      }
    }

    private List<Event> pastEvents() {
      Map<String, Long> sequences = new HashMap<>();
      List<Event> stored = new ArrayList<>();
      for (Object payload : events) {
        // Built once to read its aggregate id.
        String aggregateId = Event.builder().aggregateType(aggregateType).payload(payload).sequence(1).build().getAggregateId();
        long sequence = sequences.merge(aggregateId, 1L, Long::sum);
        stored.add(Event.builder().aggregateType(aggregateType).payload(payload).sequence(sequence).build());
      }
      return stored;
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

  /** The outcome of a command. The expectations throw an {@link AssertionError}. */
  public static final class Then<A> {

    private final CommandResult result;
    private final A state;

    private Then(CommandResult result, A state) {
      this.result = result;
      this.state = state;
    }

    /** The command's result, as written to its result topic. */
    public CommandResult result() {
      return result;
    }

    /** The payloads of the command's events in stored order; empty when it was rejected. */
    public List<Object> events() {
      return result instanceof CommandResult.Success success
          ? success.events().stream().map(Event::getPayload).toList()
          : List.of();
    }

    /** The aggregate after the command; {@code null} before it is created or after it is removed. */
    public A state() {
      return state;
    }

    /** Why the command was rejected, as "Exception: message" of the root cause; {@code null} when it was accepted. */
    public String cause() {
      return result instanceof CommandResult.Failure failure ? failure.cause() : null;
    }

    /** The command was accepted. */
    public Then<A> expectSuccess() {
      if (cause() != null) {
        throw new AssertionError("Expected " + type() + " to succeed, but it was rejected: " + cause());
      }
      return this;
    }

    /** The command was accepted with exactly these events, in this order. */
    public Then<A> expectEvents(Object... events) {
      expectSuccess();
      List<Object> expected = List.of(events);
      if (!expected.equals(events())) {
        throw new AssertionError("Expected " + type() + " to give events " + expected + ", but it gave " + events() + ".");
      }
      return this;
    }

    /** The command was accepted without events. */
    public Then<A> expectNoEvents() {
      return expectEvents();
    }

    /** The command was rejected, and nothing of it was stored. */
    public Then<A> expectRejection() {
      if (cause() == null) {
        throw new AssertionError("Expected " + type() + " to be rejected, but it succeeded with events " + events() + ".");
      }
      return this;
    }

    /** The command was rejected by this exception, as the root cause. */
    public Then<A> expectRejection(Class<? extends Throwable> exception) {
      expectRejection();
      if (!Causes.isOf(cause(), exception)) {
        throw new AssertionError("Expected " + type() + " to be rejected with " + Causes.shortName(exception)
            + ", but it was rejected with: " + cause());
      }
      return this;
    }

    /** The command was rejected by this exception with this message, as the root cause. */
    public Then<A> expectRejection(Class<? extends Throwable> exception, String message) {
      expectRejection();
      String expected = Causes.of(exception, message);
      if (!expected.equals(cause())) {
        throw new AssertionError("Expected " + type() + " to be rejected with \"" + expected + "\", but it was rejected with: " + cause());
      }
      return this;
    }

    /** Checks the aggregate after the command, {@code null} before it is created or after it is removed. */
    public Then<A> expectState(Consumer<A> assertions) {
      assertions.accept(state);
      return this;
    }

    private String type() {
      return result.command().getType();
    }
  }
}
