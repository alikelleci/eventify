package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.AggregateRoot;
import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import lombok.Value;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A handler can be written for a supertype of the messages it handles, e.g. the interface that groups them. The
 * messages always have their own class: the handler is found through it.
 */
@DisplayName("Handlers for a supertype")
class SupertypeHandlerTest {

  @Value
  @AggregateRoot
  public static class Counter {
    @AggregateId
    UUID id;
    int count;
  }

  @TopicInfo("commands.counter")
  public interface CounterCommand {
    UUID getId();
  }

  @TopicInfo("events.counter")
  public interface CounterEvent {
    UUID getId();
  }

  @Value
  public static class Increment implements CounterCommand {
    @AggregateId
    UUID id;
  }

  @Value
  public static class Reset implements CounterCommand {
    @AggregateId
    UUID id;
  }

  @Value
  public static class Incremented implements CounterEvent {
    @AggregateId
    UUID id;
  }

  @Value
  public static class WasReset implements CounterEvent {
    @AggregateId
    UUID id;
  }

  public static class CounterHandler {
    final List<String> handled = new ArrayList<>();

    /** Every counter command, by its interface. */
    @HandleCommand
    public Object handle(CounterCommand command, Counter state) {
      return command instanceof Reset ? new WasReset(command.getId()) : new Incremented(command.getId());
    }

    /** The events without a handler of their own. */
    @ApplyEvent
    public Counter apply(CounterEvent event, Counter state) {
      return new Counter(event.getId(), state == null ? 1 : state.getCount() + 1);
    }

    /** Its own handler: goes before the one for the interface. */
    @ApplyEvent
    public Counter apply(WasReset event, Counter state) {
      return new Counter(event.getId(), 0);
    }

    @HandleEvent
    public void onAny(CounterEvent event) {
      handled.add("any " + event.getClass().getSimpleName());
    }

    @HandleEvent
    public void on(WasReset event) {
      handled.add("reset");
    }
  }

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> commands;
  private TestInputTopic<String, Event> eventInput;
  private TestOutputTopic<String, Command> results;
  private TestOutputTopic<String, Event> events;
  private CounterHandler handler;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "supertype-handler-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    handler = new CounterHandler();
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(properties).registerHandler(handler).build().topology());
    commands = driver.createInputTopic("commands.counter", new StringSerializer(), new JsonSerializer<>());
    eventInput = driver.createInputTopic("events.counter", new StringSerializer(), new JsonSerializer<>());
    results = driver.createOutputTopic("commands.counter.results", new StringDeserializer(), new JsonDeserializer<>(Command.class));
    events = driver.createOutputTopic("events.counter", new StringDeserializer(), new JsonDeserializer<>(Event.class));
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("Should handle commands and apply events with handlers written for their interface")
  void handlersForTheInterfaceAreUsed() {
    UUID id = UUID.randomUUID();
    send(new Increment(id));
    send(new Increment(id));
    send(new Reset(id));
    send(new Increment(id));

    assertThat(results.readValuesToList())
        .extracting(result -> result.getMetadata().get(Metadata.RESULT))
        .containsExactly("success", "success", "success", "success");
    assertThat(events.readValuesToList())
        .extracting(Event::getType)
        .containsExactly("Incremented", "Incremented", "WasReset", "Incremented");
  }

  @Test
  @DisplayName("Should invoke the event handlers of the event's class and of its supertypes")
  void eventHandlersOfAllMatchingTypesAreInvoked() {
    UUID id = UUID.randomUUID();
    Event event = Event.builder().payload(new WasReset(id)).build();
    eventInput.pipeInput(event.getAggregateId(), event);

    assertThat(handler.handled).containsExactlyInAnyOrder("reset", "any WasReset");
  }

  private void send(Object payload) {
    Command command = Command.builder().payload(payload).build();
    commands.pipeInput(command.getAggregateId(), command);
  }
}
