package io.github.alikelleci.eventify.core.command;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import jakarta.validation.constraints.NotBlank;
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

import static org.assertj.core.api.Assertions.assertThat;

/** Bean Validation rejects an invalid command before its handler runs; it produces no events. */
@DisplayName("Command validation")
class CommandValidationTest {

  @Topic("commands.customer")
  public record Register(@AggregateId String id, @NotBlank String name) {
  }

  @Topic("events.customer")
  public record Registered(@AggregateId String id, String name) {
  }

  @AggregateRoot("customer")
  public record Customer(@AggregateId String id, String name) {
  }

  public static class CustomerHandler {
    @CommandHandler
    public Registered handle(Register command, Customer state) {
      return new Registered(command.id(), command.name());
    }

    @EventSourcingHandler
    public Customer handle(Registered event, Customer state) {
      return new Customer(event.id(), event.name());
    }
  }

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> commands;
  private TestOutputTopic<String, CommandResult> results;
  private TestOutputTopic<String, Event> events;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "command-validation-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    driver = new TopologyTestDriver(Eventify.builder().streamsConfig(properties).registerHandler(new CustomerHandler()).build().topology());
    commands = driver.createInputTopic("commands.customer", new StringSerializer(), new CommandSerde().serializer());
    results = driver.createOutputTopic("commands.customer.results", new StringDeserializer(), new JsonDeserializer<>(CommandResult.class));
    events = driver.createOutputTopic("events.customer", new StringDeserializer(), new EventSerde().deserializer());
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  @Test
  @DisplayName("Should handle a valid command")
  void aValidCommand() {
    send(new Register("customer-1", "Ada"));

    assertThat(results.readValue()).isInstanceOf(CommandResult.Success.class);
    assertThat(events.readValuesToList()).hasSize(1);
  }

  @Test
  @DisplayName("Should reject an invalid command with the violation as its cause, and store nothing")
  void anInvalidCommand() {
    send(new Register("customer-1", " "));

    // The violation's message is in the JVM's language: only its kind and field are checked.
    assertThat(results.readValue()).isInstanceOfSatisfying(CommandResult.Failure.class, failure ->
        assertThat(failure.cause()).startsWith("ConstraintViolationException: name: "));
    assertThat(events.isEmpty()).isTrue();
    KeyValueStore<String, Event> eventStore = driver.getKeyValueStore("event-store");
    assertThat(eventStore.approximateNumEntries()).isZero();
  }

  private void send(Object payload) {
    Command command = Command.builder().payload(payload).build();
    commands.pipeInput(command.getAggregateId(), command);
  }
}
