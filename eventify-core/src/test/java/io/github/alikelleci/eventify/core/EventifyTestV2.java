package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.example.customer.core.Customer;
import io.github.alikelleci.eventify.core.example.customer.core.CustomerCommandHandler;
import io.github.alikelleci.eventify.core.example.customer.core.CustomerEventHandler;
import io.github.alikelleci.eventify.core.example.customer.core.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.core.example.customer.core.CustomerEventUpcaster;
import io.github.alikelleci.eventify.core.example.customer.core.CustomerResultHandler;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerCommand;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
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
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static io.github.alikelleci.eventify.core.factory.CommandFactory.buildAddCreditsCommand;
import static io.github.alikelleci.eventify.core.factory.CommandFactory.buildCreateCustomerCommand;
import static io.github.alikelleci.eventify.core.util.Matchers.assertCommandResult;
import static io.github.alikelleci.eventify.core.util.Matchers.assertEvent;
import static io.github.alikelleci.eventify.core.util.Matchers.assertSnapshot;
import static org.assertj.core.api.Assertions.assertThat;


@Slf4j
@DisplayName("Eventify Test")
class EventifyTestV2 {

  private TopologyTestDriver testDriver;
  private TestInputTopic<String, Command> commandsTopic;
  private TestOutputTopic<String, Command> commandResultsTopic;
  private TestOutputTopic<String, Event> eventsTopic;
  private KeyValueStore<String, Event> eventStore;
  private KeyValueStore<String, AggregateState> snapshotStore;

  @BeforeEach
  void setup() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventify-test-v3");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    Eventify eventify = Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler())
//        .registerHandler(new CustomerEventHandler())
//        .registerHandler(new CustomerResultHandler())
//        .registerHandler(new CustomerEventUpcaster())
        .build();

    testDriver = new TopologyTestDriver(eventify.topology());

    commandsTopic = testDriver.createInputTopic(
        CustomerCommand.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), new JsonSerializer<>());

    commandResultsTopic = testDriver.createOutputTopic(
        CustomerCommand.class.getAnnotation(TopicInfo.class).value().concat(".results"),
        new StringDeserializer(), new JsonDeserializer<>(Command.class));

    eventsTopic = testDriver.createOutputTopic(
        CustomerEvent.class.getAnnotation(TopicInfo.class).value(),
        new StringDeserializer(), new JsonDeserializer<>(Event.class));

    eventStore = testDriver.getKeyValueStore("event-store");
    snapshotStore = testDriver.getKeyValueStore("snapshot-store");
  }

  @AfterEach
  void tearDown() {
    if (testDriver != null) {
      testDriver.close();
    }
  }

  @Nested
  @DisplayName("Command Handling")
  class CommandHandlingTests {

    @Test
    @DisplayName("Should process command and produce event")
    void processCommand() {
      Command command = buildCreateCustomerCommand("customer-1", "John", "Doe", 100);
      commandsTopic.pipeInput(command.getAggregateId(), command);

      List<Command> results = commandResultsTopic.readValuesToList();
      assertThat(results).hasSize(1);
      assertCommandResult(command, results.get(0), true);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events).hasSize(1);
      assertEvent(command, events.get(0), CustomerCreated.class);

      List<Event> eventsInStore = readEventsFromStore("customer-1");
      assertThat(eventsInStore).hasSize(1);
      assertEvent(command, eventsInStore.get(0), CustomerCreated.class);
    }

    @Test
    @DisplayName("Should handle command validation failure")
    void commandValidationFailure() {
      Command command = buildAddCreditsCommand("customer-1", 100);
      commandsTopic.pipeInput(command.getAggregateId(), command);

      List<Command> results = commandResultsTopic.readValuesToList();
      assertThat(results).hasSize(1);
      assertCommandResult(command, results.get(0), false);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events).isEmpty();

      List<Event> eventsInStore = readEventsFromStore("customer-1");
      assertThat(eventsInStore).isEmpty();
    }

    @Test
    @DisplayName("Should process multiple commands sequentially")
    void multipleCommands() {
      Command command1 = buildCreateCustomerCommand("customer-1", "Bob", "Smith", 100);
      Command command2 = buildAddCreditsCommand("customer-1", 50);

      commandsTopic.pipeInput(command1.getAggregateId(), command1);
      commandsTopic.pipeInput(command2.getAggregateId(), command2);

      List<Command> results = commandResultsTopic.readValuesToList();
      assertThat(results).hasSize(2);
      assertCommandResult(command1, results.get(0), true);
      assertCommandResult(command2, results.get(1), true);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events).hasSize(2);
      assertEvent(command1, events.get(0), CustomerCreated.class);
      assertEvent(command2, events.get(1), CreditsAdded.class);

      List<Event> eventsInStore = readEventsFromStore("customer-1");
      assertThat(eventsInStore).hasSize(2);
      assertEvent(command1, eventsInStore.get(0), CustomerCreated.class);
      assertEvent(command2, eventsInStore.get(1), CreditsAdded.class);
    }
  }

  @Nested
  @DisplayName("Snapshotting")
  class SnapshottingTests {

    @Test
    @DisplayName("Should create snapshot at threshold")
    void createSnapshot() {
      List<Command> commands = List.of(
          buildCreateCustomerCommand("customer-1", "Ivy", "Jones", 100),
          buildAddCreditsCommand("customer-1", 1),
          buildAddCreditsCommand("customer-1", 1),
          buildAddCreditsCommand("customer-1", 1),
          buildAddCreditsCommand("customer-1", 1),
          buildAddCreditsCommand("customer-1", 1)
      );

      commands.forEach(cmd -> commandsTopic.pipeInput(cmd.getAggregateId(), cmd));

      List<Command> results = commandResultsTopic.readValuesToList();
      assertThat(results).hasSize(6);
      for (int i = 0; i < commands.size(); i++) {
        assertCommandResult(commands.get(i), results.get(i), true);
      }

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events).hasSize(6);

      List<Event> eventsInStore = readEventsFromStore("customer-1");
      assertThat(eventsInStore).hasSize(6);

      AggregateState snapshot = snapshotStore.get("customer-1");
      assertThat(snapshot).isNotNull();
      assertSnapshot(events.get(4), snapshot, Customer.class, 5);

      Customer customer = (Customer) snapshot.getPayload();
      assertThat(customer.getId()).isEqualTo("customer-1");
      assertThat(customer.getCredits()).isEqualTo(104);
    }

    @Test
    @DisplayName("Should restore from snapshot and apply remaining events")
    void restoreFromSnapshot() {
      List<Command> commands = List.of(
          buildCreateCustomerCommand("customer-12", "Jack", "King", 100),
          buildAddCreditsCommand("customer-12", 1),
          buildAddCreditsCommand("customer-12", 1),
          buildAddCreditsCommand("customer-12", 1),
          buildAddCreditsCommand("customer-12", 1),
          buildAddCreditsCommand("customer-12", 10)
      );

      commands.forEach(cmd -> commandsTopic.pipeInput(cmd.getAggregateId(), cmd));

      List<Command> results = commandResultsTopic.readValuesToList();
      assertThat(results).hasSize(6);
      results.forEach(result -> assertThat(result.getMetadata().get("$result")).isEqualTo("success"));

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events).hasSize(6);

      AggregateState snapshot = snapshotStore.get("customer-12");
      assertThat(snapshot).isNotNull();
      assertThat(snapshot.getVersion()).isEqualTo(5L);

      Customer customer = (Customer) snapshot.getPayload();
      assertThat(customer.getCredits()).isEqualTo(104);
    }
  }

  @Nested
  @DisplayName("Upcasting")
  class UpcastingTests {

    @Test
    @DisplayName("Should upcast events")
    void upcasting() {
      Command command = buildCreateCustomerCommand("customer-1", "Liam", "Moore", 100);
      commandsTopic.pipeInput(command.getAggregateId(), command);

      List<Command> results = commandResultsTopic.readValuesToList();
      assertThat(results).hasSize(1);

      List<Event> events = eventsTopic.readValuesToList();
      assertThat(events).hasSize(1);

      List<Event> eventsInStore = readEventsFromStore("customer-1");
      assertThat(eventsInStore).hasSize(1);

      CustomerCreated created = (CustomerCreated) events.get(0).getPayload();
      assertThat(created.getFirstName()).isEqualTo("Liam");
    }
  }

  private List<Event> readEventsFromStore(String aggregateId) {
    return IteratorUtils.toList(eventStore.all())
        .stream()
        .map(keyValue -> keyValue.value)
        .filter(event -> event.getAggregateId().equals(aggregateId))
        .filter(event -> event.getId().startsWith(aggregateId + "@"))
        .toList();
  }
}
