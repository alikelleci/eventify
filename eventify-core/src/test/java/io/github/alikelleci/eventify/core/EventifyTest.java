package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.example.customer.core.Customer;
import io.github.alikelleci.eventify.core.example.customer.core.CustomerCommandHandler;
import io.github.alikelleci.eventify.core.example.customer.core.CustomerEventSourcingHandler;
import io.github.alikelleci.eventify.core.example.customer.core.CustomerEventUpcaster;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerCommand;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CreditsAdded;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CreditsIssued;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CustomerCreated;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.CustomerDeleted;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.FirstNameChanged;
import io.github.alikelleci.eventify.core.example.customer.shared.CustomerEvent.LastNameChanged;
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
import static io.github.alikelleci.eventify.core.factory.CommandFactory.buildChangeFirstNameCommand;
import static io.github.alikelleci.eventify.core.factory.CommandFactory.buildChangeLastNameCommand;
import static io.github.alikelleci.eventify.core.factory.CommandFactory.buildCreateCustomerCommand;
import static io.github.alikelleci.eventify.core.factory.CommandFactory.buildDeleteCustomerCommand;
import static io.github.alikelleci.eventify.core.factory.CommandFactory.buildIssueCreditsCommand;
import static io.github.alikelleci.eventify.core.util.Matchers.assertCommandResult;
import static io.github.alikelleci.eventify.core.util.Matchers.assertEvent;
import static io.github.alikelleci.eventify.core.util.Matchers.assertSnapshot;
import static org.assertj.core.api.Assertions.assertThat;


@Slf4j
@DisplayName("Eventify Test")
class EventifyTest {

  static Eventify.EventifyBuilder baseBuilder() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventify-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    return Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new CustomerCommandHandler())
        .registerHandler(new CustomerEventSourcingHandler());
  }

  static TestInputTopic<String, Command> commandsTopic(TopologyTestDriver driver) {
    return driver.createInputTopic(
        CustomerCommand.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), new JsonSerializer<>());
  }

  static TestOutputTopic<String, Command> commandResultsTopic(TopologyTestDriver driver) {
    return driver.createOutputTopic(
        CustomerCommand.class.getAnnotation(TopicInfo.class).value().concat(".results"),
        new StringDeserializer(), new JsonDeserializer<>(Command.class));
  }

  static TestOutputTopic<String, Event> eventsTopic(TopologyTestDriver driver) {
    return driver.createOutputTopic(
        CustomerEvent.class.getAnnotation(TopicInfo.class).value(),
        new StringDeserializer(), new JsonDeserializer<>(Event.class));
  }

  static List<Event> readEventsFromStore(KeyValueStore<String, Event> eventStore, String aggregateId) {
    return IteratorUtils.toList(eventStore.all())
        .stream()
        .map(kv -> kv.value)
        .filter(event -> event.getAggregateId().equals(aggregateId))
        .filter(event -> event.getId().startsWith(aggregateId + "@"))
        .toList();
  }


  @Nested
  @DisplayName("Command Handling")
  class CommandHandlingTests {

    TopologyTestDriver driver;
    TestInputTopic<String, Command> commands;
    TestOutputTopic<String, Command> results;
    TestOutputTopic<String, Event> events;
    KeyValueStore<String, Event> eventStore;

    @BeforeEach
    void setup() {
      driver = new TopologyTestDriver(baseBuilder().build().topology());
      commands = commandsTopic(driver);
      results = commandResultsTopic(driver);
      events = eventsTopic(driver);
      eventStore = driver.getKeyValueStore("event-store");
    }

    @AfterEach
    void tearDown() {
      driver.close();
    }

    @Test
    @DisplayName("Should create customer and produce CustomerCreated event")
    void createCustomer() {
      Command command = buildCreateCustomerCommand("customer-1", "John", "Doe", 100);
      commands.pipeInput(command.getAggregateId(), command);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(1);
      assertCommandResult(command, resultList.get(0), true);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(1);
      assertEvent(command, eventList.get(0), CustomerCreated.class);

      List<Event> storedEvents = readEventsFromStore(eventStore, "customer-1");
      assertThat(storedEvents).hasSize(1);
      assertEvent(command, storedEvents.get(0), CustomerCreated.class);
    }

    @Test
    @DisplayName("Should fail with bean validation error when credits exceed @Max(100)")
    void beanValidationFailure() {
      Command command = buildCreateCustomerCommand("customer-1", "John", "Doe", 200);
      commands.pipeInput(command.getAggregateId(), command);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(1);
      assertCommandResult(command, resultList.get(0), false);

      assertThat(events.readValuesToList()).isEmpty();
      assertThat(readEventsFromStore(eventStore, "customer-1")).isEmpty();
    }

    @Test
    @DisplayName("Should fail with business rule error when customer does not exist")
    void businessRuleFailure() {
      Command command = buildAddCreditsCommand("customer-1", 50);
      commands.pipeInput(command.getAggregateId(), command);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(1);
      assertCommandResult(command, resultList.get(0), false);

      assertThat(events.readValuesToList()).isEmpty();
      assertThat(readEventsFromStore(eventStore, "customer-1")).isEmpty();
    }

    @Test
    @DisplayName("Should fail when creating a customer that already exists")
    void createDuplicateCustomer() {
      Command command1 = buildCreateCustomerCommand("customer-1", "John", "Doe", 100);
      Command command2 = buildCreateCustomerCommand("customer-1", "Jane", "Doe", 50);

      commands.pipeInput(command1.getAggregateId(), command1);
      commands.pipeInput(command2.getAggregateId(), command2);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(2);
      assertCommandResult(command1, resultList.get(0), true);
      assertCommandResult(command2, resultList.get(1), false);

      assertThat(readEventsFromStore(eventStore, "customer-1")).hasSize(1);
    }

    @Test
    @DisplayName("Should change first and last name and reflect updated state")
    void changeNames() {
      Command create = buildCreateCustomerCommand("customer-1", "John", "Doe", 100);
      Command changeFirst = buildChangeFirstNameCommand("customer-1", "Jane");
      Command changeLast = buildChangeLastNameCommand("customer-1", "Smith");

      commands.pipeInput(create.getAggregateId(), create);
      commands.pipeInput(changeFirst.getAggregateId(), changeFirst);
      commands.pipeInput(changeLast.getAggregateId(), changeLast);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(3);
      assertCommandResult(create, resultList.get(0), true);
      assertCommandResult(changeFirst, resultList.get(1), true);
      assertCommandResult(changeLast, resultList.get(2), true);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(3);
      assertEvent(changeFirst, eventList.get(1), FirstNameChanged.class);
      assertEvent(changeLast, eventList.get(2), LastNameChanged.class);
      assertThat(((FirstNameChanged) eventList.get(1).getPayload()).getFirstName()).isEqualTo("Jane");
      assertThat(((LastNameChanged) eventList.get(2).getPayload()).getLastName()).isEqualTo("Smith");
    }

    @Test
    @DisplayName("Should issue credits and produce CreditsIssued event")
    void issueCredits() {
      Command create = buildCreateCustomerCommand("customer-1", "John", "Doe", 100);
      Command issue = buildIssueCreditsCommand("customer-1", 40);

      commands.pipeInput(create.getAggregateId(), create);
      commands.pipeInput(issue.getAggregateId(), issue);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(2);
      assertCommandResult(create, resultList.get(0), true);
      assertCommandResult(issue, resultList.get(1), true);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(2);
      assertEvent(issue, eventList.get(1), CreditsIssued.class);
      assertThat(((CreditsIssued) eventList.get(1).getPayload()).getAmount()).isEqualTo(40);
    }

    @Test
    @DisplayName("Should fail issuing credits when balance is insufficient")
    void issueCreditsInsufficientBalance() {
      Command create = buildCreateCustomerCommand("customer-1", "John", "Doe", 100);
      Command issue = buildIssueCreditsCommand("customer-1", 200);

      commands.pipeInput(create.getAggregateId(), create);
      commands.pipeInput(issue.getAggregateId(), issue);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(2);
      assertCommandResult(create, resultList.get(0), true);
      assertCommandResult(issue, resultList.get(1), false);

      assertThat(readEventsFromStore(eventStore, "customer-1")).hasSize(1);
    }

    @Test
    @DisplayName("Should delete customer and reject subsequent commands")
    void deleteCustomerAndRejectFollowUp() {
      Command create = buildCreateCustomerCommand("customer-1", "John", "Doe", 100);
      Command delete = buildDeleteCustomerCommand("customer-1");
      Command addAfterDelete = buildAddCreditsCommand("customer-1", 50);

      commands.pipeInput(create.getAggregateId(), create);
      commands.pipeInput(delete.getAggregateId(), delete);
      commands.pipeInput(addAfterDelete.getAggregateId(), addAfterDelete);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(3);
      assertCommandResult(create, resultList.get(0), true);
      assertCommandResult(delete, resultList.get(1), true);
      assertCommandResult(addAfterDelete, resultList.get(2), false);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(2);
      assertEvent(delete, eventList.get(1), CustomerDeleted.class);
    }
  }


  @Nested
  @DisplayName("Snapshotting")
  class SnapshottingTests {

    TopologyTestDriver driver;
    TestInputTopic<String, Command> commands;
    TestOutputTopic<String, Command> results;
    TestOutputTopic<String, Event> events;
    KeyValueStore<String, AggregateState> snapshotStore;
    KeyValueStore<String, Event> eventStore;

    @BeforeEach
    void setup() {
      driver = new TopologyTestDriver(baseBuilder().build().topology());
      commands = commandsTopic(driver);
      results = commandResultsTopic(driver);
      events = eventsTopic(driver);
      snapshotStore = driver.getKeyValueStore("snapshot-store");
      eventStore = driver.getKeyValueStore("event-store");
    }

    @AfterEach
    void tearDown() {
      driver.close();
    }

    @Test
    @DisplayName("Should create snapshot when threshold is reached on next command load")
    void createSnapshot() {
      // Snapshot is triggered during loadAggregate at the start of the 6th command,
      // after 5 events are already in the store (5 % threshold(5) == 0)
      List<Command> commandList = List.of(
          buildCreateCustomerCommand("customer-1", "Ivy", "Jones", 100),
          buildAddCreditsCommand("customer-1", 1),
          buildAddCreditsCommand("customer-1", 1),
          buildAddCreditsCommand("customer-1", 1),
          buildAddCreditsCommand("customer-1", 1), // 5th event stored
          buildAddCreditsCommand("customer-1", 1)  // 6th command triggers snapshot
      );
      commandList.forEach(cmd -> commands.pipeInput(cmd.getAggregateId(), cmd));

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(6);
      for (int i = 0; i < commandList.size(); i++) {
        assertCommandResult(commandList.get(i), resultList.get(i), true);
      }

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(6);
      assertThat(readEventsFromStore(eventStore, "customer-1")).hasSize(6);

      AggregateState snapshot = snapshotStore.get("customer-1");
      assertThat(snapshot).isNotNull();
      assertSnapshot(eventList.get(4), snapshot, Customer.class, 5);
      assertThat(((Customer) snapshot.getPayload()).getId()).isEqualTo("customer-1");
      assertThat(((Customer) snapshot.getPayload()).getCredits()).isEqualTo(104);
    }

    @Test
    @DisplayName("Should resume state from snapshot and correctly apply subsequent events")
    void resumeFromSnapshot() {
      List<Command> commandList = List.of(
          buildCreateCustomerCommand("customer-1", "Ivy", "Jones", 100),
          buildAddCreditsCommand("customer-1", 1),
          buildAddCreditsCommand("customer-1", 1),
          buildAddCreditsCommand("customer-1", 1),
          buildAddCreditsCommand("customer-1", 1), // 5th event stored
          buildAddCreditsCommand("customer-1", 1)  // 6th command triggers snapshot at version 5
      );
      commandList.forEach(cmd -> commands.pipeInput(cmd.getAggregateId(), cmd));

      Command addAfterSnapshot = buildAddCreditsCommand("customer-1", 10);
      commands.pipeInput(addAfterSnapshot.getAggregateId(), addAfterSnapshot);

      // Snapshot at version 5, credits = 104
      AggregateState snapshot = snapshotStore.get("customer-1");
      assertThat(snapshot).isNotNull();
      assertThat(snapshot.getVersion()).isEqualTo(5);
      assertThat(((Customer) snapshot.getPayload()).getCredits()).isEqualTo(104);

      // 7th command: state rebuilt from snapshot + event 6, then adds 10
      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(7);
      assertCommandResult(addAfterSnapshot, resultList.get(6), true);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(7);
      assertEvent(addAfterSnapshot, eventList.get(6), CreditsAdded.class);
      assertThat(((CreditsAdded) eventList.get(6).getPayload()).getAmount()).isEqualTo(10);
    }
  }


  @Nested
  @DisplayName("Upcasting")
  class UpcastingTests {

    TopologyTestDriver driver;
    TestInputTopic<String, Command> commands;
    TestOutputTopic<String, Command> results;
    KeyValueStore<String, Event> eventStore;

    @BeforeEach
    void setup() {
      driver = new TopologyTestDriver(baseBuilder()
          .registerHandler(new CustomerEventUpcaster())
          .build().topology());
      commands = commandsTopic(driver);
      results = commandResultsTopic(driver);
      eventStore = driver.getKeyValueStore("event-store");
    }

    @AfterEach
    void tearDown() {
      driver.close();
    }

    @Test
    @DisplayName("Should apply upcasters when replaying stored events")
    void upcastingAppliedOnReplay() {
      // CustomerCreated has no @Revision so revision=1, upcasters rev1→2→3 fire, setting firstName to "John v3 -> v4"
      Command create = buildCreateCustomerCommand("customer-1", "Original", "Name", 100);
      commands.pipeInput(create.getAggregateId(), create);

      // Second command forces a replay of the stored CustomerCreated event through the upcaster chain
      Command addCredits = buildAddCreditsCommand("customer-1", 10);
      commands.pipeInput(addCredits.getAggregateId(), addCredits);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(2);
      assertCommandResult(create, resultList.get(0), true);
      assertCommandResult(addCredits, resultList.get(1), true);

      // The upcasted firstName is visible on the event read from the store (deserialized through upcaster chain)
      List<Event> storedEvents = readEventsFromStore(eventStore, "customer-1");
      assertThat(storedEvents).hasSize(2);
      assertThat(((CustomerCreated) storedEvents.get(0).getPayload()).getFirstName()).isEqualTo("John v3 -> v4");
    }
  }
}
