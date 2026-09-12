package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.domain.Order;
import io.github.alikelleci.eventify.core.domain.OrderCommandHandler;
import io.github.alikelleci.eventify.core.domain.OrderEventSourcingHandler;
import io.github.alikelleci.eventify.core.domain.OrderEventUpcaster;
import io.github.alikelleci.eventify.core.domain.OrderCommand;
import io.github.alikelleci.eventify.core.domain.OrderCommand.PlaceOrder;
import io.github.alikelleci.eventify.core.domain.OrderCommand.ShipOrder;
import io.github.alikelleci.eventify.core.domain.OrderEvent;
import io.github.alikelleci.eventify.core.domain.OrderEvent.OrderPlaced;
import io.github.alikelleci.eventify.core.domain.OrderEvent.OrderConfirmed;
import io.github.alikelleci.eventify.core.domain.OrderEvent.OrderShipped;
import io.github.alikelleci.eventify.core.domain.OrderEvent.OrderDelivered;
import io.github.alikelleci.eventify.core.domain.OrderEvent.OrderCancelled;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
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

import static io.github.alikelleci.eventify.core.support.CommandFactory.buildPlaceOrderCommand;
import static io.github.alikelleci.eventify.core.support.CommandFactory.buildConfirmOrderCommand;
import static io.github.alikelleci.eventify.core.support.CommandFactory.buildShipOrderCommand;
import static io.github.alikelleci.eventify.core.support.CommandFactory.buildDeliverOrderCommand;
import static io.github.alikelleci.eventify.core.support.CommandFactory.buildCancelOrderCommand;
import static io.github.alikelleci.eventify.core.support.Matchers.assertCommandResult;
import static io.github.alikelleci.eventify.core.support.Matchers.assertEvent;
import static io.github.alikelleci.eventify.core.support.Matchers.assertSnapshot;
import static org.assertj.core.api.Assertions.assertThat;


@DisplayName("Eventify Test")
class EventifyTest {

  static Eventify.EventifyBuilder baseBuilder() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "eventify-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

    return Eventify.builder()
        .streamsConfig(properties)
        .registerHandler(new OrderCommandHandler())
        .registerHandler(new OrderEventSourcingHandler());
  }

  static TestInputTopic<String, Command> commandsTopic(TopologyTestDriver driver) {
    return driver.createInputTopic(
        OrderCommand.class.getAnnotation(TopicInfo.class).value(),
        new StringSerializer(), new JsonSerializer<>());
  }

  static TestOutputTopic<String, Command> commandResultsTopic(TopologyTestDriver driver) {
    return driver.createOutputTopic(
        OrderCommand.class.getAnnotation(TopicInfo.class).value().concat(".results"),
        new StringDeserializer(), new JsonDeserializer<>(Command.class));
  }

  static TestOutputTopic<String, Event> eventsTopic(TopologyTestDriver driver) {
    return driver.createOutputTopic(
        OrderEvent.class.getAnnotation(TopicInfo.class).value(),
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
    @DisplayName("Should place order and produce OrderPlaced event")
    void placeOrder() {
      Command command = buildPlaceOrderCommand("order-1");
      String expectedCustomer = ((PlaceOrder) command.getPayload()).getCustomer();

      commands.pipeInput(command.getAggregateId(), command);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(1);
      assertCommandResult(command, resultList.get(0), true);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(1);
      assertEvent(command, eventList.get(0), OrderPlaced.class);
      assertThat(((OrderPlaced) eventList.get(0).getPayload()).getCustomer()).isEqualTo(expectedCustomer);

      List<Event> storedEvents = readEventsFromStore(eventStore, "order-1");
      assertThat(storedEvents).hasSize(1);
      assertEvent(command, storedEvents.get(0), OrderPlaced.class);
    }

    @Test
    @DisplayName("Should fail when placing an order that already exists")
    void placeOrderDuplicate() {
      Command command1 = buildPlaceOrderCommand("order-1");
      Command command2 = buildPlaceOrderCommand("order-1");

      commands.pipeInput(command1.getAggregateId(), command1);
      commands.pipeInput(command2.getAggregateId(), command2);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(2);
      assertCommandResult(command1, resultList.get(0), true);
      assertCommandResult(command2, resultList.get(1), false);

      assertThat(readEventsFromStore(eventStore, "order-1")).hasSize(1);
    }

    @Test
    @DisplayName("Should fail to confirm an order that does not exist")
    void confirmOrderNotFound() {
      Command command = buildConfirmOrderCommand("order-1");
      commands.pipeInput(command.getAggregateId(), command);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(1);
      assertCommandResult(command, resultList.get(0), false);

      assertThat(events.readValuesToList()).isEmpty();
      assertThat(readEventsFromStore(eventStore, "order-1")).isEmpty();
    }

    @Test
    @DisplayName("Should confirm a placed order and produce OrderConfirmed event")
    void confirmOrder() {
      Command place = buildPlaceOrderCommand("order-1");
      Command confirm = buildConfirmOrderCommand("order-1");

      commands.pipeInput(place.getAggregateId(), place);
      commands.pipeInput(confirm.getAggregateId(), confirm);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(2);
      assertCommandResult(place, resultList.get(0), true);
      assertCommandResult(confirm, resultList.get(1), true);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(2);
      assertEvent(confirm, eventList.get(1), OrderConfirmed.class);
    }

    @Test
    @DisplayName("Should fail to confirm an order that is not in PLACED status")
    void confirmOrderWrongStatus() {
      Command place = buildPlaceOrderCommand("order-1");
      Command confirm = buildConfirmOrderCommand("order-1");
      Command confirmAgain = buildConfirmOrderCommand("order-1");

      commands.pipeInput(place.getAggregateId(), place);
      commands.pipeInput(confirm.getAggregateId(), confirm);
      commands.pipeInput(confirmAgain.getAggregateId(), confirmAgain);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(3);
      assertCommandResult(confirmAgain, resultList.get(2), false);

      assertThat(readEventsFromStore(eventStore, "order-1")).hasSize(2);
    }

    @Test
    @DisplayName("Should fail to ship an order that is not confirmed")
    void shipOrderNotConfirmed() {
      Command place = buildPlaceOrderCommand("order-1");
      Command ship = buildShipOrderCommand("order-1");

      commands.pipeInput(place.getAggregateId(), place);
      commands.pipeInput(ship.getAggregateId(), ship);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(2);
      assertCommandResult(ship, resultList.get(1), false);

      assertThat(readEventsFromStore(eventStore, "order-1")).hasSize(1);
    }

    @Test
    @DisplayName("Should ship a confirmed order and produce OrderShipped event")
    void shipOrder() {
      Command place = buildPlaceOrderCommand("order-1");
      Command confirm = buildConfirmOrderCommand("order-1");
      Command ship = buildShipOrderCommand("order-1");
      String expectedTrackingNumber = ((ShipOrder) ship.getPayload()).getTrackingNumber();

      commands.pipeInput(place.getAggregateId(), place);
      commands.pipeInput(confirm.getAggregateId(), confirm);
      commands.pipeInput(ship.getAggregateId(), ship);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(3);
      assertCommandResult(ship, resultList.get(2), true);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(3);
      assertEvent(ship, eventList.get(2), OrderShipped.class);
      assertThat(((OrderShipped) eventList.get(2).getPayload()).getTrackingNumber()).isEqualTo(expectedTrackingNumber);
    }

    @Test
    @DisplayName("Should fail to deliver an order that is not shipped")
    void deliverOrderNotShipped() {
      Command place = buildPlaceOrderCommand("order-1");
      Command confirm = buildConfirmOrderCommand("order-1");
      Command deliver = buildDeliverOrderCommand("order-1");

      commands.pipeInput(place.getAggregateId(), place);
      commands.pipeInput(confirm.getAggregateId(), confirm);
      commands.pipeInput(deliver.getAggregateId(), deliver);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(3);
      assertCommandResult(deliver, resultList.get(2), false);

      assertThat(readEventsFromStore(eventStore, "order-1")).hasSize(2);
    }

    @Test
    @DisplayName("Should deliver a shipped order and produce OrderDelivered event")
    void deliverOrder() {
      Command place = buildPlaceOrderCommand("order-1");
      Command confirm = buildConfirmOrderCommand("order-1");
      Command ship = buildShipOrderCommand("order-1");
      Command deliver = buildDeliverOrderCommand("order-1");

      commands.pipeInput(place.getAggregateId(), place);
      commands.pipeInput(confirm.getAggregateId(), confirm);
      commands.pipeInput(ship.getAggregateId(), ship);
      commands.pipeInput(deliver.getAggregateId(), deliver);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(4);
      assertCommandResult(deliver, resultList.get(3), true);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(4);
      assertEvent(deliver, eventList.get(3), OrderDelivered.class);
    }

    @Test
    @DisplayName("Should fail to cancel an order that is already shipped")
    void cancelOrderAlreadyShipped() {
      Command place = buildPlaceOrderCommand("order-1");
      Command confirm = buildConfirmOrderCommand("order-1");
      Command ship = buildShipOrderCommand("order-1");
      Command cancel = buildCancelOrderCommand("order-1", "changed my mind");

      commands.pipeInput(place.getAggregateId(), place);
      commands.pipeInput(confirm.getAggregateId(), confirm);
      commands.pipeInput(ship.getAggregateId(), ship);
      commands.pipeInput(cancel.getAggregateId(), cancel);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(4);
      assertCommandResult(cancel, resultList.get(3), false);

      assertThat(readEventsFromStore(eventStore, "order-1")).hasSize(3);
    }

    @Test
    @DisplayName("Should fail to cancel an order that is already delivered")
    void cancelOrderAlreadyDelivered() {
      Command place = buildPlaceOrderCommand("order-1");
      Command confirm = buildConfirmOrderCommand("order-1");
      Command ship = buildShipOrderCommand("order-1");
      Command deliver = buildDeliverOrderCommand("order-1");
      Command cancel = buildCancelOrderCommand("order-1", "too late");

      commands.pipeInput(place.getAggregateId(), place);
      commands.pipeInput(confirm.getAggregateId(), confirm);
      commands.pipeInput(ship.getAggregateId(), ship);
      commands.pipeInput(deliver.getAggregateId(), deliver);
      commands.pipeInput(cancel.getAggregateId(), cancel);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(5);
      assertCommandResult(cancel, resultList.get(4), false);

      assertThat(readEventsFromStore(eventStore, "order-1")).hasSize(4);
    }

    @Test
    @DisplayName("Should cancel a placed order, remove aggregate, and reject subsequent commands")
    void cancelOrderAndRejectFollowUp() {
      Command place = buildPlaceOrderCommand("order-1");
      Command cancel = buildCancelOrderCommand("order-1", "out of stock");
      Command confirmAfterCancel = buildConfirmOrderCommand("order-1");

      commands.pipeInput(place.getAggregateId(), place);
      commands.pipeInput(cancel.getAggregateId(), cancel);
      commands.pipeInput(confirmAfterCancel.getAggregateId(), confirmAfterCancel);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(3);
      assertCommandResult(place, resultList.get(0), true);
      assertCommandResult(cancel, resultList.get(1), true);
      assertCommandResult(confirmAfterCancel, resultList.get(2), false);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(2);
      assertEvent(cancel, eventList.get(1), OrderCancelled.class);
      assertThat(((OrderCancelled) eventList.get(1).getPayload()).getReason()).isEqualTo("out of stock");
    }

    @Test
    @DisplayName("Should run the full happy-path lifecycle: place, confirm, ship, deliver")
    void fullLifecycle() {
      Command place = buildPlaceOrderCommand("order-1");
      Command confirm = buildConfirmOrderCommand("order-1");
      Command ship = buildShipOrderCommand("order-1");
      Command deliver = buildDeliverOrderCommand("order-1");

      List<Command> commandList = List.of(place, confirm, ship, deliver);
      commandList.forEach(cmd -> commands.pipeInput(cmd.getAggregateId(), cmd));

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(4);
      for (int i = 0; i < commandList.size(); i++) {
        assertCommandResult(commandList.get(i), resultList.get(i), true);
      }

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(4);
      assertEvent(place, eventList.get(0), OrderPlaced.class);
      assertEvent(confirm, eventList.get(1), OrderConfirmed.class);
      assertEvent(ship, eventList.get(2), OrderShipped.class);
      assertEvent(deliver, eventList.get(3), OrderDelivered.class);

      assertThat(readEventsFromStore(eventStore, "order-1")).hasSize(4);
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
      // Snapshot is triggered during loadAggregate at the start of the 4th command,
      // after 3 events are already in the store (3 % threshold(3) == 0)
      List<Command> commandList = List.of(
          buildPlaceOrderCommand("order-1"),
          buildConfirmOrderCommand("order-1"),
          buildShipOrderCommand("order-1"),   // 3rd event stored
          buildDeliverOrderCommand("order-1") // 4th command triggers snapshot
      );
      commandList.forEach(cmd -> commands.pipeInput(cmd.getAggregateId(), cmd));

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(4);
      for (int i = 0; i < commandList.size(); i++) {
        assertCommandResult(commandList.get(i), resultList.get(i), true);
      }

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(4);
      assertThat(readEventsFromStore(eventStore, "order-1")).hasSize(4);

      AggregateState snapshot = snapshotStore.get("order-1");
      assertThat(snapshot).isNotNull();
      assertSnapshot(eventList.get(2), snapshot, Order.class, 3);
      assertThat(((Order) snapshot.getPayload()).getId()).isEqualTo("order-1");
      assertThat(((Order) snapshot.getPayload()).getStatus()).isEqualTo("SHIPPED");
    }

    @Test
    @DisplayName("Should resume state from snapshot and correctly apply the next event")
    void resumeFromSnapshot() {
      Command place = buildPlaceOrderCommand("order-1");
      Command confirm = buildConfirmOrderCommand("order-1");
      Command ship = buildShipOrderCommand("order-1");
      Command deliver = buildDeliverOrderCommand("order-1");

      commands.pipeInput(place.getAggregateId(), place);
      commands.pipeInput(confirm.getAggregateId(), confirm);
      commands.pipeInput(ship.getAggregateId(), ship);
      commands.pipeInput(deliver.getAggregateId(), deliver);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(4);
      // If the snapshot at version 3 (status=SHIPPED) had NOT been resumed correctly,
      // this command would fail its "must be SHIPPED" validation in OrderCommandHandler.
      assertCommandResult(deliver, resultList.get(3), true);

      List<Event> eventList = events.readValuesToList();
      assertThat(eventList).hasSize(4);

      AggregateState snapshot = snapshotStore.get("order-1");
      assertThat(snapshot).isNotNull();
      assertSnapshot(eventList.get(2), snapshot, Order.class, 3);
      assertThat(((Order) snapshot.getPayload()).getStatus()).isEqualTo("SHIPPED");

      // 4th command: state rebuilt from snapshot(v3) + event 4, correctly producing OrderDelivered
      assertEvent(deliver, eventList.get(3), OrderDelivered.class);
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
          .registerHandler(new OrderEventUpcaster())
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
      Command place = buildPlaceOrderCommand("order-1");
      String expectedCustomer = ((PlaceOrder) place.getPayload()).getCustomer();
      commands.pipeInput(place.getAggregateId(), place);

      // Second command forces a replay of the stored OrderPlaced event through the upcaster chain
      Command confirm = buildConfirmOrderCommand("order-1");
      commands.pipeInput(confirm.getAggregateId(), confirm);

      List<Command> resultList = results.readValuesToList();
      assertThat(resultList).hasSize(2);
      assertCommandResult(place, resultList.get(0), true);
      // If the upcast chain throws (unresolved type, malformed JSON) or the resulting
      // event fails to deserialize into OrderPlaced, this command fails instead.
      assertCommandResult(confirm, resultList.get(1), true);

      // Confirms that replaying the upcasted OrderPlaced event still reconstructs valid
      // state - the synthetic fields the upcaster adds (shippingAddress, couponCode)
      // aren't part of OrderPlaced, so they're simply ignored on deserialization; what
      // matters here is that replay doesn't break and the payload comes back intact.
      List<Event> storedEvents = readEventsFromStore(eventStore, "order-1");
      assertThat(storedEvents).hasSize(2);
      assertThat(((OrderPlaced) storedEvents.get(0).getPayload()).getCustomer()).isEqualTo(expectedCustomer);
    }
  }
}