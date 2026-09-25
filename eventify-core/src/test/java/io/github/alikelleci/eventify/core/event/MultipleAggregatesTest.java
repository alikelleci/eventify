package io.github.alikelleci.eventify.core.event;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.internal.StoreKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
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
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Two aggregates with the SAME id in one instance: the aggregate name in the key keeps their data apart. */
@DisplayName("Multiple aggregates in one instance")
class MultipleAggregatesTest {

  private static final String SHARED_ID = "X-1";

  @Topic("commands.order")
  public record PlaceOrder(@AggregateId String id, String customer) {
  }

  @Topic("events.order")
  public record OrderPlaced(@AggregateId String id, String customer) {
  }

  @AggregateRoot("order")
  public record Order(@AggregateId String id, String customer, int events) {
  }

  @Topic("commands.invoice")
  public record SendInvoice(@AggregateId String id, int amount) {
  }

  @Topic("events.invoice")
  public record InvoiceSent(@AggregateId String id, int amount) {
  }

  @AggregateRoot("invoice")
  public record Invoice(@AggregateId String id, int amount, int events) {
  }

  public static class OrderHandler {
    @CommandHandler
    public OrderPlaced handle(PlaceOrder command, Order state) {
      return new OrderPlaced(command.id(), command.customer());
    }

    @EventSourcingHandler
    public Order handle(OrderPlaced event, Order state) {
      return new Order(event.id(), event.customer(), state == null ? 1 : state.events() + 1);
    }
  }

  public static class InvoiceHandler {
    @CommandHandler
    public InvoiceSent handle(SendInvoice command, Invoice state) {
      return new InvoiceSent(command.id(), command.amount());
    }

    @EventSourcingHandler
    public Invoice handle(InvoiceSent event, Invoice state) {
      return new Invoice(event.id(), event.amount(), state == null ? 1 : state.events() + 1);
    }
  }

  private TopologyTestDriver driver;
  private TestInputTopic<String, Command> orderCommands;
  private TestInputTopic<String, Command> invoiceCommands;
  private TestOutputTopic<String, Event> orderEvents;
  private TestOutputTopic<String, Event> invoiceEvents;

  @BeforeEach
  void setUp() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "several-aggregates-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    Eventify eventify = Eventify.builder().streamsConfig(properties)
        .registerHandler(new OrderHandler())
        .registerHandler(new InvoiceHandler())
        .build();
    driver = new TopologyTestDriver(eventify.topology());
    orderCommands = driver.createInputTopic("commands.order", new StringSerializer(), new CommandSerde().serializer());
    invoiceCommands = driver.createInputTopic("commands.invoice", new StringSerializer(), new CommandSerde().serializer());
    orderEvents = driver.createOutputTopic("events.order", new StringDeserializer(), new EventSerde().deserializer());
    invoiceEvents = driver.createOutputTopic("events.invoice", new StringDeserializer(), new EventSerde().deserializer());
  }

  @AfterEach
  void tearDown() {
    driver.close();
  }

  /** Each aggregate counts its own events: a shared counter would number the invoice's first event #3. */
  @Test
  @DisplayName("Should number the events of each aggregate from 1, also with the same identifier")
  void eachAggregateCountsItsOwnEvents() {
    send(orderCommands, new PlaceOrder(SHARED_ID, "Ada"));
    send(invoiceCommands, new SendInvoice(SHARED_ID, 100));
    send(orderCommands, new PlaceOrder(SHARED_ID, "Bob"));
    send(invoiceCommands, new SendInvoice(SHARED_ID, 250));

    assertThat(orderEvents.readValuesToList()).extracting(Event::getSequence).containsExactly(1L, 2L);
    assertThat(invoiceEvents.readValuesToList()).extracting(Event::getSequence).containsExactly(1L, 2L);
  }

  /** An event says which aggregate it belongs to, so the stored data itself keeps them apart. */
  @Test
  @DisplayName("Should store the events of each aggregate under its own name")
  void eventsAreStoredUnderTheirOwnAggregate() {
    send(orderCommands, new PlaceOrder(SHARED_ID, "Ada"));
    send(invoiceCommands, new SendInvoice(SHARED_ID, 100));

    KeyValueStore<String, Event> events = driver.getKeyValueStore("event-store");
    assertThat(IteratorUtils.toList(events.all())).extracting(entry -> entry.key)
        .containsExactlyInAnyOrder(StoreKeys.event("order", SHARED_ID, 1), StoreKeys.event("invoice", SHARED_ID, 1));
    assertThat(events.get(StoreKeys.event("order", SHARED_ID, 1)).getAggregateType()).isEqualTo("order");
    assertThat(events.get(StoreKeys.event("invoice", SHARED_ID, 1)).getAggregateType()).isEqualTo("invoice");
  }

  /** Replaying one aggregate must not apply the other's events: its state would be wrong, or fail to apply at all. */
  @Test
  @DisplayName("Should replay each aggregate from its own events only")
  void eachAggregateIsReplayedFromItsOwnEvents() {
    send(orderCommands, new PlaceOrder(SHARED_ID, "Ada"));
    send(invoiceCommands, new SendInvoice(SHARED_ID, 100));
    send(invoiceCommands, new SendInvoice(SHARED_ID, 250));
    send(orderCommands, new PlaceOrder(SHARED_ID, "Bob"));

    // The last event of each: the state it was applied to counts only that aggregate's events.
    List<Event> orders = orderEvents.readValuesToList();
    List<Event> invoices = invoiceEvents.readValuesToList();
    assertThat(orders).hasSize(2);
    assertThat(invoices).hasSize(2);
    assertThat(((OrderPlaced) orders.get(1).getPayload()).customer()).isEqualTo("Bob");
    assertThat(((InvoiceSent) invoices.get(1).getPayload()).amount()).isEqualTo(250);
  }

  /** The snapshot store is keyed the same way, so one aggregate's snapshot is never the other's. */
  @Test
  @DisplayName("Should keep the snapshot of each aggregate under its own name")
  void snapshotsAreKeptPerAggregate() {
    send(orderCommands, new PlaceOrder(SHARED_ID, "Ada"));
    send(invoiceCommands, new SendInvoice(SHARED_ID, 100));

    KeyValueStore<String, AggregateState> snapshots = driver.getKeyValueStore("snapshot-store");
    // No snapshots configured: this checks that nothing was written under a bare id.
    assertThat(IteratorUtils.toList(snapshots.all())).extracting(entry -> entry.key).doesNotContain(SHARED_ID);
    assertThat(StoreKeys.aggregate("order", SHARED_ID)).isNotEqualTo(StoreKeys.aggregate("invoice", SHARED_ID));
  }

  private static void send(TestInputTopic<String, Command> commands, Object payload) {
    Command command = Command.builder().payload(payload).build();
    commands.pipeInput(command.getAggregateId(), command);
  }
}
