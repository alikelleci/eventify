package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.MessageIds;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.store.ReadOnlyEventStore;
import io.github.alikelleci.eventify.core.support.InMemoryStore;
import io.github.alikelleci.eventify.core.testdomain.order.Order;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderCancelled;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderConfirmed;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderPlaced;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderShipped;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEventSourcingHandler;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Aggregate replay")
class AggregateReplayerTest {

  private final InMemoryStore<Event> storedEvents = new InMemoryStore<>();
  private final ReadOnlyEventStore eventStore = ReadOnlyEventStore.of(storedEvents);
  private final AggregateReplayer replay = eventify().getAggregateReplayer();

  /** An event no event sourcing handler applies, e.g. one only an event handler reacts to. */
  public static class OrderViewed {
    @AggregateId
    String id;

    OrderViewed(String id) {
      this.id = id;
    }
  }

  @Test
  @DisplayName("Should apply the aggregate's events in order")
  void appliesTheEventsOfTheAggregateInOrder() {
    store(placed("order-1"));
    store(placed("order-10")); // sorts right before order-1@...
    store(confirmed("order-1"));
    store(placed("order-2"));  // sorts right after
    Event shipped = store(shipped("order-1"));

    AggregateReplayer.Result result = replayed("order-1", null, null);

    assertThat(order(result).getStatus()).isEqualTo("SHIPPED");
    assertThat(result.state().getVersion()).isEqualTo(3);
    assertThat(result.state().getEventId()).isEqualTo(shipped.getId());
    assertThat(result.replayed()).isEqualTo(3);
  }

  @Test
  @DisplayName("Should start after the starting state and continue its version")
  void startsAfterTheStartingStateAndContinuesItsVersion() {
    store(placed("order-1"));
    Event confirmed = store(confirmed("order-1"));
    store(shipped("order-1"));
    AggregateState snapshot = replayed("order-1", null, confirmed.getId()).state();

    AggregateReplayer.Result result = replayed("order-1", snapshot, null);

    assertThat(order(result).getStatus()).isEqualTo("SHIPPED");
    assertThat(result.state().getVersion()).isEqualTo(3);
    assertThat(result.replayed()).isEqualTo(1);
  }

  @Test
  @DisplayName("Should stop after the given event")
  void stopsAfterTheGivenEvent() {
    store(placed("order-1"));
    Event confirmed = store(confirmed("order-1"));
    store(shipped("order-1"));

    AggregateReplayer.Result result = replayed("order-1", null, confirmed.getId());

    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(2);
    assertThat(result.state().getEventId()).isEqualTo(confirmed.getId());
  }

  @Test
  @DisplayName("Should return the starting state when it is at the given event")
  void aStartingStateAtTheGivenEventIsTheAnswer() {
    store(placed("order-1"));
    Event confirmed = store(confirmed("order-1"));
    AggregateState snapshot = replayed("order-1", null, confirmed.getId()).state();

    AggregateReplayer.Result result = replayed("order-1", snapshot, confirmed.getId());

    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(2);
    assertThat(result.replayed()).isZero();
  }

  @Test
  @DisplayName("Should refuse a replay that would apply the wrong events")
  void refusesAReplayThatWouldApplyTheWrongEvents() {
    Event placed = store(placed("order-1"));
    Event confirmed = store(confirmed("order-1"));
    Event otherOrder = store(placed("order-2"));
    AggregateState atConfirmed = replayed("order-1", null, confirmed.getId()).state();

    // Before the starting state
    assertThatThrownBy(() -> replayed("order-1", atConfirmed, placed.getId()))
        .isInstanceOf(IllegalArgumentException.class);
    // Another aggregate's event: the range would cover the events in between
    assertThatThrownBy(() -> replayed("order-1", null, otherOrder.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  @DisplayName("Should not apply the events of an aggregate whose id starts with this id and '@'")
  void theEventsOfAnAggregateWhoseIdStartsWithThisIdAndAtAreNotApplied() {
    store(placed("ada"));
    store(placed("ada@example.com")); // key "ada@example.com@ULID": in the key range of "ada"
    store(placed("ada@-team"));       // "-" sorts before every ULID
    store(confirmed("ada"));
    List<String> seen = new ArrayList<>();

    AggregateReplayer.Result result = replayed("ada", null, null,
        (event, state, version) -> seen.add(event.getAggregateId()));

    assertThat(seen).containsExactly("ada", "ada");
    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(2);
    assertThat(replayed("ada@example.com", null, null).state().getVersion()).isEqualTo(1);
  }

  @Test
  @DisplayName("Should count events without a handler, and leave the state as it was")
  void eventsWithoutAHandlerAreCountedAndLeaveTheStateAsItWas() {
    store(placed("order-1"));
    store(Event.builder().payload(new OrderViewed("order-1")).build());
    store(confirmed("order-1"));
    List<String> seen = new ArrayList<>();

    AggregateReplayer.Result result = replayed("order-1", null, null,
        (event, state, version) -> seen.add(event.getType() + "@v" + version));

    assertThat(seen).containsExactly("OrderPlaced@v0", "OrderViewed@v1", "OrderConfirmed@v2");
    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(3);
    assertThat(result.replayed()).isEqualTo(3);
  }

  @Test
  @DisplayName("Should move the state on to the last event, also when that event has no handler")
  void theStateMovesOnToAnEventWithoutAHandler() {
    Event placed = store(placed("order-1"));
    Event viewed = store(Event.builder().payload(new OrderViewed("order-1")).build());
    Order afterPlaced = order(replayed("order-1", null, placed.getId()));

    AggregateReplayer.Result result = replayed("order-1", null, null);

    assertThat(order(result)).isEqualTo(afterPlaced);
    assertThat(result.state().getEventId()).isEqualTo(viewed.getId());
    assertThat(result.state().getTimestamp()).isEqualTo(viewed.getTimestamp());
    assertThat(result.state().getVersion()).isEqualTo(2);
  }

  @Test
  @DisplayName("Should have no state without events or after the aggregate was removed")
  void noEventsOrARemovedAggregateIsNoState() {
    assertThat(replayed("order-1", null, null)).isEqualTo(new AggregateReplayer.Result(null, 0));

    store(placed("order-1"));
    store(Event.builder().payload(OrderCancelled.builder().id("order-1").build()).build());
    AggregateReplayer.Result result = replayed("order-1", null, null);

    assertThat(result.state()).isNull();
    assertThat(result.replayed()).isEqualTo(2);
  }

  /** Its class was renamed or removed since it was stored: read back, its payload is null. */
  @Test
  @DisplayName("Should refuse to replay a stored event whose class no longer exists, and say which one")
  void aStoredEventWhoseClassNoLongerExistsIsRefusedClearly() {
    store(placed("order-1"));
    String key = MessageIds.createCompoundKey("order-1");
    String json = "{\"id\":\"" + key + "\",\"type\":\"OrderArchived\",\"aggregateId\":\"order-1\",\"revision\":1,"
        + "\"metadata\":{},\"payload\":{\"@class\":\"com.acme.OrderArchived\",\"id\":\"order-1\"}}";
    store(new JsonDeserializer<>(Event.class).deserialize("events", json.getBytes(StandardCharsets.UTF_8)));

    assertThatThrownBy(() -> replayed("order-1", null, null))
        .isInstanceOf(IllegalStateException.class)
        .hasMessageContaining(key)
        .hasMessageContaining("OrderArchived")
        .hasMessageContaining("upcaster");
  }

  private Event store(Event event) {
    storedEvents.put(event.getId(), event);
    return event;
  }

  private static Order order(AggregateReplayer.Result result) {
    return (Order) result.state().getPayload();
  }

  private static Event placed(String id) {
    return Event.builder().payload(OrderPlaced.builder().id(id).customer("Ada").build()).build();
  }

  private static Event confirmed(String id) {
    return Event.builder().payload(OrderConfirmed.builder().id(id).build()).build();
  }

  private static Event shipped(String id) {
    return Event.builder().payload(OrderShipped.builder().id(id).trackingNumber("T-1").build()).build();
  }

  private static Eventify eventify() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "replay-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return Eventify.builder().streamsConfig(properties).registerHandler(new OrderEventSourcingHandler()).build();
  }

  /** The aggregate's stored events after {@code start}, up to and including {@code untilEventId}, applied to {@code start}. */
  private AggregateReplayer.Result replayed(String aggregateId, AggregateState start, String untilEventId) {
    return replayed(aggregateId, start, untilEventId, null);
  }

  private AggregateReplayer.Result replayed(String aggregateId, AggregateState start, String untilEventId,
                                            AggregateReplayer.Listener listener) {
    try (ReadOnlyEventStore.Events toApply = eventStore.events(aggregateId, start != null ? start.getEventId() : null, untilEventId)) {
      return replay.replay(toApply, start, listener);
    }
  }
}
