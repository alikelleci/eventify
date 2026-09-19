package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.exception.EventReplayException;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import io.github.alikelleci.eventify.core.serialization.JsonDeserializer;
import io.github.alikelleci.eventify.core.store.ReadOnlyEventStore;
import io.github.alikelleci.eventify.core.store.StoreKeys;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Aggregate replay")
class AggregateReplayerTest {

  private final InMemoryStore<Event> storedEvents = new InMemoryStore<>();
  private final ReadOnlyEventStore eventStore = ReadOnlyEventStore.of(storedEvents);
  private final AggregateReplayer replay = eventify().getAggregateReplayer();
  private final Map<String, Long> lastSequences = new HashMap<>();

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
    store(placed("order-10")); // sorts right after order-1@...
    store(confirmed("order-1"));
    store(placed("order-0"));  // sorts right before
    store(shipped("order-1"));

    AggregateReplayer.Result result = replayed("order-1", null, null);

    assertThat(order(result).getStatus()).isEqualTo("SHIPPED");
    assertThat(result.state().getVersion()).isEqualTo(3);
    assertThat(result.replayed()).isEqualTo(3);
  }

  @Test
  @DisplayName("Should start after the starting state and continue its version")
  void startsAfterTheStartingStateAndContinuesItsVersion() {
    store(placed("order-1"));
    store(confirmed("order-1"));
    store(shipped("order-1"));
    AggregateState snapshot = replayed("order-1", null, 2L).state();

    AggregateReplayer.Result result = replayed("order-1", snapshot, null);

    assertThat(order(result).getStatus()).isEqualTo("SHIPPED");
    assertThat(result.state().getVersion()).isEqualTo(3);
    assertThat(result.replayed()).isEqualTo(1);
  }

  @Test
  @DisplayName("Should stop after the given sequence")
  void stopsAfterTheGivenSequence() {
    store(placed("order-1"));
    store(confirmed("order-1"));
    store(shipped("order-1"));

    AggregateReplayer.Result result = replayed("order-1", null, 2L);

    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(2);
  }

  @Test
  @DisplayName("Should return the starting state when it is at the given sequence")
  void aStartingStateAtTheGivenSequenceIsTheAnswer() {
    store(placed("order-1"));
    store(confirmed("order-1"));
    AggregateState snapshot = replayed("order-1", null, 2L).state();

    AggregateReplayer.Result result = replayed("order-1", snapshot, 2L);

    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(2);
    assertThat(result.replayed()).isZero();
  }

  @Test
  @DisplayName("Should refuse a replay with a missing event")
  void refusesAReplayWithAMissingEvent() {
    store(placed("order-1"));
    store(shipped("order-1"), 3); // 2 is missing

    assertThatThrownBy(() -> replayed("order-1", null, null))
        .isInstanceOf(EventReplayException.class)
        .hasMessageContaining("expected #2, found #3");
  }

  /** E.g. an event stored before events had a sequence, or one written to the store by hand. */
  @Test
  @DisplayName("Should refuse a replay with an event without a sequence")
  void refusesAReplayWithAnEventWithoutASequence() {
    store(placed("order-1"));
    storedEvents.put(StoreKeys.of("order-1", 2), stored("{\"id\":\"old-1\",\"type\":\"OrderConfirmed\",\"aggregateId\":\"order-1\",\"revision\":1,"
        + "\"metadata\":{},\"payload\":{\"@class\":\"" + OrderConfirmed.class.getName() + "\",\"id\":\"order-1\"}}")); // no sequence

    assertThatThrownBy(() -> replayed("order-1", null, null))
        .isInstanceOf(EventReplayException.class)
        .hasMessageContaining("expected #2, found #0");
  }

  /** E.g. an event copied to another key: its sequence no longer matches its place. */
  @Test
  @DisplayName("Should refuse a replay with an event in the wrong place")
  void refusesAReplayWithAnEventInTheWrongPlace() {
    Event placed = store(placed("order-1"));
    storedEvents.put(StoreKeys.of("order-1", 2), placed);

    assertThatThrownBy(() -> replayed("order-1", null, null))
        .isInstanceOf(EventReplayException.class)
        .hasMessageContaining("expected #2, found #1");
  }

  @Test
  @DisplayName("Should not apply the events of an aggregate whose id starts with this id and '@'")
  void theEventsOfAnAggregateWhoseIdStartsWithThisIdAndAtAreNotApplied() {
    store(placed("ada"));
    store(placed("ada@1"));           // key "ada@1@000…1": in the key range of "ada"
    store(placed("ada@example.com")); // after the key range of "ada"
    store(confirmed("ada"));
    List<String> seen = new ArrayList<>();

    AggregateReplayer.Result result = replayed("ada", null, null,
        (event, state) -> seen.add(event.getAggregateId()));

    assertThat(seen).containsExactly("ada", "ada");
    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(2);
    assertThat(replayed("ada@1", null, null).state().getVersion()).isEqualTo(1);
  }

  @Test
  @DisplayName("Should count events without a handler, and leave the state as it was")
  void eventsWithoutAHandlerAreCountedAndLeaveTheStateAsItWas() {
    store(placed("order-1"));
    store(new OrderViewed("order-1"));
    store(confirmed("order-1"));
    List<String> seen = new ArrayList<>();

    AggregateReplayer.Result result = replayed("order-1", null, null,
        (event, state) -> seen.add(event.getType() + "@v" + (state != null ? state.getVersion() : 0)));

    assertThat(seen).containsExactly("OrderPlaced@v0", "OrderViewed@v1", "OrderConfirmed@v2");
    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(3);
    assertThat(result.replayed()).isEqualTo(3);
  }

  @Test
  @DisplayName("Should move the state on to the last event, also when that event has no handler")
  void theStateMovesOnToAnEventWithoutAHandler() {
    store(placed("order-1"));
    Event viewed = store(new OrderViewed("order-1"));
    Order afterPlaced = order(replayed("order-1", null, 1L));

    AggregateReplayer.Result result = replayed("order-1", null, null);

    assertThat(order(result)).isEqualTo(afterPlaced);
    assertThat(result.state().getTimestamp()).isEqualTo(viewed.getTimestamp());
    assertThat(result.state().getVersion()).isEqualTo(2);
  }

  @Test
  @DisplayName("Should have no state without events or after the aggregate was removed")
  void noEventsOrARemovedAggregateIsNoState() {
    assertThat(replayed("order-1", null, null)).isEqualTo(new AggregateReplayer.Result(null, 0));

    store(placed("order-1"));
    store(OrderCancelled.builder().id("order-1").build());
    AggregateReplayer.Result result = replayed("order-1", null, null);

    assertThat(result.state()).isNull();
    assertThat(result.replayed()).isEqualTo(2);
  }

  /** Its class was renamed or removed since it was stored: read back, its payload is null. */
  @Test
  @DisplayName("Should refuse to replay a stored event whose class no longer exists, and say which one")
  void aStoredEventWhoseClassNoLongerExistsIsRefusedClearly() {
    store(placed("order-1"));
    String id = "archived-1";
    String json = "{\"id\":\"" + id + "\",\"type\":\"OrderArchived\",\"aggregateId\":\"order-1\",\"revision\":1,\"sequence\":2,"
        + "\"metadata\":{},\"payload\":{\"@class\":\"com.acme.OrderArchived\",\"id\":\"order-1\"}}";
    storedEvents.put(StoreKeys.of("order-1", 2), stored(json));

    assertThatThrownBy(() -> replayed("order-1", null, null))
        .isInstanceOf(EventReplayException.class)
        .hasMessageContaining(id)
        .hasMessageContaining("OrderArchived")
        .hasMessageContaining("upcaster");
  }

  /** Stores the payload as its aggregate's next event. */
  private Event store(Object payload) {
    return store(payload, lastSequences.merge(AggregateIdResolver.getAggregateId(payload), 1L, Long::sum));
  }

  /** Stores the payload as the event with this sequence, also when that is not the aggregate's next one. */
  private Event store(Object payload, long sequence) {
    Event event = Event.builder().payload(payload).sequence(sequence).build();
    storedEvents.put(StoreKeys.of(event.getAggregateId(), sequence), event);
    return event;
  }

  /** An event as it comes out of the store: read from JSON, so also one no builder would make. */
  private static Event stored(String json) {
    return new JsonDeserializer<>(Event.class).deserialize("events", json.getBytes(StandardCharsets.UTF_8));
  }

  private static Order order(AggregateReplayer.Result result) {
    return (Order) result.state().getPayload();
  }

  private static OrderPlaced placed(String id) {
    return OrderPlaced.builder().id(id).customer("Ada").build();
  }

  private static OrderConfirmed confirmed(String id) {
    return OrderConfirmed.builder().id(id).build();
  }

  private static OrderShipped shipped(String id) {
    return OrderShipped.builder().id(id).trackingNumber("T-1").build();
  }

  private static Eventify eventify() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "replay-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return Eventify.builder().streamsConfig(properties).registerHandler(new OrderEventSourcingHandler()).build();
  }

  /** The aggregate's stored events after {@code start}, up to and including {@code untilSequence} (all when {@code null}), applied to {@code start}. */
  private AggregateReplayer.Result replayed(String aggregateId, AggregateState start, Long untilSequence) {
    return replayed(aggregateId, start, untilSequence, null);
  }

  private AggregateReplayer.Result replayed(String aggregateId, AggregateState start, Long untilSequence,
                                            AggregateReplayer.Listener listener) {
    try (ReadOnlyEventStore.Events toApply = eventStore.events(aggregateId, start != null ? start.getVersion() + 1 : 1,
        untilSequence != null ? untilSequence : Long.MAX_VALUE)) {
      return replay.replay(toApply, start, listener);
    }
  }
}
