package io.github.alikelleci.eventify.core.messaging.eventsourcing;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.order.Order;
import io.github.alikelleci.eventify.core.order.OrderEvent.OrderCancelled;
import io.github.alikelleci.eventify.core.order.OrderEvent.OrderConfirmed;
import io.github.alikelleci.eventify.core.order.OrderEvent.OrderPlaced;
import io.github.alikelleci.eventify.core.order.OrderEvent.OrderShipped;
import io.github.alikelleci.eventify.core.order.OrderEventSourcingHandler;
import io.github.alikelleci.eventify.core.support.InMemoryStore;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class AggregateReplayTest {

  private final InMemoryStore<Event> eventStore = new InMemoryStore<>();
  private final AggregateReplay replay = new AggregateReplay(eventify().getEventSourcingHandlers());

  /** An event no event sourcing handler applies, e.g. one only an event handler reacts to. */
  public static class OrderViewed {
    @AggregateId
    String id;

    OrderViewed(String id) {
      this.id = id;
    }
  }

  @Test
  void appliesTheEventsOfTheAggregateInOrder() {
    store(placed("order-1"));
    store(placed("order-10")); // sorts right before order-1@...
    store(confirmed("order-1"));
    store(placed("order-2"));  // sorts right after
    Event shipped = store(shipped("order-1"));

    AggregateReplay.Result result = replay.replay(eventStore, "order-1", null, null);

    assertThat(order(result).getStatus()).isEqualTo("SHIPPED");
    assertThat(result.state().getVersion()).isEqualTo(3);
    assertThat(result.state().getEventId()).isEqualTo(shipped.getId());
    assertThat(result.applied()).isEqualTo(3);
  }

  @Test
  void startsAfterTheStartingStateAndContinuesItsVersion() {
    store(placed("order-1"));
    Event confirmed = store(confirmed("order-1"));
    store(shipped("order-1"));
    AggregateState snapshot = replay.replay(eventStore, "order-1", null, confirmed.getId()).state();

    AggregateReplay.Result result = replay.replay(eventStore, "order-1", snapshot, null);

    assertThat(order(result).getStatus()).isEqualTo("SHIPPED");
    assertThat(result.state().getVersion()).isEqualTo(3);
    assertThat(result.applied()).isEqualTo(1);
  }

  @Test
  void stopsAfterTheGivenEvent() {
    store(placed("order-1"));
    Event confirmed = store(confirmed("order-1"));
    store(shipped("order-1"));

    AggregateReplay.Result result = replay.replay(eventStore, "order-1", null, confirmed.getId());

    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(2);
    assertThat(result.state().getEventId()).isEqualTo(confirmed.getId());
  }

  @Test
  void aStartingStateAtTheGivenEventIsTheAnswer() {
    store(placed("order-1"));
    Event confirmed = store(confirmed("order-1"));
    AggregateState snapshot = replay.replay(eventStore, "order-1", null, confirmed.getId()).state();

    AggregateReplay.Result result = replay.replay(eventStore, "order-1", snapshot, confirmed.getId());

    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(2);
    assertThat(result.applied()).isZero();
  }

  @Test
  void refusesAReplayThatWouldApplyTheWrongEvents() {
    Event placed = store(placed("order-1"));
    Event confirmed = store(confirmed("order-1"));
    Event otherOrder = store(placed("order-2"));
    AggregateState atConfirmed = replay.replay(eventStore, "order-1", null, confirmed.getId()).state();

    // Before the starting state
    assertThatThrownBy(() -> replay.replay(eventStore, "order-1", atConfirmed, placed.getId()))
        .isInstanceOf(IllegalArgumentException.class);
    // Another aggregate's event: the range would cover the events in between
    assertThatThrownBy(() -> replay.replay(eventStore, "order-1", null, otherOrder.getId()))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void theEventsOfAnAggregateWhoseIdStartsWithThisIdAndAtAreNotApplied() {
    store(placed("ada"));
    store(placed("ada@example.com")); // key "ada@example.com@ULID": in the key range of "ada"
    store(placed("ada@-team"));       // "-" sorts before every ULID
    store(confirmed("ada"));
    List<String> seen = new ArrayList<>();

    AggregateReplay.Result result = replay.replay(eventStore, "ada", null, null,
        (event, state, version) -> seen.add(event.getAggregateId()));

    assertThat(seen).containsExactly("ada", "ada");
    assertThat(order(result).getStatus()).isEqualTo("CONFIRMED");
    assertThat(result.state().getVersion()).isEqualTo(2);
    assertThat(replay.replay(eventStore, "ada@example.com", null, null).state().getVersion()).isEqualTo(1);
  }

  @Test
  void eventsWithoutAHandlerAreSeenButNotCounted() {
    store(placed("order-1"));
    store(Event.builder().payload(new OrderViewed("order-1")).build());
    store(confirmed("order-1"));
    List<String> seen = new ArrayList<>();

    AggregateReplay.Result result = replay.replay(eventStore, "order-1", null, null,
        (event, state, version) -> seen.add(event.getType() + "@v" + version));

    assertThat(seen).containsExactly("OrderPlaced@v0", "OrderViewed@v1", "OrderConfirmed@v1");
    assertThat(result.state().getVersion()).isEqualTo(2);
    assertThat(result.applied()).isEqualTo(2);
  }

  @Test
  void noEventsOrARemovedAggregateIsNoState() {
    assertThat(replay.replay(eventStore, "order-1", null, null)).isEqualTo(new AggregateReplay.Result(null, 0));

    store(placed("order-1"));
    store(Event.builder().payload(OrderCancelled.builder().id("order-1").build()).build());
    AggregateReplay.Result result = replay.replay(eventStore, "order-1", null, null);

    assertThat(result.state()).isNull();
    assertThat(result.applied()).isEqualTo(2);
  }

  private Event store(Event event) {
    eventStore.put(event.getId(), event);
    return event;
  }

  private static Order order(AggregateReplay.Result result) {
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
}
