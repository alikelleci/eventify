package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.exception.EventReplayException;
import io.github.alikelleci.eventify.core.aggregate.exception.SnapshotOutdatedException;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.internal.ApplyEventMethod;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.store.EventStore;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import io.github.alikelleci.eventify.core.support.InMemoryStore;
import io.github.alikelleci.eventify.core.testdomain.order.Order;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderCancelled;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderConfirmed;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderPlaced;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEvent.OrderShipped;
import io.github.alikelleci.eventify.core.testdomain.order.OrderEventSourcingHandler;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Aggregate repository replay")
class AggregateRepositoryTest {

  private final InMemoryStore<Event> storedEvents = new InMemoryStore<>();
  private final InMemoryStore<AggregateState> storedSnapshots = new InMemoryStore<>();
  private final Eventify eventify = eventify();
  private final AggregateRepository repository = new AggregateRepository(new EventStore(storedEvents), new SnapshotStore(storedSnapshots),
      eventify.getHandlers().eventSourcingHandlers(), new AggregateDefinitions(List.of(Order.class)));

  @Test
  @DisplayName("Should replay stored events in sequence order")
  void replaysStoredEvents() {
    store(placed("order-1"), 1);
    store(confirmed("order-1"), 2);
    store(shipped("order-1"), 3);

    AggregateState state = repository.replay("order", "order-1");

    assertThat(state.getPayload()).isInstanceOf(Order.class);
    assertThat(((Order) state.getPayload()).getStatus()).isEqualTo("SHIPPED");
    assertThat(state.getVersion()).isEqualTo(3);
  }

  @Test
  @DisplayName("Should refuse an aggregate type this repository does not handle")
  void rejectsUnknownAggregateType() {
    assertThatThrownBy(() -> repository.replay("odrer", "order-1"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("odrer");
  }

  @Test
  @DisplayName("Should reject a gap before command processing can use the state")
  void rejectsAGap() {
    store(placed("order-1"), 1);
    store(shipped("order-1"), 3);

    assertThatThrownBy(() -> repository.replay("order", "order-1"))
        .isInstanceOf(EventReplayException.class)
        .hasMessageContaining("expected #2, found #3");
  }

  @Test
  @DisplayName("Should reject an event whose envelope belongs to another aggregate")
  void rejectsAnEventForAnotherAggregate() {
    Event copiedUnderTheWrongKey = event(placed("another-order"), 1);
    storedEvents.put(StoreKeys.of("order", "order-1", 1), copiedUnderTheWrongKey);

    assertThatThrownBy(() -> repository.replay("order", "order-1"))
        .isInstanceOf(EventReplayException.class)
        .hasMessageContaining("belongs to order another-order", "read as order order-1");
  }

  @Test
  @DisplayName("Should reject an apply handler that returns another aggregate type")
  void rejectsAnApplyHandlerThatReturnsAnotherAggregateType() {
    ApplyEventMethod wrongHandler = new ApplyEventMethod(null, null) {
      @Override
      public AggregateState handle(Event event, AggregateState state) {
        return AggregateState.after(event, new OtherAggregate(event.getAggregateId()));
      }
    };
    AggregateRepository repository = new AggregateRepository(new EventStore(storedEvents), new SnapshotStore(storedSnapshots),
        Map.of(Viewed.class, wrongHandler), new AggregateDefinitions(List.of(Order.class)));

    assertThatThrownBy(() -> repository.applyEvents("order", AggregateState.empty("order-1"), List.of(event(new Viewed("order-1"), 1))))
        .isInstanceOf(EventReplayException.class)
        .hasMessageContaining("requires " + Order.class.getName());
  }

  @Test
  @DisplayName("Should retain the version when an event removes the aggregate payload")
  void retainsVersionAfterRemoval() {
    store(placed("order-1"), 1);
    store(OrderCancelled.builder().id("order-1").build(), 2);

    AggregateState state = repository.replay("order", "order-1");

    assertThat(state.getPayload()).isNull();
    assertThat(state.getVersion()).isEqualTo(2);
  }

  @Test
  @DisplayName("Should use a null-payload snapshot of a removed aggregate as the next replay start")
  void usesDeletedSnapshot() {
    AggregateState removed = AggregateState.after(event(OrderCancelled.builder().id("order-1").build(), 2), null);
    storedSnapshots.put(StoreKeys.snapshot("order", "order-1"), removed);
    store(placed("order-1"), 3);

    AggregateState state = repository.replay("order", "order-1");

    assertThat(((Order) state.getPayload()).getStatus()).isEqualTo("PLACED");
    assertThat(state.getVersion()).isEqualTo(3);
  }

  @Test
  @DisplayName("Should apply produced events in memory before they are saved")
  void appliesProducedEventsInMemory() {
    AggregateState before = AggregateState.empty("order-1");
    Event placed = event(placed("order-1"), 1);

    AggregateState after = repository.applyEvents(before, List.of(placed));

    assertThat(((Order) after.getPayload()).getStatus()).isEqualTo("PLACED");
    assertThat(after.getVersion()).isOne();
  }

  @Test
  void startsFromSnapshotAndStopsAtRequestedSequence() {
    store(placed("order-1"), 1);
    store(confirmed("order-1"), 2);
    store(shipped("order-1"), 3);
    AggregateState snapshot = repository.replay("order", "order-1", 2, null);
    assertThat(((Order) snapshot.getPayload()).getStatus()).isEqualTo("CONFIRMED");
    assertThat(snapshot.getVersion()).isEqualTo(2);
    storedSnapshots.put(StoreKeys.snapshot("order", "order-1"), snapshot);
    storedEvents.delete(StoreKeys.of("order", "order-1", 1));

    assertThat(repository.replay("order", "order-1", 2, null)).isSameAs(snapshot);
    AggregateState current = repository.replay("order", "order-1");
    assertThat(current.getVersion()).isEqualTo(3);
    assertThat(((Order) current.getPayload()).getStatus()).isEqualTo("SHIPPED");
  }

  @ParameterizedTest
  @ValueSource(longs = {0, 1})
  void rejectsMissingOrRepeatedSequence(long sequence) throws Exception {
    store(placed("order-1"), 1);
    ObjectNode json = eventify.getObjectMapper().valueToTree(event(confirmed("order-1"), 2));
    json.put("sequence", sequence); // Persisted data can predate sequences, or have been copied to the wrong key.
    storedEvents.put(StoreKeys.of("order", "order-1", 2), eventify.getObjectMapper().treeToValue(json, Event.class));

    assertThatThrownBy(() -> repository.replay("order", "order-1"))
        .isInstanceOf(EventReplayException.class)
        .hasMessageContaining("expected #2, found #" + sequence);
  }

  @Test
  void refusesUnreadableEventInsteadOfSkippingIt() throws Exception {
    store(placed("order-1"), 1);
    ObjectNode json = eventify.getObjectMapper().valueToTree(event(confirmed("order-1"), 2));
    json.put("id", "archived-event");
    ((ObjectNode) json.get("payload")).put("@class", "com.acme.RemovedEvent");
    storedEvents.put(StoreKeys.of("order", "order-1", 2), eventify.getObjectMapper().treeToValue(json, Event.class));

    assertThatThrownBy(() -> repository.replay("order", "order-1"))
        .isInstanceOf(EventReplayException.class)
        .hasMessageContaining("archived-event", "upcaster");
  }

  public record Viewed(@AggregateId String id) {}

  @AggregateRoot("other")
  public record OtherAggregate(@AggregateId String id) {}

  @Test
  void advancesVersionAndNotifiesListenerForEventsWithoutHandler() {
    store(placed("order-1"), 1);
    Event viewed = event(new Viewed("order-1"), 2);
    storedEvents.put(StoreKeys.of("order", "order-1", 2), viewed);
    List<Long> previousVersions = new ArrayList<>();

    AggregateState state = repository.replay("order", "order-1", 2,
        (event, before) -> previousVersions.add(before.getVersion()));

    assertThat(previousVersions).containsExactly(0L, 1L);
    assertThat(state.getVersion()).isEqualTo(2);
    assertThat(state.getTimestamp()).isEqualTo(viewed.getTimestamp());
    assertThat(((Order) state.getPayload()).getStatus()).isEqualTo("PLACED");
  }

  @Test
  void emptyAndRemovedAggregatesKeepTheirVersionWithoutPayload() {
    AggregateState empty = repository.replay("order", "order-1");
    assertThat(empty.getVersion()).isZero();
    assertThat(empty.getAggregateId()).isEqualTo("order-1");
    assertThat(empty.getPayload()).isNull();

    store(placed("order-1"), 1);
    store(OrderCancelled.builder().id("order-1").build(), 2);
    store(new Viewed("order-1"), 3);
    AggregateState removed = repository.replay("order", "order-1");
    assertThat(removed.getVersion()).isEqualTo(3);
    assertThat(removed.getPayload()).isNull();
  }

  @Test
  void replayDoesNotIncludeIdsWithTheSamePrefix() {
    store(placed("ada"), 1);
    store(placed("ada@1"), 1);
    store(placed("ada@example.com"), 1);
    store(confirmed("ada"), 2);
    List<String> ids = new ArrayList<>();
    AggregateState state = repository.replay("order", "ada", 2, (event, before) -> ids.add(event.getAggregateId()));
    assertThat(ids).containsExactly("ada", "ada");
    assertThat(state.getVersion()).isEqualTo(2);
    assertThat(repository.replay("order", "ada@1").getVersion()).isOne();
  }

  @Test
  void outdatedSnapshotWithNoHistoryCannotRestartAtVersionZero() throws Exception {
    store(placed("order-1"), 1);
    store(confirmed("order-1"), 2);
    ObjectNode json = eventify.getObjectMapper().valueToTree(repository.replay("order", "order-1"));
    json.put("revision", 999);
    storedSnapshots.put(StoreKeys.snapshot("order", "order-1"), eventify.getObjectMapper().treeToValue(json, AggregateState.class));
    storedEvents.delete(StoreKeys.of("order", "order-1", 1));
    storedEvents.delete(StoreKeys.of("order", "order-1", 2));

    assertThat(repository.allEventsStored("order", "order-1")).isFalse();
    assertThatThrownBy(() -> repository.replay("order", "order-1"))
        .isInstanceOf(SnapshotOutdatedException.class);
    assertThat(repository.replay("order", "order-1", 2, null)).isNull(); // A reader gets unknown, not an empty aggregate.
    assertThat(repository.allEventsStored("order", "never-created")).isTrue();
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void closesStoreIteratorsAlsoWhenReplayFails(boolean corrupt) {
    ClosingStore events = new ClosingStore();
    events.put(StoreKeys.of("order", "one", 1), event(placed("one"), 1));
    events.put(StoreKeys.of("order", "one", 2), event(confirmed("one"), corrupt ? 1 : 2));
    AggregateRepository repository = new AggregateRepository(new EventStore(events), new SnapshotStore(storedSnapshots),
        eventify.getHandlers().eventSourcingHandlers(), new AggregateDefinitions(List.of(Order.class)));

    if (corrupt) {
      assertThatThrownBy(() -> repository.replay("order", "one", 2, null)).isInstanceOf(EventReplayException.class);
    } else {
      assertThat(repository.replay("order", "one", 2, null).getVersion()).isEqualTo(2);
    }
    assertThat(events.opened).isPositive();
    assertThat(events.closed).isEqualTo(events.opened);
  }

  private static class ClosingStore extends InMemoryStore<Event> {
    int opened;
    int closed;

    @Override
    public KeyValueIterator<String, Event> range(String from, String to) {
      KeyValueIterator<String, Event> iterator = super.range(from, to);
      opened++;
      return new KeyValueIterator<>() {
        @Override public boolean hasNext() { return iterator.hasNext(); }
        @Override public KeyValue<String, Event> next() { return iterator.next(); }
        @Override public String peekNextKey() { return iterator.peekNextKey(); }
        @Override public void close() { iterator.close(); closed++; }
      };
    }
  }

  private void store(Object payload, long sequence) {
    Event event = event(payload, sequence);
    storedEvents.put(StoreKeys.of("order", event.getAggregateId(), sequence), event);
  }

  private static Event event(Object payload, long sequence) {
    return Event.builder().aggregateType("order").payload(payload).sequence(sequence).build();
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
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "repository-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return Eventify.builder().streamsConfig(properties).registerHandler(new OrderEventSourcingHandler()).build();
  }
}
