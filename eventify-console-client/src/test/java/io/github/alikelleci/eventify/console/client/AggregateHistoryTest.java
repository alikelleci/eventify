package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.util.RawValue;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.AggregateReplayer;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.aggregate.exception.EventSourcingException;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.MetadataKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.store.ReadOnlyEventStore;
import io.github.alikelleci.eventify.core.store.ReadOnlySnapshotStore;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The state before and after an event, with and without a snapshot, and with the events before a snapshot deleted. */
@DisplayName("Aggregate history (console time travel)")
class AggregateHistoryTest {

  @AggregateRoot
  public static class Counter {
    @AggregateId
    final String id;
    final int value;

    Counter(String id, int value) {
      this.id = id;
      this.value = value;
    }

    public String getId() {
      return id;
    }

    public int getValue() {
      return value;
    }
  }

  public static class Started {
    @AggregateId
    final String id;

    Started(String id) {
      this.id = id;
    }
  }

  public static class Incremented {
    @AggregateId
    final String id;

    Incremented(String id) {
      this.id = id;
    }
  }

  /** Like most handlers, it needs the state before an event that changes the aggregate: it fails without one. */
  public static class CounterHandler {
    @ApplyEvent
    public Counter apply(Started event, Counter state) {
      return new Counter(event.id, 1);
    }

    @ApplyEvent
    public Counter apply(Incremented event, Counter state) {
      return new Counter(event.id, state.value + 1);
    }
  }

  /** An aggregate whose handlers change the state they are given, and return it. */
  @AggregateRoot
  public static class MutableCounter {
    @AggregateId
    String id;
    int value;

    public String getId() {
      return id;
    }

    public int getValue() {
      return value;
    }
  }

  public static class MutableCounterStarted {
    @AggregateId
    final String id;

    MutableCounterStarted(String id) {
      this.id = id;
    }
  }

  public static class MutableCounterIncremented {
    @AggregateId
    final String id;

    MutableCounterIncremented(String id) {
      this.id = id;
    }
  }

  public static class MutableCounterHandler {
    @ApplyEvent
    public MutableCounter apply(MutableCounterStarted event, MutableCounter state) {
      MutableCounter counter = new MutableCounter();
      counter.id = event.id;
      counter.value = 1;
      return counter;
    }

    @ApplyEvent
    public MutableCounter apply(MutableCounterIncremented event, MutableCounter state) {
      state.value++;
      return state;
    }
  }

  private final Eventify eventify = eventify();
  private final AggregateHistory history = new AggregateHistory(eventify.getAggregateReplayer(), eventify.getObjectMapper());
  private final AggregateReplayer replay = eventify.getAggregateReplayer();
  private final InMemoryStore<Event> storedEvents = new InMemoryStore<>();
  private final InMemoryStore<AggregateState> storedSnapshots = new InMemoryStore<>();
  private final ReadOnlyEventStore events = ReadOnlyEventStore.of(storedEvents);
  private final ReadOnlySnapshotStore snapshots = ReadOnlySnapshotStore.of(storedSnapshots);

  private Event first;
  private Event second;
  private Event third;

  @BeforeEach
  void storeEvents() {
    first = store(new Started("counter-1"));
    store(new Started("counter-10"));  // another aggregate, stored right before counter-1@...
    second = store(new Incremented("counter-1"));
    third = store(new Incremented("counter-1"));
  }

  @Test
  @DisplayName("Should give the state before and after an event")
  void theStateBeforeAndAfterAnEvent() {
    assertDetail(second, 1, 2);
    assertDetail(third, 2, 3);
  }

  @Test
  @DisplayName("Should have no state before the first event")
  void beforeTheFirstEventThereIsNoState() {
    ConsoleService.EventDetail detail = detail(first);

    assertThat(detail.previousState()).isNull();
    assertValue(detail.state(), 1, 1);
  }

  @Test
  @DisplayName("Should replay an event after the snapshot from the snapshot")
  void anEventAfterTheSnapshotStartsFromTheSnapshot() {
    snapshotAt(second);
    storedEvents.delete(first.getId()); // proves the snapshot is used: without it, the replay would miss this event

    assertDetail(third, 2, 3);
  }

  @Test
  @DisplayName("Should give the states at the snapshot's own event when all events are kept")
  void theSnapshotsOwnEventWithAllEventsKept() {
    snapshotAt(second);

    assertDetail(second, 1, 2);
  }

  @Test
  @DisplayName("Should give the snapshot as the state at its own event, and an unknown state before it, when earlier events were deleted")
  void theSnapshotsOwnEventWithTheEventsBeforeItDeleted() {
    snapshotAt(second);
    storedEvents.delete(first.getId()); // @EnableSnapshotting(deleteEvents = true)

    ConsoleService.EventDetail detail = detail(second);

    // The snapshot is the state after this event; what came before it is gone, so the state before it is unknown.
    assertValue(detail.state(), 2, 2);
    assertThat(detail.stateKnown()).isTrue();
    assertThat(detail.previousState()).isNull();
    assertThat(detail.previousStateKnown()).isFalse();
  }

  /** The first events were deleted at an earlier snapshot, a later one replaced it, and an event before it is still there. */
  @Test
  @DisplayName("Should report the states of an event before the snapshot as unknown when the first events were deleted")
  void anEventBeforeTheSnapshotWithTheFirstEventsDeletedIsUnknown() {
    snapshotAt(third);
    storedEvents.delete(first.getId());

    ConsoleService.EventDetail detail = detail(second);

    // Replayed from the first event there is, it would be the state after one event: 1 instead of 2.
    assertThat(detail.event()).isEqualTo(second);
    assertThat(detail.state()).isNull();
    assertThat(detail.stateKnown()).isFalse();
    assertThat(detail.previousState()).isNull();
    assertThat(detail.previousStateKnown()).isFalse();
    assertThat(history.stateAt(events, snapshots, "counter-1", second.getId())).isNull();
  }

  /** A handler that fails with every event still there is an error, not a state unknown because of deleted events. */
  @Test
  @DisplayName("Should fail, not report an unknown state, when a handler fails while all events are there")
  void aHandlerThatFailsWithAllEventsThereIsNotAnUnknownState() {
    snapshotAt(third);
    storedEvents.put(first.getId(), Event.builder().payload(new Incremented("counter-1")).build().withId(first.getId())); // no state before it

    assertThatThrownBy(() -> detail(second)).isInstanceOf(EventSourcingException.class);
  }

  @Test
  @DisplayName("Should give an event before the snapshot the state before the next event")
  void anEventBeforeTheSnapshotHasTheStateBeforeTheNextEvent() {
    Event fourth = store(new Incremented("counter-1"));
    snapshotAt(fourth);

    assertDetail(first, 0, 1);
    assertDetail(second, 1, 2);
    assertDetail(fourth, 3, 4);
    assertValue(history.stateAt(events, snapshots, "counter-1", first.getId()), 1, 1);
  }

  @Test
  @DisplayName("Should replay an event before the snapshot from the first event")
  void anEventBeforeTheSnapshotIsReplayedFromTheFirstEvent() {
    snapshotAt(third);

    assertDetail(second, 1, 2);
  }

  @Test
  @DisplayName("Should not find an event that is not stored")
  void anEventThatIsNotThereIsNotFound() {
    snapshotAt(second);
    storedEvents.delete(first.getId());

    assertThat(detail(first)).isNull();
    assertThat(history.stateAt(events, snapshots, "counter-1", first.getId())).isNull();
  }

  @Test
  @DisplayName("Should give the state at an event, and the current state")
  void theStateAtAnEvent() {
    snapshotAt(second);
    storedEvents.delete(first.getId());

    assertValue(history.stateAt(events, snapshots, "counter-1", null), 3, 3);
    assertValue(history.stateAt(events, snapshots, "counter-1", second.getId()), 2, 2);
    assertValue(history.stateAt(events, snapshots, "counter-1", third.getId()), 3, 3);
  }

  /** "counter-1@x": its keys ("counter-1@x@ULID") are in the key range of "counter-1". */
  @Test
  @DisplayName("Should not show the events of an aggregate whose id starts with this id and '@'")
  void theEventsOfAnAggregateWhoseIdStartsWithThisIdAndAtAreNotThisAggregates() {
    Event foreign = store(new Incremented("counter-1@x"));
    Event foreignBeforeUlids = store(new Incremented("counter-1@-x")); // "-" sorts before every ULID
    assertThat(foreign.getId()).startsWith("counter-1@");

    assertThat(history.events(events, "counter-1", null, 50).events()).containsExactly(third, second, first);
    assertThat(history.eventsOfCommand(events, "counter-1", "counter-1@x", foreign.getMetadata().getCorrelationId())).isEmpty();
    assertValue(history.stateAt(events, snapshots, "counter-1", null), 3, 3);
    assertDetail(first, 0, 1);
    assertThat(history.events(events, "counter-1@x", null, 50).events()).containsExactly(foreign);
    assertThat(history.events(events, "counter-1@-x", null, 50).events()).containsExactly(foreignBeforeUlids);
  }

  @Test
  @DisplayName("Should page the events newest first")
  void theEventsArePagedNewestFirst() {
    store(new Incremented("counter-1@x")); // in the range, not on a page

    ConsoleService.EventsPage page = history.events(events, "counter-1", null, 2);
    assertThat(page.events()).containsExactly(third, second);
    assertThat(page.nextCursor()).isEqualTo(first.getId().substring("counter-1@".length()));

    ConsoleService.EventsPage next = history.events(events, "counter-1", page.nextCursor(), 2);
    assertThat(next.events()).containsExactly(first);
    assertThat(next.nextCursor()).isNull();
  }

  @Test
  @DisplayName("Should give the state at an event before the snapshot")
  void theStateAtAnEventBeforeTheSnapshot() {
    snapshotAt(third);

    assertValue(history.stateAt(events, snapshots, "counter-1", second.getId()), 2, 2);
  }

  /** Each state is the state at its own event, not the state the handlers after it made of the same object. */
  @Test
  @DisplayName("Should keep earlier states unchanged when a handler changes the state it is given")
  void aHandlerThatChangesTheStateItIsGivenDoesNotChangeTheStatesBeforeIt() {
    store(new MutableCounterStarted("mutable-counter-1"));
    Event incremented = store(new MutableCounterIncremented("mutable-counter-1"));
    Event incrementedAgain = store(new MutableCounterIncremented("mutable-counter-1"));

    ConsoleService.EventDetail detail = history.eventDetail(events, snapshots, "mutable-counter-1", incremented.getId());
    assertValue(detail.previousState(), 1, 1);
    assertValue(detail.state(), 2, 2);

    // With a snapshot after the event, the replay goes on past it.
    storedSnapshots.put("mutable-counter-1", replayed("mutable-counter-1", null, incrementedAgain.getId()).state());
    detail = history.eventDetail(events, snapshots, "mutable-counter-1", incremented.getId());
    assertValue(detail.previousState(), 1, 1);
    assertValue(detail.state(), 2, 2);
    assertValue(history.stateAt(events, snapshots, "mutable-counter-1", incremented.getId()), 2, 2);
  }

  /** The states are sent to the console as the JSON objects they are, as before: not as text. */
  @Test
  @DisplayName("Should send the states as JSON objects, not as text")
  void theStatesAreWrittenAsJsonObjects() throws Exception {
    JsonNode json = eventify.getObjectMapper().readTree(eventify.getObjectMapper().writeValueAsString(detail(second)));

    assertThat(json.get("state").isObject()).isTrue();
    assertThat(json.at("/state/payload/value").asInt()).isEqualTo(2);
    assertThat(json.at("/state/version").asLong()).isEqualTo(2);
    assertThat(json.at("/previousState/payload/value").asInt()).isEqualTo(1);
  }

  private void assertDetail(Event event, int before, int after) {
    ConsoleService.EventDetail detail = detail(event);
    assertThat(detail.event()).isEqualTo(event);
    assertThat(detail.stateKnown()).isTrue();
    assertThat(detail.previousStateKnown()).isTrue();
    if (before == 0) {
      assertThat(detail.previousState()).isNull();
    } else {
      assertValue(detail.previousState(), before, before);
    }
    assertValue(detail.state(), after, after);
  }

  private void assertValue(RawValue state, int value, long version) {
    assertThat(state).isNotNull();
    JsonNode json = read(state);
    assertThat(json.at("/payload/value").asInt()).isEqualTo(value);
    assertThat(json.get("version").asLong()).isEqualTo(version);
  }

  private JsonNode read(RawValue state) {
    try {
      return eventify.getObjectMapper().readTree(state.rawValue().toString());
    } catch (Exception e) {
      throw new AssertionError(e);
    }
  }

  private ConsoleService.EventDetail detail(Event event) {
    return history.eventDetail(events, snapshots, "counter-1", event.getId());
  }

  /** Stores the state after this event as the snapshot, as the application does. */
  private void snapshotAt(Event event) {
    storedSnapshots.put("counter-1", replayed("counter-1", null, event.getId()).state());
  }

  /** Two commands of one saga share the correlation id: each shows only its own events. */
  @Test
  @DisplayName("Should give the events a command produced, not those of other commands with the same correlation id")
  void theEventsOfACommand() {
    Event one = store(Event.builder().payload(new Incremented("counter-1")).metadata(Map.of(
        MetadataKeys.CORRELATION_ID, "saga", MetadataKeys.CAUSATION_ID, "counter-1@01AAAAAAAAAAAAAAAAAAAAAAAA")).build());
    Event two = store(Event.builder().payload(new Incremented("counter-1")).metadata(Map.of(
        MetadataKeys.CORRELATION_ID, "saga", MetadataKeys.CAUSATION_ID, "counter-1@01BBBBBBBBBBBBBBBBBBBBBBBB")).build());

    assertThat(history.eventsOfCommand(events, "counter-1", "counter-1@01AAAAAAAAAAAAAAAAAAAAAAAA", "saga")).containsExactly(one);
    assertThat(history.eventsOfCommand(events, "counter-1", "counter-1@01BBBBBBBBBBBBBBBBBBBBBBBB", "saga")).containsExactly(two);
  }

  /** Stored before events named their command: found by the correlation id, without taking events that do name another. */
  @Test
  @DisplayName("Should find events without a causation id by the command's correlation id")
  void theEventsOfACommandWithoutCausationIds() {
    Event legacy = store(Event.builder().payload(new Incremented("counter-1")).metadata(MetadataKeys.CORRELATION_ID, "old").build());
    store(Event.builder().payload(new Incremented("counter-1")).metadata(Map.of(
        MetadataKeys.CORRELATION_ID, "old", MetadataKeys.CAUSATION_ID, "counter-1@01BBBBBBBBBBBBBBBBBBBBBBBB")).build());

    assertThat(history.eventsOfCommand(events, "counter-1", "counter-1@01AAAAAAAAAAAAAAAAAAAAAAAA", "old")).containsExactly(legacy);
    assertThat(history.eventsOfCommand(events, "counter-1", "counter-1@01AAAAAAAAAAAAAAAAAAAAAAAA", null)).isEmpty();
  }

  private Event store(Event event) {
    storedEvents.put(event.getId(), event);
    return event;
  }

  private Event store(Object payload) {
    return store(Event.builder().payload(payload).build());
  }

  private static Eventify eventify() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "history-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return Eventify.builder().streamsConfig(properties).registerHandler(new CounterHandler()).registerHandler(new MutableCounterHandler()).build();
  }

  /** The aggregate's stored events after {@code start}, up to and including {@code untilEventId}, applied to {@code start}. */
  private AggregateReplayer.Result replayed(String aggregateId, AggregateState start, String untilEventId) {
    return replayed(aggregateId, start, untilEventId, null);
  }

  private AggregateReplayer.Result replayed(String aggregateId, AggregateState start, String untilEventId,
                                            AggregateReplayer.Listener listener) {
    try (ReadOnlyEventStore.Events toApply = events.events(aggregateId, start != null ? start.getEventId() : null, untilEventId)) {
      return replay.replay(toApply, start, listener);
    }
  }
}
