package io.github.alikelleci.eventify.console.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.util.RawValue;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.AggregateRepository;
import io.github.alikelleci.eventify.core.aggregate.AggregateDefinitions;
import io.github.alikelleci.eventify.core.aggregate.AggregateState;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.ApplyEvent;
import io.github.alikelleci.eventify.core.aggregate.exception.EventReplayException;
import io.github.alikelleci.eventify.core.aggregate.exception.EventSourcingException;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.MetadataKeys;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.store.EventStore;
import io.github.alikelleci.eventify.core.aggregate.SnapshotStore;
import io.github.alikelleci.eventify.core.store.internal.StoreKeys;
import io.github.alikelleci.eventify.console.protocol.Requests;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The state before and after an event, with and without a snapshot, and with the events before a snapshot deleted. */
@DisplayName("Aggregate history (console time travel)")
class AggregateHistoryTest {

  @AggregateRoot("counter")
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

    @ApplyEvent
    public Counter apply(Removed event, Counter state) {
      return null;
    }
  }

  public record Removed(@AggregateId String id) {}

  private final Eventify eventify = eventify();
  private final AggregateHistory history = new AggregateHistory(eventify.getObjectMapper());
  private final InMemoryStore<Event> storedEvents = new InMemoryStore<>();
  private final InMemoryStore<AggregateState> storedSnapshots = new InMemoryStore<>();
  private final AggregateRepository repository = new AggregateRepository(new EventStore(storedEvents),
      new SnapshotStore(storedSnapshots), eventify.getHandlers().eventSourcingHandlers(),
      new AggregateDefinitions(List.of(Counter.class)));

  private final Map<String, Long> lastSequences = new HashMap<>();

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
    ConsoleViews.EventDetail detail = detail(first);

    assertThat(detail.previousState()).isNull();
    assertValue(detail.state(), 1, 1);
  }

  @Test
  @DisplayName("Should replay an event after the snapshot from the snapshot")
  void anEventAfterTheSnapshotStartsFromTheSnapshot() {
    snapshotAt(second);
    storedEvents.delete(key(first)); // proves the snapshot is used: without it, the replay would miss this event

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
    storedEvents.delete(key(first)); // @EnableSnapshotting(deleteEvents = true)

    ConsoleViews.EventDetail detail = detail(second);

    // The snapshot is the state after this event; what came before it is gone, so the state before it is unknown.
    assertValue(detail.state(), 2, 2);
    assertThat(detail.stateKnown()).isTrue();
    assertThat(detail.previousState()).isNull();
    assertThat(detail.previousStateKnown()).isFalse();
  }

  @Test
  @DisplayName("Should report every state as unknown when the snapshot is outdated and the events before it were deleted")
  void anOutdatedSnapshotWithTheEventsBeforeItDeleted() {
    snapshotAt(second);
    storedSnapshots.put(StoreKeys.snapshot("counter", "counter-1"), outdated(storedSnapshots.get(StoreKeys.snapshot("counter", "counter-1"))));
    storedEvents.delete(key(first)); // @EnableSnapshotting(deleteEvents = true)

    ConsoleViews.EventDetail detail = detail(third);

    // Replayed from the first event there is, the state after the third event would be 2 instead of 3.
    assertThat(detail.stateKnown()).isFalse();
    assertThat(detail.previousStateKnown()).isFalse();
    assertThat(history.stateAt(repository, "counter", "counter-1", null)).isNull();
  }

  @Test
  @DisplayName("Should rebuild the states from the events when the snapshot is outdated and all events are kept")
  void anOutdatedSnapshotWithAllEventsKept() {
    snapshotAt(second);
    storedSnapshots.put(StoreKeys.snapshot("counter", "counter-1"), outdated(storedSnapshots.get(StoreKeys.snapshot("counter", "counter-1"))));

    assertDetail(third, 2, 3);
  }

  /**
   * The snapshot as it is read when its aggregate can't be (e.g. its class was moved): outdated, like one of another
   * revision. It still tells its version.
   */
  private AggregateState outdated(AggregateState snapshot) {
    ObjectNode json = eventify.getObjectMapper().valueToTree(snapshot);
    json.remove("payload");
    return eventify.getObjectMapper().convertValue(json, AggregateState.class);
  }

  /** The first events were deleted at an earlier snapshot, a later one replaced it, and an event before it is still there. */
  @Test
  @DisplayName("Should report the states of an event before the snapshot as unknown when the first events were deleted")
  void anEventBeforeTheSnapshotWithTheFirstEventsDeletedIsUnknown() {
    snapshotAt(third);
    storedEvents.delete(key(first));

    ConsoleViews.EventDetail detail = detail(second);

    // Replayed from the first event there is, it would be the state after one event: 1 instead of 2.
    assertThat(detail.event()).isEqualTo(second);
    assertThat(detail.state()).isNull();
    assertThat(detail.stateKnown()).isFalse();
    assertThat(detail.previousState()).isNull();
    assertThat(detail.previousStateKnown()).isFalse();
    assertThat(history.stateAt(repository, "counter", "counter-1", second.getSequence())).isNull();
  }

  /** A gap after the second event: what comes before it can be shown, what comes after it can't be replayed. */
  @Test
  @DisplayName("Should show the states before a gap, and refuse the ones after it with the reason")
  void aGapRefusesTheStatesAfterIt() {
    Event fifth = store(new Incremented("counter-1"), 5);      // 4 is missing
    storedEvents.delete(key(third));                           // and so is 3

    assertThat(history.events(repository, "counter", "counter-1", null, 50).events()).containsExactly(fifth, second, first);
    assertDetail(second, 1, 2);
    assertThatThrownBy(() -> detail(fifth))
        .isInstanceOf(EventReplayException.class)
        .hasMessageContaining("expected #3, found #5");
    assertThatThrownBy(() -> history.stateAt(repository, "counter", "counter-1", null))
        .isInstanceOf(EventReplayException.class);
  }

  /** A handler that fails with every event still there is an error, not a state unknown because of deleted events. */
  @Test
  @DisplayName("Should fail, not report an unknown state, when a handler fails while all events are there")
  void aHandlerThatFailsWithAllEventsThereIsNotAnUnknownState() {
    snapshotAt(third);
    store(new Incremented("counter-1"), 1); // replaces the first event: no state before it

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
    assertValue(history.stateAt(repository, "counter", "counter-1", first.getSequence()), 1, 1);
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
    storedEvents.delete(key(first));

    assertThat(detail(first)).isNull();
    assertThat(history.stateAt(repository, "counter", "counter-1", first.getSequence())).isNull();
  }

  @Test
  @DisplayName("Should give the state at an event, and the current state")
  void theStateAtAnEvent() {
    snapshotAt(second);
    storedEvents.delete(key(first));

    assertValue(history.stateAt(repository, "counter", "counter-1", null), 3, 3);
    assertValue(history.stateAt(repository, "counter", "counter-1", second.getSequence()), 2, 2);
    assertValue(history.stateAt(repository, "counter", "counter-1", third.getSequence()), 3, 3);
  }

  /** "counter-1@1" and "counter-1@x": their keys start where the keys of "counter-1" start, and must stay out of it. */
  @Test
  @DisplayName("Should not show the events of an aggregate whose id starts with this id and '@'")
  void theEventsOfAnAggregateWhoseIdStartsWithThisIdAndAtAreNotThisAggregates() {
    Event foreign = store(new Incremented("counter-1@1"));
    Event foreignAfter = store(new Incremented("counter-1@x"));
    // Their keys start where the keys of "counter-1" start, and the separator keeps them out of its range.
    assertThat(key(foreign)).isGreaterThan(StoreKeys.last("counter", "counter-1"));
    assertThat(key(foreignAfter)).isGreaterThan(StoreKeys.last("counter", "counter-1"));

    assertThat(history.events(repository, "counter", "counter-1", null, 50).events()).containsExactly(third, second, first);
    assertThat(history.eventsOfCommand(repository, new Requests.EventsOfCommand("counter", "counter-1", String.valueOf(foreign.getMetadata().getCausationId())))).isEmpty();
    assertValue(history.stateAt(repository, "counter", "counter-1", null), 3, 3);
    assertDetail(first, 0, 1);
    assertThat(history.events(repository, "counter", "counter-1@1", null, 50).events()).containsExactly(foreign);
    assertThat(history.events(repository, "counter", "counter-1@x", null, 50).events()).containsExactly(foreignAfter);
  }

  @Test
  @DisplayName("Should page the events newest first")
  void theEventsArePagedNewestFirst() {
    store(new Incremented("counter-1@1")); // next to the range, not on a page

    ConsoleViews.EventsPage page = history.events(repository, "counter", "counter-1", null, 2);
    assertThat(page.events()).containsExactly(third, second);
    assertThat(page.nextCursor()).isEqualTo(first.getSequence());

    ConsoleViews.EventsPage next = history.events(repository, "counter", "counter-1", page.nextCursor(), 2);
    assertThat(next.events()).containsExactly(first);
    assertThat(next.nextCursor()).isNull();
  }

  @Test
  @DisplayName("Should give the state at an event before the snapshot")
  void theStateAtAnEventBeforeTheSnapshot() {
    snapshotAt(third);

    assertValue(history.stateAt(repository, "counter", "counter-1", second.getSequence()), 2, 2);
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
    ConsoleViews.EventDetail detail = detail(event);
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

  private ConsoleViews.EventDetail detail(Event event) {
    return history.eventDetail(repository, "counter", "counter-1", event.getSequence());
  }

  /** Stores the state after this event as the snapshot, as the application does. */
  private void snapshotAt(Event event) {
    storedSnapshots.put(StoreKeys.snapshot("counter", "counter-1"), repository.replay("counter", "counter-1", event.getSequence(), null));
  }

  /** Two commands of one saga share the correlation id: each shows only the events that name it as their cause. */
  @Test
  @DisplayName("Should give the events a command produced, not those of other commands with the same correlation id")
  void theEventsOfACommand() {
    Event one = store(new Incremented("counter-1"), Map.of(
        MetadataKeys.CORRELATION_ID, "saga", MetadataKeys.CAUSATION_ID, "command-a"));
    Event two = store(new Incremented("counter-1"), Map.of(
        MetadataKeys.CORRELATION_ID, "saga", MetadataKeys.CAUSATION_ID, "command-b"));

    assertThat(history.eventsOfCommand(repository, new Requests.EventsOfCommand("counter", "counter-1", "command-a"))).containsExactly(one);
    assertThat(history.eventsOfCommand(repository, new Requests.EventsOfCommand("counter", "counter-1", "command-b"))).containsExactly(two);
  }

  /** E.g. an event stored before events named their command: which command produced it isn't known. */
  @Test
  @DisplayName("Should give no events for a command when no event names it as its cause")
  void anEventWithoutACausationIdIsOfNoCommand() {
    store(new Incremented("counter-1"), Map.of(MetadataKeys.CORRELATION_ID, "old"));

    assertThat(history.eventsOfCommand(repository, new Requests.EventsOfCommand("counter", "counter-1", "command-a"))).isEmpty();
  }

  /** Stores the payload as its aggregate's next event. */
  private Event store(Object payload) {
    return store(payload, (Map<String, String>) null);
  }

  private Event store(Object payload, Map<String, String> metadata) {
    return store(payload, metadata, lastSequences.merge(AggregateIdResolver.getAggregateId(payload), 1L, Long::sum));
  }

  /** Stores the payload as the event with this sequence, also when that is not the aggregate's next one. */
  private Event store(Object payload, long sequence) {
    return store(payload, null, sequence);
  }

  private Event store(Object payload, Map<String, String> metadata, long sequence) {
    Event event = Event.builder().aggregateType("counter").payload(payload).metadata(Metadata.of(metadata)).sequence(sequence).build();
    storedEvents.put(key(event), event);
    return event;
  }

  private static String key(Event event) {
    return StoreKeys.of(event.getAggregateType(), event.getAggregateId(), event.getSequence());
  }

  @Test
  void anAggregateWithoutEventsHasNoConsoleState() {
    assertThat(history.stateAt(repository, "counter", "never-created", null)).isNull();
  }

  @Test
  void deletedPayloadIsKnownAbsentInConsoleButKeepsRepositoryVersion() {
    Event removed = store(new Removed("counter-1"));
    ConsoleViews.EventDetail detail = detail(removed);
    assertThat(detail.stateKnown()).isTrue();
    assertThat(detail.state()).isNull();
    assertValue(detail.previousState(), 3, 3);
    assertThat(history.stateAt(repository, "counter", "counter-1", null)).isNull();
    assertThat(repository.replay("counter", "counter-1").getVersion()).isEqualTo(4);

    snapshotAt(removed);
    storedEvents.delete(key(first));
    storedEvents.delete(key(second));
    storedEvents.delete(key(third));
    detail = detail(removed);
    assertThat(detail.stateKnown()).isTrue();
    assertThat(detail.state()).isNull();
    assertThat(detail.previousStateKnown()).isFalse();
    assertThat(history.stateAt(repository, "counter", "counter-1", null)).isNull();
  }

  private static Eventify eventify() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "history-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return Eventify.builder().streamsConfig(properties).registerHandler(new CounterHandler()).build();
  }

}
