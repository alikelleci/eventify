package io.github.alikelleci.eventify.console.client;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.AggregateRoot;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateReplay;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.annotations.ApplyEvent;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.exceptions.AggregateInvocationException;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** The state before and after an event, with and without a snapshot, and with the events before a snapshot deleted. */
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

  private final Eventify eventify = eventify();
  private final AggregateHistory history = new AggregateHistory(eventify.getEventSourcingHandlers());
  private final AggregateReplay replay = new AggregateReplay(eventify.getEventSourcingHandlers());
  private final InMemoryStore<Event> events = new InMemoryStore<>();
  private final InMemoryStore<AggregateState> snapshots = new InMemoryStore<>();

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
  void theStateBeforeAndAfterAnEvent() {
    assertDetail(second, 1, 2);
    assertDetail(third, 2, 3);
  }

  @Test
  void beforeTheFirstEventThereIsNoState() {
    ConsoleService.EventDetail detail = detail(first);

    assertThat(detail.previousState()).isNull();
    assertValue(detail.state(), 1, 1);
  }

  @Test
  void anEventAfterTheSnapshotStartsFromTheSnapshot() {
    snapshotAt(second);
    events.delete(first.getId()); // proves the snapshot is used: without it, the replay would miss this event

    assertDetail(third, 2, 3);
  }

  @Test
  void theSnapshotsOwnEventWithAllEventsKept() {
    snapshotAt(second);

    assertDetail(second, 1, 2);
  }

  @Test
  void theSnapshotsOwnEventWithTheEventsBeforeItDeleted() {
    snapshotAt(second);
    events.delete(first.getId()); // @EnableSnapshotting(deleteEvents = true)

    ConsoleService.EventDetail detail = detail(second);

    // The snapshot is the state after this event; what came before it is gone, so the state before it is unknown.
    assertValue(detail.state(), 2, 2);
    assertThat(detail.stateKnown()).isTrue();
    assertThat(detail.previousState()).isNull();
    assertThat(detail.previousStateKnown()).isFalse();
  }

  /** The first events were deleted at an earlier snapshot, a later one replaced it, and an event before it is still there. */
  @Test
  void anEventBeforeTheSnapshotWithTheFirstEventsDeletedIsUnknown() {
    snapshotAt(third);
    events.delete(first.getId());

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
  void aHandlerThatFailsWithAllEventsThereIsNotAnUnknownState() {
    snapshotAt(third);
    events.put(first.getId(), Event.builder().payload(new Incremented("counter-1")).build().withId(first.getId())); // no state before it

    assertThatThrownBy(() -> detail(second)).isInstanceOf(AggregateInvocationException.class);
  }

  @Test
  void anEventBeforeTheSnapshotHasTheStateBeforeTheNextEvent() {
    Event fourth = store(new Incremented("counter-1"));
    snapshotAt(fourth);

    assertDetail(first, 0, 1);
    assertDetail(second, 1, 2);
    assertDetail(fourth, 3, 4);
    assertValue(history.stateAt(events, snapshots, "counter-1", first.getId()), 1, 1);
  }

  @Test
  void anEventBeforeTheSnapshotIsReplayedFromTheFirstEvent() {
    snapshotAt(third);

    assertDetail(second, 1, 2);
  }

  @Test
  void anEventThatIsNotThereIsNotFound() {
    snapshotAt(second);
    events.delete(first.getId());

    assertThat(detail(first)).isNull();
    assertThat(history.stateAt(events, snapshots, "counter-1", first.getId())).isNull();
  }

  @Test
  void theStateAtAnEvent() {
    snapshotAt(second);
    events.delete(first.getId());

    assertValue(history.stateAt(events, snapshots, "counter-1", null), 3, 3);
    assertValue(history.stateAt(events, snapshots, "counter-1", second.getId()), 2, 2);
    assertValue(history.stateAt(events, snapshots, "counter-1", third.getId()), 3, 3);
  }

  /** "counter-1@x": its keys ("counter-1@x@ULID") are in the key range of "counter-1". */
  @Test
  void theEventsOfAnAggregateWhoseIdStartsWithThisIdAndAtAreNotThisAggregates() {
    Event foreign = store(new Incremented("counter-1@x"));
    Event foreignBeforeUlids = store(new Incremented("counter-1@-x")); // "-" sorts before every ULID
    assertThat(foreign.getId()).startsWith("counter-1@");

    assertThat(history.events(events, "counter-1", null, 50).events()).containsExactly(third, second, first);
    assertThat(history.eventsByCorrelation(events, "counter-1", foreign.getMetadata().getCorrelationId())).isEmpty();
    assertValue(history.stateAt(events, snapshots, "counter-1", null), 3, 3);
    assertDetail(first, 0, 1);
    assertThat(history.events(events, "counter-1@x", null, 50).events()).containsExactly(foreign);
    assertThat(history.events(events, "counter-1@-x", null, 50).events()).containsExactly(foreignBeforeUlids);
  }

  @Test
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
  void theStateAtAnEventBeforeTheSnapshot() {
    snapshotAt(third);

    assertValue(history.stateAt(events, snapshots, "counter-1", second.getId()), 2, 2);
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

  private static void assertValue(AggregateState state, int value, long version) {
    assertThat(state).isNotNull();
    assertThat(((Counter) state.getPayload()).value).isEqualTo(value);
    assertThat(state.getVersion()).isEqualTo(version);
  }

  private ConsoleService.EventDetail detail(Event event) {
    return history.eventDetail(events, snapshots, "counter-1", event.getId());
  }

  /** Stores the state after this event as the snapshot, as the application does. */
  private void snapshotAt(Event event) {
    snapshots.put("counter-1", replay.replay(events, "counter-1", null, event.getId()).state());
  }

  private Event store(Object payload) {
    Event event = Event.builder().payload(payload).build();
    events.put(event.getId(), event);
    return event;
  }

  private static Eventify eventify() {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "history-test");
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    return Eventify.builder().streamsConfig(properties).registerHandler(new CounterHandler()).build();
  }
}
