package io.github.alikelleci.eventify.util;

import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.List;

import static io.github.alikelleci.eventify.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.RESULT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class Matchers {

  public static void assertCommandResult(Command command, Command commandResult, boolean isSuccess) {
    Metadata metadata = Metadata.builder()
        .addAll(command.getMetadata())
        .remove(ID)
        .remove(RESULT)
        .remove(CAUSE)
        .build();

    assertThat(commandResult.getType(), is(command.getType()));
    assertThat(commandResult.getId(), is(command.getId()));
    assertThat(commandResult.getAggregateId(), is(command.getAggregateId()));
    assertThat(commandResult.getTimestamp(), is(command.getTimestamp()));

    // Metadata
    assertThat(commandResult.getMetadata(), is(notNullValue()));
    assertThat(commandResult.getMetadata().size(), is(metadata.size() + (isSuccess ? 2 : 3))); // ID, RESULT and CAUSE are added
    metadata.forEach((key, value) ->
        assertThat(commandResult.getMetadata(), hasEntry(key, value)));
    assertThat(commandResult.getMetadata().get(ID), is(commandResult.getId()));
    assertThat(commandResult.getMetadata().get(RESULT), is(isSuccess ? "success" : "failure"));
    assertThat(commandResult.getMetadata().get(CAUSE), isSuccess ? emptyOrNullString() : notNullValue());

    // Payload
    assertThat(commandResult.getPayload(), is(command.getPayload()));
  }

  public static void assertEvent(Command command, Event event) {
    Metadata metadata = Metadata.builder()
        .addAll(command.getMetadata())
        .remove(ID)
        .remove(RESULT)
        .remove(CAUSE)
        .build();

    assertThat(event.getType(), is(notNullValue()));
    assertThat(event.getId(), startsWith(command.getAggregateId().concat("@")));
    assertThat(event.getAggregateId(), is(command.getAggregateId()));
    assertThat(event.getTimestamp(), is(command.getTimestamp()));

    // Metadata
    assertThat(event.getMetadata(), is(notNullValue()));
    assertThat(event.getMetadata().size(), is(metadata.size() + 1)); // ID is added
    metadata.forEach((key, value) ->
        assertThat(event.getMetadata(), hasEntry(key, value)));
    assertThat(event.getMetadata().get(ID), is(event.getId()));
    assertThat(event.getMetadata().get(RESULT), emptyOrNullString());
    assertThat(event.getMetadata().get(CAUSE), emptyOrNullString());

    // Payload
    assertThat(event.getPayload(), is(notNullValue()));
  }


  public static void assertEvent(Command command, Event event, Class<?> type) {
    Metadata metadata = Metadata.builder()
        .addAll(command.getMetadata())
        .remove(ID)
        .remove(RESULT)
        .remove(CAUSE)
        .build();

    assertThat(event.getType(), is(type.getSimpleName()));
    assertThat(event.getId(), startsWith(command.getAggregateId().concat("@")));
    assertThat(event.getAggregateId(), is(command.getAggregateId()));
    assertThat(event.getTimestamp(), is(command.getTimestamp()));

    // Metadata
    assertThat(event.getMetadata(), is(notNullValue()));
    assertThat(event.getMetadata().size(), is(metadata.size() + 1)); // ID is added
    metadata.forEach((key, value) ->
        assertThat(event.getMetadata(), hasEntry(key, value)));
    assertThat(event.getMetadata().get(ID), is(event.getId()));
    assertThat(event.getMetadata().get(RESULT), emptyOrNullString());
    assertThat(event.getMetadata().get(CAUSE), emptyOrNullString());

    // Payload
    assertThat(event.getPayload(), instanceOf(type));
  }

  public static void assertEventsInStore(KeyValueStore<String, Event> eventStore, List<Event> events) {
    events.forEach(event -> {
      Event eventInStore = eventStore.get(event.getId());
      assertThat(eventInStore, is(notNullValue()));
      assertThat(eventInStore.toString(), is(event.toString()));
    });
  }

  public static void assertSnapshot(Event event, Aggregate snapshot, Class<?> type, long version) {
    Metadata metadata = Metadata.builder()
        .addAll(event.getMetadata())
        .remove(ID)
        .remove(RESULT)
        .remove(CAUSE)
        .build();

    assertThat(snapshot.getVersion(), is(version));
    assertThat(snapshot.getEventId(), is(event.getId()));
    assertThat(snapshot.getType(), is(type.getSimpleName()));
    assertThat(snapshot.getId(), startsWith(event.getAggregateId().concat("@")));
    assertThat(snapshot.getAggregateId(), is(event.getAggregateId()));
    assertThat(snapshot.getTimestamp(), is(event.getTimestamp()));

    // Metadata
    assertThat(snapshot.getMetadata(), is(notNullValue()));
    assertThat(snapshot.getMetadata().size(), is(metadata.size() + 1)); // ID is added
    metadata.forEach((key, value) ->
        assertThat(snapshot.getMetadata(), hasEntry(key, value)));
    assertThat(snapshot.getMetadata().get(ID), is(snapshot.getId()));

    // Payload
    assertThat(snapshot.getPayload(), instanceOf(type));
  }
}
