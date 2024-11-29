package io.github.alikelleci.eventify.util;

import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;

import java.util.HashMap;
import java.util.Map;

import static io.github.alikelleci.eventify.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;
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
    Map<String, String> commandMetadata = new HashMap<>(command.getMetadata().getEntries());
    commandMetadata.keySet().removeIf(key -> key.startsWith("$"));

    Map<String, String> commandResultMetadata = new HashMap<>(commandResult.getMetadata().getEntries());
    commandResultMetadata.keySet().removeIf(key -> key.startsWith("$"));

    assertThat(commandResult.getType(), is(command.getType()));
    assertThat(commandResult.getId(), is(command.getId()));
    assertThat(commandResult.getAggregateId(), is(command.getAggregateId()));
    assertThat(commandResult.getTimestamp(), is(command.getTimestamp()));

    // Metadata
    assertThat(commandResultMetadata.size(), is(commandMetadata.size()));
    commandMetadata.forEach((key, value) ->
        assertThat(commandResultMetadata, hasEntry(key, value)));
    assertThat(commandResult.getMetadata(), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(CORRELATION_ID), is(command.getMetadata().get(CORRELATION_ID)));
    assertThat(commandResult.getMetadata().get(RESULT), is(isSuccess ? "success" : "failure"));
    assertThat(commandResult.getMetadata().get(CAUSE), isSuccess ? emptyOrNullString() : notNullValue());

    // Payload
    assertThat(commandResult.getPayload(), is(command.getPayload()));
  }

  public static void assertEvent(Command command, Event event) {
    Map<String, String> commandMetadata = new HashMap<>(command.getMetadata().getEntries());
    commandMetadata.keySet().removeIf(key -> key.startsWith("$"));

    Map<String, String> eventMetadata = new HashMap<>(event.getMetadata().getEntries());
    eventMetadata.keySet().removeIf(key -> key.startsWith("$"));

    assertThat(event.getType(), is(notNullValue()));
    assertThat(event.getId(), startsWith(command.getAggregateId().concat("@")));
    assertThat(event.getAggregateId(), is(command.getAggregateId()));
    assertThat(event.getTimestamp(), is(command.getTimestamp()));

    // Metadata
    assertThat(eventMetadata.size(), is(commandMetadata.size())); // ID is added
    commandMetadata.forEach((key, value) ->
        assertThat(eventMetadata, hasEntry(key, value)));
    assertThat(event.getMetadata(), is(notNullValue()));
    assertThat(event.getMetadata().get(CORRELATION_ID), is(command.getMetadata().get(CORRELATION_ID)));
    assertThat(event.getMetadata().get(RESULT), emptyOrNullString());
    assertThat(event.getMetadata().get(CAUSE), emptyOrNullString());

    // Payload
    assertThat(event.getPayload(), is(notNullValue()));
  }

  public static void assertEvent(Command command, Event event, Class<?> type) {
    assertEvent(command, event);
    assertThat(event.getType(), is(type.getSimpleName()));
    assertThat(event.getPayload(), instanceOf(type));
  }

  public static void assertSnapshot(Event event, Aggregate snapshot) {
    Map<String, String> eventMetadata = new HashMap<>(event.getMetadata().getEntries());
    eventMetadata.keySet().removeIf(key -> key.startsWith("$"));

    Map<String, String> snapshotMetadata = new HashMap<>(snapshot.getMetadata().getEntries());
    snapshotMetadata.keySet().removeIf(key -> key.startsWith("$"));

    assertThat(snapshot.getVersion(), is(notNullValue()));
    assertThat(snapshot.getEventId(), is(event.getId()));
    assertThat(snapshot.getType(), is(notNullValue()));
    assertThat(snapshot.getId(), startsWith(event.getAggregateId().concat("@")));
    assertThat(snapshot.getAggregateId(), is(event.getAggregateId()));
    assertThat(snapshot.getTimestamp(), is(event.getTimestamp()));

    // Metadata
    assertThat(snapshotMetadata.size(), is(eventMetadata.size()));
    eventMetadata.forEach((key, value) ->
        assertThat(snapshotMetadata, hasEntry(key, value)));
    assertThat(snapshot.getMetadata(), is(notNullValue()));
    assertThat(snapshot.getMetadata().get(CORRELATION_ID), is(event.getMetadata().get(CORRELATION_ID)));
    assertThat(event.getMetadata().get(RESULT), emptyOrNullString());
    assertThat(event.getMetadata().get(CAUSE), emptyOrNullString());

    // Payload
    assertThat(snapshot.getPayload(), is(notNullValue()));
  }

  public static void assertSnapshot(Event event, Aggregate snapshot, Class<?> type, long version) {
    assertSnapshot(event, snapshot);
    assertThat(snapshot.getVersion(), is(version));
    assertThat(snapshot.getType(), is(type.getSimpleName()));
    assertThat(snapshot.getPayload(), instanceOf(type));
  }
}
