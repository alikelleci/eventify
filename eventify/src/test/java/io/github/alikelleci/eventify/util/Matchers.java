package io.github.alikelleci.eventify.util;

import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;

import java.util.HashMap;
import java.util.Map;

import static io.github.alikelleci.eventify.messaging.Metadata.AGGREGATE_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.EVENT_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.RESULT;
import static io.github.alikelleci.eventify.messaging.Metadata.REVISION;
import static io.github.alikelleci.eventify.messaging.Metadata.TIMESTAMP;
import static io.github.alikelleci.eventify.messaging.Metadata.VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class Matchers {

  public static void assertCommandResult(Command command, Command commandResult, boolean isSuccess) {
    Map<String, String> commandMetadata = new HashMap<>(command.getMetadata());
    commandMetadata.keySet().removeIf(key -> key.startsWith("$"));

    Map<String, String> commandResultMetadata = new HashMap<>(commandResult.getMetadata());
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
    assertThat(commandResult.getMetadata().get(ID), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(ID), startsWith(commandResult.getAggregateId() + "@"));
    assertThat(commandResult.getMetadata().get(ID), is(commandResult.getId()));
    assertThat(commandResult.getMetadata().get(TIMESTAMP), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(TIMESTAMP), is(commandResult.getTimestamp().toString()));
    assertThat(commandResult.getMetadata().get(AGGREGATE_ID), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(AGGREGATE_ID), is(commandResult.getAggregateId()));
    assertThat(commandResult.getMetadata().get(RESULT), is(isSuccess ? "success" : "failure"));
    assertThat(commandResult.getMetadata().get(CAUSE), isSuccess ? emptyOrNullString() : notNullValue());
    assertThat(commandResult.getMetadata().get(CORRELATION_ID), is(command.getMetadata().get(CORRELATION_ID)));


    // Payload
    assertThat(commandResult.getPayload(), is(command.getPayload()));
  }

  public static void assertEvent(Command command, Event event) {
    Map<String, String> commandMetadata = new HashMap<>(command.getMetadata());
    commandMetadata.keySet().removeIf(key -> key.startsWith("$"));

    Map<String, String> eventMetadata = new HashMap<>(event.getMetadata());
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
    assertThat(event.getMetadata().get(ID), is(notNullValue()));
    assertThat(event.getMetadata().get(ID), startsWith(event.getAggregateId() + "@"));
    assertThat(event.getMetadata().get(ID), is(event.getId()));
    assertThat(event.getMetadata().get(TIMESTAMP), is(notNullValue()));
    assertThat(event.getMetadata().get(TIMESTAMP), is(event.getTimestamp().toString()));
    assertThat(event.getMetadata().get(AGGREGATE_ID), is(notNullValue()));
    assertThat(event.getMetadata().get(AGGREGATE_ID), is(event.getAggregateId()));
    assertThat(event.getMetadata().get(RESULT), emptyOrNullString());
    assertThat(event.getMetadata().get(CAUSE), emptyOrNullString());
    assertThat(event.getMetadata().get(REVISION), is(notNullValue()));
    assertThat(event.getMetadata().get(REVISION), is(String.valueOf(event.getRevision())));
    assertThat(event.getMetadata().get(CORRELATION_ID), is(command.getMetadata().get(CORRELATION_ID)));

    // Payload
    assertThat(event.getPayload(), is(notNullValue()));
  }

  public static void assertEvent(Command command, Event event, Class<?> type) {
    assertEvent(command, event);
    assertThat(event.getType(), is(type.getSimpleName()));
    assertThat(event.getPayload(), instanceOf(type));
  }

  public static void assertSnapshot(Event event, Aggregate snapshot) {
    Map<String, String> eventMetadata = new HashMap<>(event.getMetadata());
    eventMetadata.keySet().removeIf(key -> key.startsWith("$"));

    Map<String, String> snapshotMetadata = new HashMap<>(snapshot.getMetadata());
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
        assertThat(snapshot.getMetadata(), hasEntry(key, value)));
    assertThat(snapshot.getMetadata(), is(notNullValue()));
    assertThat(snapshot.getMetadata().get(ID), is(notNullValue()));
    assertThat(snapshot.getMetadata().get(ID), startsWith(snapshot.getAggregateId() + "@"));
    assertThat(snapshot.getMetadata().get(ID), is(snapshot.getId()));
    assertThat(snapshot.getMetadata().get(TIMESTAMP), is(notNullValue()));
    assertThat(snapshot.getMetadata().get(TIMESTAMP), is(snapshot.getTimestamp().toString()));
    assertThat(snapshot.getMetadata().get(AGGREGATE_ID), is(notNullValue()));
    assertThat(snapshot.getMetadata().get(AGGREGATE_ID), is(snapshot.getAggregateId()));
    assertThat(event.getMetadata().get(RESULT), emptyOrNullString());
    assertThat(event.getMetadata().get(CAUSE), emptyOrNullString());
    assertThat(snapshot.getMetadata().get(EVENT_ID), is(notNullValue()));
    assertThat(snapshot.getMetadata().get(EVENT_ID), is(snapshot.getEventId()));
    assertThat(snapshot.getMetadata().get(VERSION), is(notNullValue()));
    assertThat(snapshot.getMetadata().get(VERSION), is(String.valueOf(snapshot.getVersion())));
    assertThat(snapshot.getMetadata().get(CORRELATION_ID), is(event.getMetadata().get(CORRELATION_ID)));

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
