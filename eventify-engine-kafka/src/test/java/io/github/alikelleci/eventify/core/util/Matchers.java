package io.github.alikelleci.eventify.core.util;

import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;

import java.util.HashMap;
import java.util.Map;

import static io.github.alikelleci.eventify.core.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.core.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.core.messaging.Metadata.RESULT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.startsWith;

public class Matchers {

  public static void assertCommandResult(Command command, Command commandResult, boolean isSuccess) {
    Map<String, String> metadata = new HashMap<>(command.getMetadata());
    metadata.keySet().removeIf(key -> key.startsWith("$") && !key.equals(CORRELATION_ID));

    assertThat(commandResult.getType(), is(command.getType()));
    assertThat(commandResult.getId(), is(command.getId()));
    assertThat(commandResult.getAggregateId(), is(command.getAggregateId()));
    assertThat(commandResult.getTimestamp(), is(command.getTimestamp()));

    // Metadata
    assertThat(commandResult.getMetadata(), is(notNullValue()));
    assertThat(commandResult.getMetadata().size(), is(metadata.size() + (isSuccess ? 1 : 2))); // RESULT and/or CAUSE added
    metadata.forEach((key, value) ->
        assertThat(commandResult.getMetadata(), hasEntry(key, value)));
    assertThat(commandResult.getMetadata().get(CORRELATION_ID), is(notNullValue()));
    assertThat(commandResult.getMetadata().get(CORRELATION_ID), is(command.getMetadata().get(CORRELATION_ID)));
    assertThat(commandResult.getMetadata().get(RESULT), is(isSuccess ? "success" : "failure"));
    assertThat(commandResult.getMetadata().get(CAUSE), isSuccess ? emptyOrNullString() : notNullValue());

    // Payload
    assertThat(commandResult.getPayload(), is(command.getPayload()));
  }

  public static void assertEvent(Command command, Event event) {
    Map<String, String> metadata = new HashMap<>(command.getMetadata());
    metadata.keySet().removeIf(key -> key.startsWith("$") && !key.equals(CORRELATION_ID));

    assertThat(event.getType(), is(notNullValue()));
    assertThat(event.getId(), startsWith(command.getAggregateId().concat("@")));
    assertThat(event.getAggregateId(), is(command.getAggregateId()));
    assertThat(event.getTimestamp(), is(command.getTimestamp()));

    // Metadata
    assertThat(event.getMetadata(), is(notNullValue()));
    assertThat(event.getMetadata().size(), is(metadata.size()));
    metadata.forEach((key, value) ->
        assertThat(event.getMetadata(), hasEntry(key, value)));
    assertThat(event.getMetadata().get(CORRELATION_ID), is(notNullValue()));
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

  public static void assertSnapshot(Event event, AggregateState state) {
    Map<String, String> metadata = new HashMap<>(event.getMetadata());
    metadata.keySet().removeIf(key -> key.startsWith("$") && !key.equals(CORRELATION_ID));

    assertThat(state.getVersion(), is(notNullValue()));
    assertThat(state.getEventId(), is(event.getId()));
    assertThat(state.getType(), is(notNullValue()));
    assertThat(state.getId(), startsWith(event.getAggregateId().concat("@")));
    assertThat(state.getAggregateId(), is(event.getAggregateId()));
    assertThat(state.getTimestamp(), is(event.getTimestamp()));

    // Metadata
    assertThat(state.getMetadata(), is(notNullValue()));
    assertThat(state.getMetadata().size(), is(metadata.size()));
    metadata.forEach((key, value) ->
        assertThat(state.getMetadata(), hasEntry(key, value)));
    assertThat(state.getMetadata().get(CORRELATION_ID), is(notNullValue()));
    assertThat(state.getMetadata().get(CORRELATION_ID), is(event.getMetadata().get(CORRELATION_ID)));
    assertThat(state.getMetadata().get(RESULT), emptyOrNullString());
    assertThat(state.getMetadata().get(CAUSE), emptyOrNullString());

    // Payload
    assertThat(state.getPayload(), is(notNullValue()));
  }

  public static void assertSnapshot(Event event, AggregateState state, Class<?> type, long version) {
    assertSnapshot(event, state);
    assertThat(state.getVersion(), is(version));
    assertThat(state.getType(), is(type.getSimpleName()));
    assertThat(state.getPayload(), instanceOf(type));
  }
}
