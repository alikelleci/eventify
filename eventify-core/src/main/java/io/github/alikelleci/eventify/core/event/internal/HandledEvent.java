package io.github.alikelleci.eventify.core.event.internal;

import io.github.alikelleci.eventify.core.event.Event;

import java.util.List;

/** An event and the command payloads its handlers returned, in priority order. */
public record HandledEvent(Event event, List<Object> commandPayloads) {
}
