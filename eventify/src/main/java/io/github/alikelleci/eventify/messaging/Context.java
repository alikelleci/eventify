package io.github.alikelleci.eventify.messaging;

import lombok.Value;

import java.time.Instant;

@Value
public class Context {
  String id;
  Instant timestamp;
  Metadata metadata;

  public Context(Message message) {
    this.id = message.getId();
    this.timestamp = message.getTimestamp();
    this.metadata = message.getMetadata();
  }
}
