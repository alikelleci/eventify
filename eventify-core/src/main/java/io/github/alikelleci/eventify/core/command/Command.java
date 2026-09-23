package io.github.alikelleci.eventify.core.command;

import io.github.alikelleci.eventify.core.message.Message;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.exception.PayloadMissingException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

import static io.github.alikelleci.eventify.core.message.MetadataKeys.CORRELATION_ID;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Command implements Message {
  String id;
  /** When this command was made, by the clock of the application that made it. */
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  String aggregateId;

  @Builder
  private Command(Object payload, Metadata metadata) {
    this.timestamp = Instant.now();
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.EMPTY)
        .withDefault(CORRELATION_ID, UUID.randomUUID().toString());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateId = AggregateIdResolver.getAggregateId(getPayload());
    this.id = UUID.randomUUID().toString();
  }


}
