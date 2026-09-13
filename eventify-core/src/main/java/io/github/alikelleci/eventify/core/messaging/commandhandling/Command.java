package io.github.alikelleci.eventify.core.messaging.commandhandling;

import io.github.alikelleci.eventify.core.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.core.messaging.Message;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.util.IdUtils;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import static io.github.alikelleci.eventify.core.messaging.Metadata.CORRELATION_ID;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class Command implements Message {
  String id;
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  String aggregateId;

  @Builder
  private Command(Instant timestamp, Object payload, Metadata metadata) {
    this.timestamp = Optional.ofNullable(timestamp).orElse(Instant.now());
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.builder().build());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateId = IdUtils.getAggregateId(getPayload());
    this.id = IdUtils.createCompoundKey(getAggregateId(), getTimestamp());

    getMetadata().putIfAbsent(CORRELATION_ID, UUID.randomUUID().toString());
  }

  public static class CommandBuilder {
    Metadata.MetadataBuilder metadataBuilder = Metadata.builder();

    public CommandBuilder metadata(String key, String value) {
      metadataBuilder = metadataBuilder.put(key, value);
      return this;
    }

    public CommandBuilder metadata(Map<String, String> metadata) {
      if (metadata != null) {
        metadataBuilder = metadataBuilder.putAll(metadata);
      }
      return this;
    }

    public Command build() {
      Metadata metadata = metadataBuilder.build();
      return new Command(timestamp, payload, metadata);
    }
  }

}
