package io.github.alikelleci.eventify.core.aggregate;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.Revision;
import io.github.alikelleci.eventify.core.message.exception.PayloadMissingException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import io.github.alikelleci.eventify.core.message.internal.Revisions;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AggregateState {
  Instant timestamp;
  String type;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  Object payload;
  Metadata metadata;
  String aggregateId;
  /** The sequence of the last event applied to this state: how many events the aggregate had then. */
  long version;
  /**
   * The {@link Revision} of the aggregate class this state was made with: of its fields and its event sourcing
   * handlers. A snapshot made with another revision is not used. 0 in a snapshot made before revisions were stored: it
   * counts as 1, the revision of a class without {@code @Revision}.
   */
  int revision;

  @Builder
  private AggregateState(Instant timestamp, Object payload, Metadata metadata, long version) {
    this.timestamp = Optional.ofNullable(timestamp).orElse(Instant.now());
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.builder().build());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateId = AggregateIdResolver.getAggregateId(getPayload());
    this.version = version;
    this.revision = Revisions.of(getPayload().getClass());
  }

  public static class AggregateStateBuilder {
    Metadata.MetadataBuilder metadataBuilder = Metadata.builder();

    public AggregateStateBuilder metadata(String key, String value) {
      metadataBuilder = metadataBuilder.put(key, value);
      return this;
    }

    public AggregateStateBuilder metadata(Map<String, String> metadata) {
      if (metadata != null) {
        metadataBuilder = metadataBuilder.putAll(metadata);
      }
      return this;
    }

    public AggregateState build() {
      Metadata metadata = metadataBuilder.build();
      return new AggregateState(timestamp, payload, metadata, version);
    }
  }


  /**
   * This state, unchanged, after the event: what a handler that returns the state it is given produces. Used for an
   * event without an event sourcing handler.
   */
  public AggregateState after(Event event) {
    return AggregateState.builder()
        .timestamp(event.getTimestamp())
        .payload(payload)
        .metadata(event.getMetadata())
        .build();
  }

  /** This state, at the given version. */
  public AggregateState withVersion(long version) {
    return AggregateState.builder()
        .timestamp(timestamp)
        .payload(payload)
        .metadata(metadata)
        .version(version)
        .build();
  }
}
