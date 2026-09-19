package io.github.alikelleci.eventify.core.aggregate;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.annotation.Revision;
import io.github.alikelleci.eventify.core.internal.reflection.AnnotationScanner;
import io.github.alikelleci.eventify.core.message.Message;
import io.github.alikelleci.eventify.core.message.MessageIds;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.exception.PayloadMissingException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Value;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

@Value
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class AggregateState implements Message {
  String id;
  Instant timestamp;
  String type;
  Object payload;
  Metadata metadata;
  String aggregateId;
  String eventId;
  long version;
  /**
   * The {@link Revision} of the aggregate class this state was made with: of its fields and its event sourcing
   * handlers. A snapshot made with another revision is not used. 0 in a snapshot made before revisions were stored: it
   * counts as 1, the revision of a class without {@code @Revision}.
   */
  int revision;

  @Builder
  private AggregateState(Instant timestamp, Object payload, Metadata metadata, String eventId, long version) {
    this.timestamp = Optional.ofNullable(timestamp).orElse(Instant.now());
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.builder().build());

    this.type = getPayload().getClass().getSimpleName();
    this.aggregateId = AggregateIdResolver.getAggregateId(getPayload());
    this.id = MessageIds.createCompoundKey(getAggregateId());

    this.eventId = eventId;
    this.version = version;
    this.revision = revisionOf(getPayload().getClass());
  }

  /** The {@link Revision} of an aggregate class; 1 without the annotation. */
  public static int revisionOf(Class<?> aggregateType) {
    return Optional.ofNullable(AnnotationScanner.findAnnotation(aggregateType, Revision.class))
        .map(Revision::value)
        .orElse(1);
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
      return new AggregateState(timestamp, payload, metadata, eventId, version);
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
        .eventId(event.getId())
        .build();
  }

  /** This state, at the given version. */
  public AggregateState withVersion(long version) {
    return AggregateState.builder()
        .timestamp(timestamp)
        .payload(payload)
        .metadata(metadata)
        .eventId(eventId)
        .version(version)
        .build();
  }
}
