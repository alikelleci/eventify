package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.eventify.messaging.Message;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Singular;
import lombok.ToString;
import lombok.Value;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.TIMESTAMP;

@Value
@ToString(callSuper = true)
@EqualsAndHashCode(callSuper = true)
public class Command extends Message {
  String aggregateId;

  private Command() {
    this.aggregateId = null;
  }

  @Builder
  private Command(Instant timestamp, Object payload, @Singular("metadata") Map<String, String> metadata) {
    super(timestamp, payload, metadata);

    this.aggregateId = FieldUtils.getFieldsListWithAnnotation(getPayload().getClass(), AggregateId.class)
        .stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> {
          field.setAccessible(true);
          return (String) ReflectionUtils.getField(field, getPayload());
        })
        .orElseThrow(() -> new AggregateIdMissingException("Aggregate identifier missing. Please annotate your field containing the identifier with @AggregateId."));

    this.id = this.aggregateId + "@" + getId();

    this.metadata = extendMetadata(metadata);
  }

  private Map<String, String> extendMetadata(Map<String, String> metadata) {
    Map<String, String> map = new HashMap<>(MapUtils.emptyIfNull(metadata));
    map.put(ID, getId());
    map.put(TIMESTAMP, getTimestamp().toString());
    map.put(CORRELATION_ID, UUID.randomUUID().toString());

    return new HashMap<>(map);
  }
}
