package io.github.alikelleci.eventify.experimental.messages;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.Optional;

@Getter
@ToString()
@EqualsAndHashCode()
public abstract class Message {
  private String id;
  private Instant timestamp;
  private String type;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private Object payload;
  private Metadata metadata;



  protected Message(String id, Instant timestamp, Object payload, Metadata metadata) {
    this.id = Optional.ofNullable(id)
        .orElse(createMessageId(payload));

    this.timestamp = Optional.ofNullable(timestamp)
        .orElse(Instant.now());

    this.type = Optional.ofNullable(payload)
        .map(p -> p.getClass().getSimpleName())
        .orElse(null);

    this.payload = payload;

    this.metadata = Optional.ofNullable(metadata)
        .map(m -> m.toBuilder()
            .messageId(this.id)
            .timestamp(this.timestamp)
            .build())
        .orElse(Metadata.builder()
            .messageId(this.id)
            .timestamp(this.timestamp)
            .build());
  }

  @Transient
  public TopicInfo getTopicInfo() {
    return Optional.ofNullable(getPayload())
        .map(p -> AnnotationUtils.findAnnotation(p.getClass(), TopicInfo.class))
        .orElse(null);
  }

  private String createMessageId(Object payload) {
    return Optional.ofNullable(payload).flatMap(p -> FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class).stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> {
          field.setAccessible(true);
          return (String) ReflectionUtils.getField(field, payload);
        }))
        .map(aggregateId -> aggregateId + "@" + UlidCreator.getMonotonicUlid().toString())
        .orElse(null);
  }

}
