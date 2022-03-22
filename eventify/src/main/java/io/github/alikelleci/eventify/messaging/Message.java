package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import lombok.Data;
import lombok.Getter;
import lombok.ToString;
import lombok.Value;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.Optional;

@Getter
@ToString
public class Message {
  private String id;
  private Instant timestamp;
  private String type;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  private Object payload;
  private Metadata metadata;

  protected Message(Instant timestamp, Object payload, Metadata metadata) {
    this.id = Optional.ofNullable(payload)
        .flatMap(p -> FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class).stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> {
          field.setAccessible(true);
          return (String) ReflectionUtils.getField(field, payload);
        }))
        .map(s -> s + "@" + UlidCreator.getMonotonicUlid().toString())
        .orElse(null);

    this.timestamp = Optional.ofNullable(timestamp)
        .orElse(Instant.now());;

    this.type = Optional.ofNullable(payload)
        .map(p -> p.getClass().getSimpleName())
        .orElse(null);

    this.payload = payload;

    this.metadata = Optional.ofNullable(metadata)
        .map(m -> m
            .add(Metadata.ID, this.id)
            .add(Metadata.TIMESTAMP, this.timestamp.toString()))
        .orElse(new Metadata()
            .add(Metadata.ID, this.id)
            .add(Metadata.TIMESTAMP, this.timestamp.toString()));
  }

  @Transient
  public TopicInfo getTopicInfo() {
    return Optional.ofNullable(getPayload())
        .map(p -> AnnotationUtils.findAnnotation(p.getClass(), TopicInfo.class))
        .orElse(null);
  }

}
