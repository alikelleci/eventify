package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.springframework.core.annotation.AnnotationUtils;

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
    this.id = id;

    this.timestamp = Optional.ofNullable(timestamp)
        .orElse(Instant.now());

    this.type = Optional.ofNullable(payload)
        .map(p -> p.getClass().getSimpleName())
        .orElse(null);

    this.payload = payload;
    this.metadata = Optional.ofNullable(metadata)
        .orElse(new Metadata());
  }

  public Metadata getMetadata() {
    return Optional.ofNullable(this.metadata)
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
