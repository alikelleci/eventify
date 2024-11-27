package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.common.exceptions.TopicInfoMissingException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.springframework.core.annotation.AnnotationUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.Optional;

import static io.github.alikelleci.eventify.messaging.Metadata.ID;
import static io.github.alikelleci.eventify.messaging.Metadata.TIMESTAMP;

@Getter
@ToString
@EqualsAndHashCode
public abstract class Message {
  protected String id;
  protected Instant timestamp;
  protected String type;
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  protected Object payload;
  protected Metadata metadata;

  protected Message() {
  }

  protected Message(Instant timestamp, Object payload, Metadata metadata) {
    this.payload = Optional.ofNullable(payload).orElseThrow(() -> new PayloadMissingException("Message payload is missing."));
    this.type = getPayload().getClass().getSimpleName();
    this.timestamp = Optional.ofNullable(timestamp).orElse(Instant.now());
    this.id = UlidCreator.getMonotonicUlid(getTimestamp().toEpochMilli()).toString();
    this.metadata = Optional.ofNullable(metadata).orElse(Metadata.builder().build());

    this.metadata
        .add(ID, getId())
        .add(TIMESTAMP, getTimestamp().toString());
  }


  @Transient
  public TopicInfo getTopicInfo() {
    return Optional.ofNullable(getPayload())
        .map(p -> AnnotationUtils.findAnnotation(p.getClass(), TopicInfo.class))
        .orElseThrow(() -> new TopicInfoMissingException("Topic information not found. Please annotate your payload class with @TopicInfo."));
  }

}
