package io.github.alikelleci.eventify.core.message;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.eventify.core.handler.internal.AnnotationScanner;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import io.github.alikelleci.eventify.core.message.exception.TopicMissingException;

import java.beans.Transient;
import java.time.Instant;
import java.util.Optional;


public interface Message {
  String getId();

  Instant getTimestamp();

  String getType();

  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, property = "@class")
  Object getPayload();

  Metadata getMetadata();

  @Transient
  default Topic getTopic() {
    return Optional.ofNullable(getPayload())
        .map(p -> AnnotationScanner.findAnnotation(p.getClass(), Topic.class))
        .orElseThrow(() -> new TopicMissingException("Topic information not found. Please annotate your payload class with @Topic."));
  }

}
