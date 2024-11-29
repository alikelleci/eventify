package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.common.exceptions.TopicInfoMissingException;
import org.springframework.core.annotation.AnnotationUtils;

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
  default TopicInfo getTopicInfo() {
    return Optional.ofNullable(getPayload())
        .map(p -> AnnotationUtils.findAnnotation(p.getClass(), TopicInfo.class))
        .orElseThrow(() -> new TopicInfoMissingException("Topic information not found. Please annotate your payload class with @TopicInfo."));
  }

}
