package io.github.alikelleci.eventify.messaging;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.common.exceptions.TopicInfoMissingException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.springframework.core.annotation.AnnotationUtils;

import java.beans.Transient;
import java.time.Instant;
import java.util.Optional;

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


  @Transient
  public TopicInfo getTopicInfo() {
    return Optional.ofNullable(getPayload())
        .map(p -> AnnotationUtils.findAnnotation(p.getClass(), TopicInfo.class))
        .orElseThrow(() -> new TopicInfoMissingException("Topic information not found. Please annotate your payload class with @TopicInfo."));
  }

}
