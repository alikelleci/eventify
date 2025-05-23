package io.github.alikelleci.eventify.core.common;

import io.github.alikelleci.eventify.core.common.annotations.MessageId;
import io.github.alikelleci.eventify.core.common.annotations.MetadataValue;
import io.github.alikelleci.eventify.core.common.annotations.Timestamp;
import io.github.alikelleci.eventify.core.messaging.Message;
import io.github.alikelleci.eventify.core.messaging.Metadata;

import java.lang.reflect.Parameter;

public interface CommonParameterResolver {

  default Object resolve(Parameter parameter, Message message) {
    if (parameter.getType().isAssignableFrom(Metadata.class)) {
      return message.getMetadata();
    } else if (parameter.isAnnotationPresent(Timestamp.class)) {
      return message.getTimestamp();
    } else if (parameter.isAnnotationPresent(MessageId.class)) {
      return message.getId();
    } else if (parameter.isAnnotationPresent(MetadataValue.class)) {
      MetadataValue annotation = parameter.getAnnotation(MetadataValue.class);
      String key = annotation.value();
      return key.isEmpty() ? message.getMetadata() : message.getMetadata().get(key);
    } else {
      throw new IllegalArgumentException("Unsupported parameter: " + parameter);
    }
  }
}
