package io.github.alikelleci.eventify.core.handler.internal;

import io.github.alikelleci.eventify.core.message.Message;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.MessageId;
import io.github.alikelleci.eventify.core.message.annotation.MetadataValue;
import io.github.alikelleci.eventify.core.message.annotation.Timestamp;

import java.lang.reflect.Parameter;

public interface HandlerParameterResolver {

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
