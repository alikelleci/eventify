package io.github.alikelleci.eventify.common;

import io.github.alikelleci.eventify.common.annotations.MessageId;
import io.github.alikelleci.eventify.common.annotations.MetadataValue;
import io.github.alikelleci.eventify.common.annotations.Timestamp;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;

import java.lang.reflect.Parameter;

public class ParameterValueResolver {

  public static Object resolve(Parameter parameter, Message message) {
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
