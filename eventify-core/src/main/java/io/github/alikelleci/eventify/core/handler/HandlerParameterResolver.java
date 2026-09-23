package io.github.alikelleci.eventify.core.handler;

import io.github.alikelleci.eventify.core.message.Message;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.MessageId;
import io.github.alikelleci.eventify.core.message.annotation.MetadataValue;
import io.github.alikelleci.eventify.core.message.annotation.Timestamp;

import java.lang.reflect.Parameter;

/**
 * The value a handler method gets for a parameter about its message: {@link Metadata}, {@link Timestamp},
 * {@link MessageId} or {@link MetadataValue}. Also for handler methods outside Eventify, e.g. Spring Kafka listeners.
 */
public final class HandlerParameterResolver {

  private HandlerParameterResolver() {
  }

  /** Whether {@link #resolve} has a value for this parameter. */
  public static boolean supports(Parameter parameter) {
    return parameter.getType() == Metadata.class
        || parameter.isAnnotationPresent(Timestamp.class)
        || parameter.isAnnotationPresent(MessageId.class)
        || parameter.isAnnotationPresent(MetadataValue.class);
  }

  /** @throws IllegalArgumentException when the parameter is not one of these */
  public static Object resolve(Parameter parameter, Message message) {
    if (parameter.getType() == Metadata.class) {
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
