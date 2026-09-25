package io.github.alikelleci.eventify.spring.starter.kafka;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.handler.HandlerParameterResolver;
import io.github.alikelleci.eventify.core.handler.annotation.MessageId;
import io.github.alikelleci.eventify.core.handler.annotation.MetadataValue;
import io.github.alikelleci.eventify.core.handler.annotation.Timestamp;
import io.github.alikelleci.eventify.core.message.Metadata;
import org.springframework.core.MethodParameter;
import org.springframework.messaging.Message;
import org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolver;
import org.springframework.messaging.handler.invocation.MethodArgumentResolutionException;

/**
 * Resolves the parameters an Eventify event handler can have, on {@code @KafkaListener} methods: the {@code Event}
 * itself, {@code Metadata}, {@code @Timestamp}, {@code @MessageId} and {@code @MetadataValue}.
 *
 * <p>Spring Kafka uses it for every listener, so it only takes parameters of these types and annotations; a plain
 * {@code Object} or {@code Map} parameter is left to Spring Kafka.
 */
public class EventifyArgumentResolver implements HandlerMethodArgumentResolver {

  @Override
  public boolean supportsParameter(MethodParameter parameter) {
    Class<?> type = parameter.getParameterType();
    return type == Event.class
        || type == Metadata.class
        || parameter.hasParameterAnnotation(Timestamp.class)
        || parameter.hasParameterAnnotation(MessageId.class)
        || parameter.hasParameterAnnotation(MetadataValue.class);
  }

  @Override
  public Object resolveArgument(MethodParameter parameter, Message<?> message) {
    if (!(message.getHeaders().get(EventifyRecordMessageConverter.EVENT_HEADER) instanceof Event event)) {
      throw new MethodArgumentResolutionException(message, parameter,
          "The record is not an Eventify event: use containerFactory = \"eventifyListenerContainerFactory\"");
    }
    return parameter.getParameterType() == Event.class ? event : HandlerParameterResolver.resolve(parameter.getParameter(), event);
  }
}
