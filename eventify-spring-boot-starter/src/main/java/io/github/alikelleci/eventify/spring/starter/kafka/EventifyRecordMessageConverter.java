package io.github.alikelleci.eventify.spring.starter.kafka;

import io.github.alikelleci.eventify.core.event.Event;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.core.ResolvableType;
import org.springframework.kafka.support.converter.MessagingMessageConverter;
import org.springframework.messaging.Message;
import org.springframework.messaging.support.MessageBuilder;

import java.lang.reflect.Type;

/**
 * Gives a listener the payload of the event (e.g. {@code OrderPlaced}), unless it asks for the {@code Event} itself.
 * With the payload, a class-level {@code @KafkaListener} picks the {@code @KafkaHandler} method by the payload's type,
 * as Eventify picks its handlers.
 *
 * <p>The {@code Event} is also in the {@link #EVENT_HEADER} header, for the other parameters: {@code Metadata},
 * {@code @Timestamp}, ...
 */
public class EventifyRecordMessageConverter extends MessagingMessageConverter {

  /** The header of the Spring message that holds the {@code Event}. */
  public static final String EVENT_HEADER = "eventify_event";

  @Override
  public Message<?> toMessage(ConsumerRecord<?, ?> record, Object acknowledgment, Object consumer, Type type) {
    Message<?> message = super.toMessage(record, acknowledgment, consumer, type);
    if (!(record.value() instanceof Event event)) {
      return message;
    }
    return MessageBuilder.fromMessage(message)
        .setHeader(EVENT_HEADER, event)
        .build();
  }

  @Override
  protected Object extractAndConvertValue(ConsumerRecord<?, ?> record, Type type) {
    Object value = super.extractAndConvertValue(record, type);
    if (value instanceof Event event && !asksForEvent(type)) {
      return event.getPayload();
    }
    return value;
  }

  /** {@code Object}, the type of a class-level listener, asks for the payload. */
  private static boolean asksForEvent(Type type) {
    Class<?> rawType = type == null ? null : ResolvableType.forType(type).resolve();
    return rawType != null && Event.class.isAssignableFrom(rawType);
  }
}
