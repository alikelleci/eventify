package io.github.alikelleci.eventify.core.handler;

import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.MessageId;
import io.github.alikelleci.eventify.core.message.annotation.MetadataValue;
import io.github.alikelleci.eventify.core.message.annotation.Timestamp;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Parameter;
import java.time.Instant;
import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** What a handler method gets for each parameter about its message. */
@DisplayName("Handler parameter resolver")
class HandlerParameterResolverTest {

  record OrderPlaced(@AggregateId String id) {
  }

  /** A handler method with every kind of parameter; the constants below are their positions. */
  @SuppressWarnings("unused")
  static void handler(OrderPlaced event,
                      Metadata metadata,
                      @Timestamp Instant timestamp,
                      @MessageId String messageId,
                      @MetadataValue("userId") String userId,
                      @MetadataValue("missing") String missing,
                      @MetadataValue Metadata allMetadata,
                      String unsupported) {
  }

  private static final int METADATA = 1;
  private static final int TIMESTAMP = 2;
  private static final int MESSAGE_ID = 3;
  private static final int USER_ID = 4;
  private static final int MISSING = 5;
  private static final int ALL_METADATA = 6;
  private static final int UNSUPPORTED = 7;

  private final Instant timestamp = Instant.parse("2026-09-19T10:15:30Z");
  private final Event event = Event.builder()
      .timestamp(timestamp)
      .payload(new OrderPlaced("order-1"))
      .metadata("userId", "ada")
      .build();

  @Test
  @DisplayName("Should give the message's metadata")
  void metadata() {
    assertThat(resolve(METADATA)).isSameAs(event.getMetadata());
  }

  @Test
  @DisplayName("Should give the message's timestamp for @Timestamp")
  void timestamp() {
    assertThat(resolve(TIMESTAMP)).isEqualTo(timestamp);
  }

  @Test
  @DisplayName("Should give the message's id for @MessageId")
  void messageId() {
    assertThat(resolve(MESSAGE_ID)).isEqualTo(event.getId());
  }

  @Test
  @DisplayName("Should give a metadata value for @MetadataValue with a key, null when it is missing")
  void metadataValue() {
    assertThat(resolve(USER_ID)).isEqualTo("ada");
    assertThat(resolve(MISSING)).isNull();
  }

  @Test
  @DisplayName("Should give all metadata for @MetadataValue without a key")
  void allMetadata() {
    assertThat(resolve(ALL_METADATA)).isSameAs(event.getMetadata());
  }

  @Test
  @DisplayName("Should refuse a parameter that is not about the message")
  void unsupported() {
    assertThatThrownBy(() -> resolve(UNSUPPORTED))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Unsupported parameter");
  }

  private Object resolve(int position) {
    return HandlerParameterResolver.resolve(handlerParameters()[position], event);
  }

  private static Parameter[] handlerParameters() {
    return Arrays.stream(HandlerParameterResolverTest.class.getDeclaredMethods())
        .filter(method -> method.getName().equals("handler"))
        .findFirst()
        .orElseThrow()
        .getParameters();
  }
}
