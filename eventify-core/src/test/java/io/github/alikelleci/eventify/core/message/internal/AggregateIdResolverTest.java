package io.github.alikelleci.eventify.core.message.internal;

import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMissingException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Aggregate ids")
class AggregateIdResolverTest {

  record ByUuid(@AggregateId UUID id) {
  }

  record ByNumber(@AggregateId long id) {
  }

  record TwoIds(@AggregateId String id, @AggregateId String otherId) {
  }

  record NoId(String id) {
  }

  @Test
  @DisplayName("Should take an aggregate id of any type as its text")
  void anAggregateIdOfAnyTypeIsItsText() {
    UUID uuid = UUID.randomUUID();

    assertThat(AggregateIdResolver.getAggregateId(new ByUuid(uuid))).isEqualTo(uuid.toString());
    assertThat(AggregateIdResolver.getAggregateId(new ByNumber(42))).isEqualTo("42");
  }

  @Test
  @DisplayName("Should refuse a message without an aggregate id, or with more than one")
  void exactlyOneAggregateIdIsRequired() {
    assertThatThrownBy(() -> AggregateIdResolver.getAggregateId(new NoId("a")))
        .isInstanceOf(AggregateIdMissingException.class)
        .hasMessageContaining("has no field annotated with @AggregateId");
    assertThatThrownBy(() -> AggregateIdResolver.getAggregateId(new TwoIds("a", "b")))
        .isInstanceOf(AggregateIdMissingException.class)
        .hasMessageContaining("more than one @AggregateId field");
  }
}
