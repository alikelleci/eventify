package io.github.alikelleci.eventify.core.message;

import com.github.f4b6a3.ulid.Ulid;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMissingException;
import io.github.alikelleci.eventify.core.message.internal.AggregateIdResolver;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Message ids")
class MessageIdsTest {

  @Test
  void aKeyIsTheAggregatesOnlyWithExactlyOneUlidAfterItsId() {
    String adaKey = MessageIds.createCompoundKey("ada");
    String emailKey = MessageIds.createCompoundKey("ada@example.com");

    assertThat(MessageIds.isKeyOf("ada", adaKey)).isTrue();
    assertThat(MessageIds.isKeyOf("ada@example.com", emailKey)).isTrue();

    // In the key range of "ada", but another aggregate's
    assertThat(emailKey).startsWith(MessageIds.firstKey("ada"));
    assertThat(emailKey.compareTo(MessageIds.lastKey("ada"))).isNegative();
    assertThat(MessageIds.isKeyOf("ada", emailKey)).isFalse();

    assertThat(MessageIds.isKeyOf("ada@example.com", adaKey)).isFalse();
    assertThat(MessageIds.isKeyOf("adam", MessageIds.createCompoundKey("ada"))).isFalse();
    assertThat(MessageIds.isKeyOf("ada", null)).isFalse();
  }

  /** A last key from a clock ahead of this one (another host's, or before this clock went back). */
  @Test
  @DisplayName("Should give the next event key after the last key, whatever this clock says")
  void theNextEventKeyIsAfterTheLastKeyWhateverThisClockSays() {
    String lastKey = MessageIds.firstKey("ada") + Ulid.fast().toString().replaceFirst("^.", "7"); // far in the future

    String next = MessageIds.nextEventKey("ada", lastKey);
    String afterNext = MessageIds.nextEventKey("ada", next);

    assertThat(MessageIds.isKeyOf("ada", next)).isTrue();
    assertThat(next).isGreaterThan(lastKey);
    assertThat(afterNext).isGreaterThan(next);
    assertThat(MessageIds.nextEventKey("ada", null)).startsWith(MessageIds.firstKey("ada"));
  }

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
        .hasMessageContaining("missing");
    assertThatThrownBy(() -> AggregateIdResolver.getAggregateId(new TwoIds("a", "b")))
        .isInstanceOf(AggregateIdMissingException.class)
        .hasMessageContaining("More than one");
  }
}
