package io.github.alikelleci.eventify.core.util;

import com.github.f4b6a3.ulid.Ulid;
import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.exceptions.AggregateIdMissingException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Id utils")
class IdUtilsTest {

  @Test
  void aKeyIsTheAggregatesOnlyWithExactlyOneUlidAfterItsId() {
    String adaKey = IdUtils.createCompoundKey("ada");
    String emailKey = IdUtils.createCompoundKey("ada@example.com");

    assertThat(IdUtils.isKeyOf("ada", adaKey)).isTrue();
    assertThat(IdUtils.isKeyOf("ada@example.com", emailKey)).isTrue();

    // In the key range of "ada", but another aggregate's
    assertThat(emailKey).startsWith(IdUtils.firstKey("ada"));
    assertThat(emailKey.compareTo(IdUtils.lastKey("ada"))).isNegative();
    assertThat(IdUtils.isKeyOf("ada", emailKey)).isFalse();

    assertThat(IdUtils.isKeyOf("ada@example.com", adaKey)).isFalse();
    assertThat(IdUtils.isKeyOf("adam", IdUtils.createCompoundKey("ada"))).isFalse();
    assertThat(IdUtils.isKeyOf("ada", null)).isFalse();
  }

  /** A last key from a clock ahead of this one (another host's, or before this clock went back). */
  @Test
  @DisplayName("Should give the next event key after the last key, whatever this clock says")
  void theNextEventKeyIsAfterTheLastKeyWhateverThisClockSays() {
    String lastKey = IdUtils.firstKey("ada") + Ulid.fast().toString().replaceFirst("^.", "7"); // far in the future

    String next = IdUtils.nextEventKey("ada", lastKey);
    String afterNext = IdUtils.nextEventKey("ada", next);

    assertThat(IdUtils.isKeyOf("ada", next)).isTrue();
    assertThat(next).isGreaterThan(lastKey);
    assertThat(afterNext).isGreaterThan(next);
    assertThat(IdUtils.nextEventKey("ada", null)).startsWith(IdUtils.firstKey("ada"));
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

    assertThat(IdUtils.getAggregateId(new ByUuid(uuid))).isEqualTo(uuid.toString());
    assertThat(IdUtils.getAggregateId(new ByNumber(42))).isEqualTo("42");
  }

  @Test
  @DisplayName("Should refuse a message without an aggregate id, or with more than one")
  void exactlyOneAggregateIdIsRequired() {
    assertThatThrownBy(() -> IdUtils.getAggregateId(new NoId("a")))
        .isInstanceOf(AggregateIdMissingException.class)
        .hasMessageContaining("missing");
    assertThatThrownBy(() -> IdUtils.getAggregateId(new TwoIds("a", "b")))
        .isInstanceOf(AggregateIdMissingException.class)
        .hasMessageContaining("More than one");
  }
}
