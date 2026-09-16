package io.github.alikelleci.eventify.core.util;

import com.github.f4b6a3.ulid.Ulid;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

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
  void theNextEventKeyIsAfterTheLastKeyWhateverThisClockSays() {
    String lastKey = IdUtils.firstKey("ada") + Ulid.fast().toString().replaceFirst("^.", "7"); // far in the future

    String next = IdUtils.nextEventKey("ada", lastKey);
    String afterNext = IdUtils.nextEventKey("ada", next);

    assertThat(IdUtils.isKeyOf("ada", next)).isTrue();
    assertThat(next).isGreaterThan(lastKey);
    assertThat(afterNext).isGreaterThan(next);
    assertThat(IdUtils.nextEventKey("ada", null)).startsWith(IdUtils.firstKey("ada"));
  }
}
