package io.github.alikelleci.eventify.core.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;

class IdUtilsTest {

  @Test
  void aKeyIsTheAggregatesOnlyWithExactlyOneUlidAfterItsId() {
    String adaKey = IdUtils.createCompoundKey("ada", Instant.now());
    String emailKey = IdUtils.createCompoundKey("ada@example.com", Instant.now());

    assertThat(IdUtils.isKeyOf("ada", adaKey)).isTrue();
    assertThat(IdUtils.isKeyOf("ada@example.com", emailKey)).isTrue();

    // In the key range of "ada", but another aggregate's
    assertThat(emailKey).startsWith(IdUtils.firstKey("ada"));
    assertThat(emailKey.compareTo(IdUtils.lastKey("ada"))).isNegative();
    assertThat(IdUtils.isKeyOf("ada", emailKey)).isFalse();

    assertThat(IdUtils.isKeyOf("ada@example.com", adaKey)).isFalse();
    assertThat(IdUtils.isKeyOf("adam", IdUtils.createCompoundKey("ada", Instant.now()))).isFalse();
    assertThat(IdUtils.isKeyOf("ada", null)).isFalse();
  }
}
