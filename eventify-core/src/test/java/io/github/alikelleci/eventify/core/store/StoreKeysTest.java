package io.github.alikelleci.eventify.core.store;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Store keys")
class StoreKeysTest {

  @Test
  @DisplayName("Should sort the keys of an aggregate as their sequences")
  void keysSortAsTheirSequences() {
    assertThat(StoreKeys.of("order-1", 42)).isEqualTo("order-1\u00000000000000000000042");
    assertThat(StoreKeys.of("order-1", 9)).isLessThan(StoreKeys.of("order-1", 10));
    assertThat(StoreKeys.first("order-1")).isEqualTo(StoreKeys.of("order-1", 1));
    assertThat(StoreKeys.last("order-1")).isEqualTo("order-1\u0000" + Long.MAX_VALUE);
    assertThat(StoreKeys.sequenceOf("order-1", StoreKeys.of("order-1", 42))).isEqualTo(42);
  }

  @Test
  @DisplayName("Should refuse a sequence below 1")
  void aSequenceStartsAtOne() {
    assertThatThrownBy(() -> StoreKeys.of("order-1", 0)).isInstanceOf(IllegalArgumentException.class);
  }

  /**
   * The range of an aggregate holds its events and nothing else, whatever the other identifiers look like: one that
   * starts with this one and a digit ("ada@1" was in the range when the separator was "@"), one that starts with it
   * and letters, one that is a prefix of it, and one that only shares its start.
   */
  @Test
  @DisplayName("Should keep every other aggregate out of an aggregate's key range")
  void noOtherAggregateFallsInsideTheRange() {
    String from = StoreKeys.first("ada");
    String to = StoreKeys.last("ada");

    for (String other : List.of("ada@1", "ada@example.com", "ada1", "adam", "ad", "a", "ada ", " ada", "adaZ", "ada\uffff")) {
      for (long sequence : List.of(1L, 42L, Long.MAX_VALUE)) {
        String key = StoreKeys.of(other, sequence);
        assertThat(key.compareTo(from) < 0 || key.compareTo(to) > 0)
            .as("key of '%s' #%s is outside the range of 'ada'", other, sequence)
            .isTrue();
      }
    }
    assertThat(StoreKeys.of("ada", 42)).isBetween(from, to);
  }

  /** The separator is what makes a range exact, so an identifier that holds one cannot be stored. */
  @Test
  @DisplayName("Should refuse an aggregate id that contains the separator")
  void anIdThatContainsTheSeparatorIsRefused() {
    assertThatThrownBy(() -> StoreKeys.of("ada\u00001", 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("NUL");
  }

  @Test
  @DisplayName("Should refuse to read a sequence from a key of another aggregate")
  void aKeyOfAnotherAggregateHasNoSequenceHere() {
    assertThatThrownBy(() -> StoreKeys.sequenceOf("ada", StoreKeys.of("adam", 1))).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> StoreKeys.sequenceOf("ada", "ada@0000000000000000001")).isInstanceOf(IllegalArgumentException.class);
    assertThatThrownBy(() -> StoreKeys.sequenceOf("ada", null)).isInstanceOf(IllegalArgumentException.class);
  }

}
