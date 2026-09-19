package io.github.alikelleci.eventify.core.store;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Store keys")
class StoreKeysTest {

  @Test
  @DisplayName("Should sort the keys of an aggregate as their sequences")
  void keysSortAsTheirSequences() {
    assertThat(StoreKeys.of("order-1", 42)).isEqualTo("order-1@0000000000000000042");
    assertThat(StoreKeys.of("order-1", 9)).isLessThan(StoreKeys.of("order-1", 10));
    assertThat(StoreKeys.first("order-1")).isEqualTo(StoreKeys.of("order-1", 1));
    assertThat(StoreKeys.last("order-1")).isEqualTo("order-1@" + Long.MAX_VALUE);
    assertThat(StoreKeys.sequenceOf("order-1", StoreKeys.of("order-1", 42))).isEqualTo(42);
  }

  @Test
  @DisplayName("Should refuse a sequence below 1")
  void aSequenceStartsAtOne() {
    assertThatThrownBy(() -> StoreKeys.of("order-1", 0)).isInstanceOf(IllegalArgumentException.class);
  }

  /** "ada@1" is another aggregate, whose keys are in the key range of "ada". */
  @Test
  @DisplayName("Should take a key as the aggregate's only with exactly 19 digits after its id")
  void aKeyIsTheAggregatesOnlyWithExactly19DigitsAfterItsId() {
    String adaKey = StoreKeys.of("ada", 1);
    String otherKey = StoreKeys.of("ada@1", 1);

    assertThat(otherKey).isBetween(StoreKeys.first("ada"), StoreKeys.last("ada"));
    assertThat(StoreKeys.isKeyOf("ada", adaKey)).isTrue();
    assertThat(StoreKeys.isKeyOf("ada", otherKey)).isFalse();
    assertThat(StoreKeys.isKeyOf("ada@1", otherKey)).isTrue();
    assertThat(StoreKeys.isKeyOf("ada@1", adaKey)).isFalse();
    assertThat(StoreKeys.isKeyOf("adam", adaKey)).isFalse();
    assertThat(StoreKeys.isKeyOf("ada", "ada@000000000000000000x")).isFalse();
    assertThat(StoreKeys.isKeyOf("ada", null)).isFalse();
  }

  /** An e-mail address as aggregate id: its keys sort after every sequence of the shorter id. */
  @Test
  @DisplayName("Should keep an id with @ and letters out of the key range of the shorter id")
  void anIdWithAtAndLettersIsOutsideTheRange() {
    assertThat(StoreKeys.of("ada@example.com", 1)).isGreaterThan(StoreKeys.last("ada"));
  }
}
