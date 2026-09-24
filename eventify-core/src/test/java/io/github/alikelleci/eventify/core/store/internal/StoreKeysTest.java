package io.github.alikelleci.eventify.core.store.internal;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@DisplayName("Store keys")
class StoreKeysTest {

  /** Spelled out here, so the test says what a key looks like without asking the class under test. */
  private static final String NUL = "\u0000";

  @Test
  @DisplayName("Should sort the keys of an aggregate as their sequences")
  void keysSortAsTheirSequences() {
    assertThat(StoreKeys.event("order", "order-1", 42)).isEqualTo("order" + NUL + "order-1" + NUL + "0000000000000000042");
    assertThat(StoreKeys.aggregate("order", "order-1")).isEqualTo("order" + NUL + "order-1");
    assertThat(StoreKeys.event("order", "order-1", 9)).isLessThan(StoreKeys.event("order", "order-1", 10));
    assertThat(StoreKeys.first("order", "order-1")).isEqualTo(StoreKeys.event("order", "order-1", 1));
    assertThat(StoreKeys.last("order", "order-1")).isEqualTo("order" + NUL + "order-1" + NUL + Long.MAX_VALUE);
  }

  @Test
  @DisplayName("Should write ASCII digits whatever the JVM's locale")
  void keysDoNotDependOnTheLocale() {
    Locale previous = Locale.getDefault();
    try {
      Locale.setDefault(Locale.forLanguageTag("ar-EG"));
      assertThat(StoreKeys.event("order", "order-1", 42)).isEqualTo("order" + NUL + "order-1" + NUL + "0000000000000000042");
    } finally {
      Locale.setDefault(previous);
    }
  }

  /** The keys of an aggregate start with the key of its snapshot, so both are found by the same two parts. */
  @Test
  @DisplayName("Should start the keys of an aggregate's events with the key of its snapshot")
  void eventKeysStartWithTheSnapshotKey() {
    assertThat(StoreKeys.event("order", "order-1", 42)).startsWith(StoreKeys.aggregate("order", "order-1"));
  }

  @Test
  @DisplayName("Should refuse a sequence below 1")
  void aSequenceStartsAtOne() {
    assertThatThrownBy(() -> StoreKeys.event("order", "order-1", 0)).isInstanceOf(IllegalArgumentException.class);
  }

  /** An aggregate's range holds only its events: not those of ids extending it ("PO-1-7"), prefixing it, or another aggregate. */
  @Test
  @DisplayName("Should keep every other aggregate out of an aggregate's key range")
  void noOtherAggregateFallsInsideTheRange() {
    String from = StoreKeys.first("order", "PO-1");
    String to = StoreKeys.last("order", "PO-1");

    List<String> others = List.of("PO-1-7", "PO-1@example.com", "PO-10", "PO-1 ", " PO-1", "PO", "P", "PO-1￿");
    for (String id : others) {
      for (long sequence : List.of(1L, 42L, Long.MAX_VALUE)) {
        assertOutside(StoreKeys.event("order", id, sequence), from, to, "order " + id + " #" + sequence);
      }
      assertOutside(StoreKeys.aggregate("order", id), from, to, "the snapshot of order " + id);
    }
    // The same identifier under another aggregate, and an aggregate whose name starts with this one.
    for (String aggregate : List.of("customer", "orders", "order-line", "o", "")) {
      assertOutside(StoreKeys.event(aggregate, "PO-1", 42), from, to, aggregate + " PO-1");
    }
    assertThat(StoreKeys.event("order", "PO-1", 42)).isBetween(from, to);
  }

  /** The separator is what keeps the parts of a key apart, so neither part can hold one. */
  @Test
  @DisplayName("Should refuse an aggregate id that contains the separator")
  void anIdThatContainsTheSeparatorIsRefused() {
    assertThatThrownBy(() -> StoreKeys.event("order", "order" + NUL + "1", 1))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("NUL");
  }

  private static void assertOutside(String key, String from, String to, String what) {
    assertThat(key.compareTo(from) < 0 || key.compareTo(to) > 0)
        .as("the key of %s is outside the range of order PO-1", what)
        .isTrue();
  }
}
