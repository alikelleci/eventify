package io.github.alikelleci.eventify.core.store;

/**
 * The keys of the event store: {@code aggregateId@sequence}, the sequence zero-padded to 19 digits, so the keys sort
 * the way the numbers do. All of an aggregate's events are in one key range, in the order of their sequence.
 */
public final class StoreKeys {

  /** Every long fits in 19 digits. */
  private static final int SEQUENCE_LENGTH = 19;
  private static final String SEPARATOR = "@";

  private StoreKeys() {
  }

  /** The key of the aggregate's event with this sequence. */
  public static String of(String aggregateId, long sequence) {
    if (sequence < 1) {
      throw new IllegalArgumentException("A sequence starts at 1, not " + sequence + ".");
    }
    return aggregateId + SEPARATOR + String.format("%0" + SEQUENCE_LENGTH + "d", sequence);
  }

  /** The start of the key range that holds an aggregate's events. Not every key in the range is the aggregate's: see {@link #isKeyOf}. */
  public static String first(String aggregateId) {
    return of(aggregateId, 1);
  }

  /** The end of the key range that holds an aggregate's events. */
  public static String last(String aggregateId) {
    return of(aggregateId, Long.MAX_VALUE);
  }

  /**
   * Whether a key is one of this aggregate's. The range from {@link #first} to {@link #last} also holds the keys of an
   * aggregate whose id is this id, "@" and a number, e.g. "ada@1@0000000000000000001" in the range of "ada": such a
   * key is longer. Only this aggregate id, "@" and exactly 19 digits is this aggregate's.
   */
  public static boolean isKeyOf(String aggregateId, String key) {
    if (key == null
        || key.length() != aggregateId.length() + SEPARATOR.length() + SEQUENCE_LENGTH
        || !key.startsWith(aggregateId + SEPARATOR)) {
      return false;
    }
    for (int i = aggregateId.length() + SEPARATOR.length(); i < key.length(); i++) {
      char c = key.charAt(i);
      if (c < '0' || c > '9') {
        return false;
      }
    }
    return true;
  }

  /** The sequence in a key of this aggregate (see {@link #isKeyOf}). */
  public static long sequenceOf(String aggregateId, String key) {
    if (!isKeyOf(aggregateId, key)) {
      throw new IllegalArgumentException("Key '" + key + "' is not a key of aggregate '" + aggregateId + "'.");
    }
    return Long.parseLong(key.substring(aggregateId.length() + SEPARATOR.length()));
  }
}
