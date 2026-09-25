package io.github.alikelleci.eventify.core.internal;

import java.util.Locale;

/**
 * Keys: name, id and (for an event) the sequence as 19 digits, separated by NUL. Names and ids can't hold NUL, so a
 * key range holds exactly one aggregate; with "@", "PO-1-7" would fall inside the range of "PO-1".
 */
public final class StoreKeys {

  /** Can't occur in a name or id. */
  public static final char SEPARATOR = '\u0000';

  /** Every long fits in 19 digits; Locale.ROOT keeps keys independent of the JVM locale. */
  private static final int SEQUENCE_LENGTH = 19;

  private StoreKeys() {
  }

  /** The key of the aggregate's event with this sequence. */
  public static String event(String aggregateType, String aggregateId, long sequence) {
    if (sequence < 1) {
      throw new IllegalArgumentException("A sequence starts at 1, not " + sequence + ".");
    }
    return aggregate(aggregateType, aggregateId) + SEPARATOR + String.format(Locale.ROOT, "%0" + SEQUENCE_LENGTH + "d", sequence);
  }

  /** The key of the aggregate's snapshot, which is also what the keys of its events start with. */
  public static String aggregate(String aggregateType, String aggregateId) {
    if (aggregateId.indexOf(SEPARATOR) >= 0) {
      throw new IllegalArgumentException("An aggregate identifier cannot contain a NUL character.");
    }
    return aggregateType + SEPARATOR + aggregateId;
  }

  /** The start of the key range that holds an aggregate's events, and nothing else. */
  public static String first(String aggregateType, String aggregateId) {
    return event(aggregateType, aggregateId, 1);
  }

  /** The end of the key range that holds an aggregate's events, and nothing else. */
  public static String last(String aggregateType, String aggregateId) {
    return event(aggregateType, aggregateId, Long.MAX_VALUE);
  }
}
