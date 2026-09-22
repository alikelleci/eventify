package io.github.alikelleci.eventify.core.store.internal;

import java.util.Locale;

/**
 * The keys of the stores: the aggregate's name, its identifier, and for an event its sequence zero-padded to 19
 * digits, so the events of an aggregate sort the way their sequences do.
 *
 * <p>The parts are separated by a NUL, which is what makes a key range exact. The first NUL ends the name and the
 * second ends the identifier, and neither may contain one, so exactly one aggregate can be read from a key: all of an
 * aggregate's events are in the range from {@link #first} to {@link #last}, and nothing else is. A key range therefore
 * needs no filtering, whatever the names and identifiers look like.
 *
 * <p>A readable separator would not do that. With "@", the keys of the order "PO-1-7" would fall inside the range of
 * the order "PO-1", because "7" sorts between "0" and "9" — and an identifier is application data, so no character
 * that may appear in one can be used to mark where it ends.
 */
public final class StoreKeys {

  /** Between the parts of a key: the one character a name and an identifier cannot contain. */
  public static final char SEPARATOR = '\u0000';

  /** Every long fits in 19 digits. ASCII digits in every locale: another JVM locale must not give other keys. */
  private static final int SEQUENCE_LENGTH = 19;

  private StoreKeys() {
  }

  /** The key of the aggregate's event with this sequence. */
  public static String of(String aggregateType, String aggregateId, long sequence) {
    if (sequence < 1) {
      throw new IllegalArgumentException("A sequence starts at 1, not " + sequence + ".");
    }
    return snapshot(aggregateType, aggregateId) + SEPARATOR + String.format(Locale.ROOT, "%0" + SEQUENCE_LENGTH + "d", sequence);
  }

  /** The key of the aggregate's snapshot, which is also what the keys of its events start with. */
  public static String snapshot(String aggregateType, String aggregateId) {
    if (aggregateId.indexOf(SEPARATOR) >= 0) {
      throw new IllegalArgumentException("An aggregate identifier cannot contain a NUL character.");
    }
    return aggregateType + SEPARATOR + aggregateId;
  }

  /** The start of the key range that holds an aggregate's events, and nothing else. */
  public static String first(String aggregateType, String aggregateId) {
    return of(aggregateType, aggregateId, 1);
  }

  /** The end of the key range that holds an aggregate's events, and nothing else. */
  public static String last(String aggregateType, String aggregateId) {
    return of(aggregateType, aggregateId, Long.MAX_VALUE);
  }

  /** The sequence in a key of this aggregate, e.g. one that came out of a range over it. */
  public static long sequenceOf(String aggregateType, String aggregateId, String key) {
    String prefix = snapshot(aggregateType, aggregateId) + SEPARATOR;
    if (key == null || key.length() != prefix.length() + SEQUENCE_LENGTH || !key.startsWith(prefix)) {
      throw new IllegalArgumentException("Key '" + key + "' is not a key of aggregate '" + aggregateId + "'.");
    }
    try {
      return Long.parseLong(key.substring(prefix.length()));
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Key '" + key + "' is not a key of aggregate '" + aggregateId + "': its sequence is not a number.", e);
    }
  }
}
