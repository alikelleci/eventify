package io.github.alikelleci.eventify.migration;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The store keys of both versions. Eventify 4 keys an event {@code aggregateId@ULID} and a snapshot by its aggregate id;
 * Eventify 5 keys an event {@code type NUL id NUL sequence} (19 digits) and a snapshot {@code type NUL id}.
 */
final class Keys {

  static final char NUL = '\u0000';

  /** A ULID is 26 characters of Crockford base32 (no I, L, O or U), so the id is everything before the last 27. */
  private static final Pattern V4_EVENT = Pattern.compile("(.*)@([0-9A-HJKMNP-TV-Z]{26})", Pattern.DOTALL);

  private Keys() {
  }

  /** An Eventify 4 event key, or null when the key has another form. */
  static V4EventKey v4Event(String key) {
    Matcher matcher = V4_EVENT.matcher(key);
    return matcher.matches() ? new V4EventKey(key, matcher.group(1), matcher.group(2)) : null;
  }

  static boolean isV5(String key) {
    return key.indexOf(NUL) >= 0;
  }

  /** An Eventify 5 event key, or null when the key has another form. */
  static V5EventKey v5Event(String key) {
    int first = key.indexOf(NUL);
    int second = key.indexOf(NUL, first + 1);
    if (first < 0 || second < 0 || key.length() - second - 1 != 19 || !key.substring(second + 1).chars().allMatch(c -> c >= '0' && c <= '9')) {
      return null;
    }
    try {
      return new V5EventKey(key.substring(0, first), key.substring(first + 1, second), Long.parseLong(key.substring(second + 1)));
    } catch (NumberFormatException e) {
      return null; // 19 digits above Long.MAX_VALUE
    }
  }

  static String v5Event(String aggregateType, String aggregateId, long sequence) {
    return v5Snapshot(aggregateType, aggregateId) + NUL + String.format(Locale.ROOT, "%019d", sequence);
  }

  static String v5Snapshot(String aggregateType, String aggregateId) {
    return aggregateType + NUL + aggregateId;
  }

  /** A key as a person can read it: the NUL separators shown as ␀. */
  static String printable(String key) {
    return key.replace(NUL, '␀');
  }

  /** Sorting by {@link #ulid} as text is sorting by the time it was made; ULIDs of the same millisecond are monotonic. */
  record V4EventKey(String key, String aggregateId, String ulid) {
  }

  record V5EventKey(String aggregateType, String aggregateId, long sequence) {
  }
}
