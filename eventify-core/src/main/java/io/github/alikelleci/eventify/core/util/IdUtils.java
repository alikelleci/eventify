package io.github.alikelleci.eventify.core.util;

import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.exceptions.AggregateIdMissingException;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.time.Instant;

public class IdUtils {

  /** A ULID as text is always this long. */
  private static final int ULID_LENGTH = 26;

  public static String getAggregateId(Object payload) {
    return FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class)
        .stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> getFieldValue(field, payload))
        .orElseThrow(() -> new AggregateIdMissingException("Aggregate identifier missing. Please annotate your field containing the identifier with @AggregateId."));
  }

  /** The key of a message: {@code aggregateId@ULID}. The keys of one aggregate sort in the order they were created. */
  public static String createCompoundKey(String aggregateId, Instant timestamp) {
    return aggregateId + "@" + UlidCreator.getMonotonicUlid(timestamp.toEpochMilli()).toString();
  }

  /** The start of the key range that holds an aggregate's messages. Not every key in the range is the aggregate's: see {@link #isKeyOf}. */
  public static String firstKey(String aggregateId) {
    return aggregateId + "@";
  }

  /** The end of the key range that holds an aggregate's messages: after every ULID. */
  public static String lastKey(String aggregateId) {
    return aggregateId + "@~";
  }

  /**
   * Whether a key is one of this aggregate's. The range from {@link #firstKey} to {@link #lastKey} also holds the keys of
   * an aggregate whose id is this id followed by "@", e.g. "ada@example.com@ULID" in the range of "ada": such a key
   * is longer. Only a key of exactly this aggregate id, "@" and one ULID is this aggregate's.
   */
  public static boolean isKeyOf(String aggregateId, String key) {
    return key != null
        && key.length() == aggregateId.length() + 1 + ULID_LENGTH
        && key.startsWith(firstKey(aggregateId));
  }

  @SneakyThrows
  private static String getFieldValue(Field field, Object target) {
    field.setAccessible(true);
    Object value = field.get(target);
    if (value == null) {
      throw new AggregateIdMissingException("Aggregate identifier cannot be null.");
    }
    return value.toString();
  }
}
