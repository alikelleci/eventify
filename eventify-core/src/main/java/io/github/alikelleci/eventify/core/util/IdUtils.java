package io.github.alikelleci.eventify.core.util;

import com.github.f4b6a3.ulid.Ulid;
import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.exceptions.AggregateIdMissingException;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.util.List;

public class IdUtils {

  /** A ULID as text is always this long. */
  private static final int ULID_LENGTH = 26;

  /**
   * The value of the one field annotated with {@link AggregateId}, as text. The field may have any type (e.g. a
   * {@code UUID} or a {@code long}): its {@code toString()} is the identifier.
   */
  public static String getAggregateId(Object payload) {
    List<Field> fields = FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class);
    if (fields.isEmpty()) {
      throw new AggregateIdMissingException("Aggregate identifier missing in " + payload.getClass().getName() + ". Please annotate your field containing the identifier with @AggregateId.");
    }
    if (fields.size() > 1) {
      throw new AggregateIdMissingException("More than one field of " + payload.getClass().getName() + " is annotated with @AggregateId: " + fields.stream().map(Field::getName).toList() + ". Annotate exactly one.");
    }
    return getFieldValue(fields.get(0), payload);
  }

  /**
   * A new key of a message: {@code aggregateId@ULID}, with the ULID from this host's clock, not from the message's
   * timestamp. That is not enough to keep an aggregate's events in order (clocks of hosts differ, and can go back):
   * events get their keys from {@link #nextEventKey}.
   */
  public static String createCompoundKey(String aggregateId) {
    return firstKey(aggregateId) + UlidCreator.getMonotonicUlid();
  }

  /**
   * The key of an aggregate's next event: after its last stored event, whatever this host's clock says.
   *
   * @param lastKey the key of the aggregate's last stored event; {@code null} when it has none
   */
  public static String nextEventKey(String aggregateId, String lastKey) {
    Ulid next = UlidCreator.getMonotonicUlid();
    if (lastKey != null) {
      Ulid last = Ulid.from(lastKey.substring(firstKey(aggregateId).length()));
      if (next.compareTo(last) <= 0) {
        next = last.increment();
      }
    }
    return firstKey(aggregateId) + next;
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
