package io.github.alikelleci.eventify.core.message.internal;

import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMissingException;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.util.List;

/** Reads the aggregate id of a payload: the field annotated with {@link AggregateId}. */
public class AggregateIdResolver {

  private AggregateIdResolver() {
  }

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
