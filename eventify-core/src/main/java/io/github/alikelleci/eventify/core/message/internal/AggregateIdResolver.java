package io.github.alikelleci.eventify.core.message.internal;

import io.github.alikelleci.eventify.core.internal.reflection.PerClass;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.exception.AggregateIdMissingException;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.util.List;
import java.util.function.Function;

/** Reads the aggregate id of a payload: the field annotated with {@link AggregateId}. */
public class AggregateIdResolver {

  /** The id field, per payload class. */
  private static final Function<Class<?>, Field> ID_FIELD = PerClass.of(AggregateIdResolver::findIdField);

  private AggregateIdResolver() {
  }

  /** The {@code toString()} of the one {@link AggregateId} field, e.g. a {@code UUID} or {@code long}. */
  public static String getAggregateId(Object payload) {
    return getFieldValue(ID_FIELD.apply(payload.getClass()), payload);
  }

  private static Field findIdField(Class<?> payloadClass) {
    List<Field> fields = FieldUtils.getFieldsListWithAnnotation(payloadClass, AggregateId.class);
    if (fields.isEmpty()) {
      throw new AggregateIdMissingException(payloadClass.getName() + " has no field annotated with @AggregateId.");
    }
    if (fields.size() > 1) {
      throw new AggregateIdMissingException(payloadClass.getName() + " has more than one @AggregateId field: " + fields.stream().map(Field::getName).toList());
    }
    Field field = fields.get(0);
    field.setAccessible(true);
    return field;
  }

  @SneakyThrows
  private static String getFieldValue(Field field, Object target) {
    Object value = field.get(target);
    if (value == null) {
      throw new AggregateIdMissingException("The @AggregateId field " + field.getName() + " of " + target.getClass().getName() + " is null.");
    }
    return value.toString();
  }
}
