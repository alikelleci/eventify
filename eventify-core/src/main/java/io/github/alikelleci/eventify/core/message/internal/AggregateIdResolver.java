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

  private static Field findIdField(Class<?> type) {
    List<Field> fields = FieldUtils.getFieldsListWithAnnotation(type, AggregateId.class);
    if (fields.isEmpty()) {
      throw new AggregateIdMissingException("Aggregate identifier missing in " + type.getName() + ". Please annotate your field containing the identifier with @AggregateId.");
    }
    if (fields.size() > 1) {
      throw new AggregateIdMissingException("More than one field of " + type.getName() + " is annotated with @AggregateId: " + fields.stream().map(Field::getName).toList() + ". Annotate exactly one.");
    }
    Field field = fields.get(0);
    field.setAccessible(true);
    return field;
  }

  @SneakyThrows
  private static String getFieldValue(Field field, Object target) {
    Object value = field.get(target);
    if (value == null) {
      throw new AggregateIdMissingException("Aggregate identifier cannot be null.");
    }
    return value.toString();
  }
}
