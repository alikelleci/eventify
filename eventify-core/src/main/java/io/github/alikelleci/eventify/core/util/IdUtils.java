package io.github.alikelleci.eventify.core.util;

import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.exceptions.AggregateIdMissingException;
import lombok.SneakyThrows;
import org.apache.commons.lang3.reflect.FieldUtils;

import java.lang.reflect.Field;
import java.time.Instant;

public class IdUtils {

  public static String getAggregateId(Object payload) {
    return FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class)
        .stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> getFieldValue(field, payload))
        .orElseThrow(() -> new AggregateIdMissingException("Aggregate identifier missing. Please annotate your field containing the identifier with @AggregateId."));
  }

  public static String createCompoundKey(String aggregateId, Instant timestamp) {
    return aggregateId + "@" + UlidCreator.getMonotonicUlid(timestamp.toEpochMilli()).toString();
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
