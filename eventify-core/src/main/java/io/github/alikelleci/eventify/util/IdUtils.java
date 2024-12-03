package io.github.alikelleci.eventify.util;

import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.util.ReflectionUtils;

import java.time.Instant;

public class IdUtils {

  public static String getAggregateId(Object payload) {
    return FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class)
        .stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> {
          field.setAccessible(true);
          return (String) ReflectionUtils.getField(field, payload);
        })
        .orElseThrow(() -> new AggregateIdMissingException("Aggregate identifier missing. Please annotate your field containing the identifier with @AggregateId."));
  }

  public static String createCompoundKey(String aggregateId, Instant timestamp) {
    return aggregateId + "@" + UlidCreator.getMonotonicUlid(timestamp.toEpochMilli()).toString();
  }
}
