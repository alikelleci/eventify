package io.github.alikelleci.eventify.util;

import com.github.f4b6a3.ulid.UlidCreator;
import io.github.alikelleci.eventify.common.annotations.AggregateId;
import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import lombok.experimental.UtilityClass;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.springframework.core.annotation.AnnotationUtils;
import org.springframework.util.ReflectionUtils;

@UtilityClass
public class CommonUtils {

  public String getAggregateId(Object payload) {
    if (payload == null) {
      return null;
    }

    return FieldUtils.getFieldsListWithAnnotation(payload.getClass(), AggregateId.class).stream()
        .filter(field -> field.getType() == String.class)
        .findFirst()
        .map(field -> {
          field.setAccessible(true);
          return (String) ReflectionUtils.getField(field, payload);
        })
        .orElse(null);
  }

  public TopicInfo getTopicInfo(Object payload) {
    if (payload == null) {
      return null;
    }
    return AnnotationUtils.findAnnotation(payload.getClass(), TopicInfo.class);
  }

  public String createMessageId(String aggregateId) {
    return aggregateId + "@" + UlidCreator.getMonotonicUlid().toString();
  }
}
