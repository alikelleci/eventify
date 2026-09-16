package io.github.alikelleci.eventify.core.messaging.upcasting;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.eventify.core.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.core.messaging.upcasting.exceptions.UpcastingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.UnaryOperator;

@Slf4j
public class Upcaster implements UnaryOperator<JsonNode> {

  private final Object target;
  private final Method method;

  public Upcaster(Object target, Method method) {
    this.target = target;
    this.method = method;
  }

  @Override
  public JsonNode apply(JsonNode jsonNode) {
    try {
      return doInvoke(jsonNode);
    } catch (Exception e) {
      throw new UpcastingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private JsonNode doInvoke(JsonNode jsonNode) throws InvocationTargetException, IllegalAccessException {
    return (JsonNode) method.invoke(target, jsonNode);
  }

  public Method getMethod() {
    return method;
  }

  /**
   * Adds an {@link Upcast} method to the upcasters, by the type it upcasts. One per type and revision: with two, the
   * chain would take one of them, depending on the order they were registered in.
   */
  public static void register(MultiValuedMap<String, Upcaster> upcasters, Object target, Method method) {
    if (method.getParameterCount() != 1) {
      return;
    }
    Upcast upcast = method.getAnnotation(Upcast.class);
    upcasters.get(upcast.type()).stream()
        .filter(existing -> existing.getMethod().getAnnotation(Upcast.class).revision() == upcast.revision())
        .findFirst()
        .ifPresent(existing -> {
          throw new IllegalStateException("Two upcasters for " + upcast.type() + " revision " + upcast.revision() + ": " + existing.getMethod() + " and " + method);
        });
    upcasters.put(upcast.type(), new Upcaster(target, method));
  }
}
