package io.github.alikelleci.eventify.core.upcasting.internal;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcast;
import io.github.alikelleci.eventify.core.upcasting.exception.UpcastingException;
import lombok.Getter;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Getter
public class UpcasterMethod  {

  private final Object handler;
  private final Method method;
  /** From its {@link Upcast}: the class name and the revision it upcasts from. */
  private final String type;
  private final int revision;

  public UpcasterMethod(Object handler, Method method) {
    this.handler = handler;
    this.method = method;
    Upcast upcast = method.getAnnotation(Upcast.class);
    this.type = upcast.type();
    this.revision = upcast.revision();
  }

  public JsonNode handle(JsonNode jsonNode) {
    try {
      return invokeHandler(jsonNode);
    } catch (Exception e) {
      throw new UpcastingException(ExceptionUtils.getRootCauseMessage(e), ExceptionUtils.getRootCause(e));
    }
  }

  private JsonNode invokeHandler(JsonNode jsonNode) throws InvocationTargetException, IllegalAccessException {
    return (JsonNode) method.invoke(handler, jsonNode);
  }
}
