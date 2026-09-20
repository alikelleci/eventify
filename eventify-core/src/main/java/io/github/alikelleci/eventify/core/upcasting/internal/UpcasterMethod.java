package io.github.alikelleci.eventify.core.upcasting.internal;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcast;
import io.github.alikelleci.eventify.core.upcasting.exception.UpcastingException;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

@Slf4j
@Getter
public class UpcasterMethod  {

  private final Object handler;
  private final Method method;
  /** The class name it upcasts, and the revision it upcasts from: read once, from its {@link Upcast}. */
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

  public String type() {
    return type;
  }

  public int revision() {
    return revision;
  }
}
