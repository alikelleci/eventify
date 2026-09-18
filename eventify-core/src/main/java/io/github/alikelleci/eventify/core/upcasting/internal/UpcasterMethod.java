package io.github.alikelleci.eventify.core.upcasting.internal;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcast;
import io.github.alikelleci.eventify.core.upcasting.exception.UpcastingException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.UnaryOperator;

@Slf4j
public class UpcasterMethod implements UnaryOperator<JsonNode> {

  private final Object target;
  private final Method method;
  /** The class name it upcasts, and the revision it upcasts from: read once, from its {@link Upcast}. */
  private final String type;
  private final int revision;

  public UpcasterMethod(Object target, Method method) {
    this.target = target;
    this.method = method;
    Upcast upcast = method.getAnnotation(Upcast.class);
    this.type = upcast.type();
    this.revision = upcast.revision();
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

  public String type() {
    return type;
  }

  public int revision() {
    return revision;
  }
}
