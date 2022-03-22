package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.messaging.resulthandling.annotations.HandleResult;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.experimental.Delegate;
import org.apache.commons.collections4.MultiValuedMap;

import java.lang.reflect.Method;

public class ResultHandlerMap implements MultiValuedMap<Class<?>, ResultHandler> {

  @Delegate
  private MultiValuedMap<Class<?>, ResultHandler> map;

  public boolean add(Object listener) {
    HandlerUtils.findMethodsWithAnnotation(listener.getClass(), HandleResult.class)
        .forEach(method -> addHandler(listener, method));

    return true;
  }

  private void addHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      this.put(type, new ResultHandler(listener, method));
    }
  }
}
