package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.eventhandling.annotations.HandleEvent;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.experimental.Delegate;
import org.apache.commons.collections4.MultiValuedMap;

import java.lang.reflect.Method;
import java.util.List;

public class ResultHandlerMap implements MultiValuedMap<Class<?>, ResultHandler> {

  @Delegate(excludes = ExcludedMethods.class)
  private MultiValuedMap<Class<?>, ResultHandler> map;

  @Override
  public boolean put(Class<?> type, ResultHandler handler) {
    List<Method> ResultHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleEvent.class);
    ResultHandlerMethods
        .forEach(method -> addResultHandler(handler, method));

    return true;
  }

  private void addResultHandler(Object listener, Method method) {
    if (method.getParameterCount() == 1 || method.getParameterCount() == 2) {
      Class<?> type = method.getParameters()[0].getType();
      this.put(type, new ResultHandler(listener, method));
    }
  }


  /**
   * Excluded methods that Lombok will not implement, we will implement/override these methods.
   */
  private abstract class ExcludedMethods {

    public abstract boolean put(Class<?> type, ResultHandler handler);
  }
}
