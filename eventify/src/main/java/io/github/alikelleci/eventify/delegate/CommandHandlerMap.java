package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.experimental.Delegate;

import java.lang.reflect.Method;
import java.util.Map;

public class CommandHandlerMap implements Map<Class<?>, CommandHandler> {

  @Delegate
  private Map<Class<?>, CommandHandler> map;

  public void putHandler(Object listener) {
    HandlerUtils.findMethodsWithAnnotation(listener.getClass(), HandleCommand.class)
        .forEach(method -> add(listener, method));
  }

  private void add(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      this.put(type, new CommandHandler(listener, method));
    }
  }
}
