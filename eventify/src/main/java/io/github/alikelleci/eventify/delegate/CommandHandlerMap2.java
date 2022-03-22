package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.experimental.Delegate;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class CommandHandlerMap2 implements Map<Class<?>, CommandHandler> {

  @Delegate
  private Map<Class<?>, CommandHandler> map;

  public void put(Object listener) {
    List<Method> eventHandlerMethods = HandlerUtils.findMethodsWithAnnotation(listener.getClass(), HandleCommand.class);
    eventHandlerMethods
        .forEach(method -> addCommandHandler(listener, method));
  }

  private void addCommandHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      this.put(type, new CommandHandler(listener, method));
    }
  }

}
