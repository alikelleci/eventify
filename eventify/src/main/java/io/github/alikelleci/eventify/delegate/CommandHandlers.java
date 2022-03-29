package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.util.HandlerUtils;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CommandHandlers {
  private final Map<Class<?>, CommandHandler> commandHandlers = new HashMap<>();

  public void registerHandler(Object handler) {
    List<Method> commandHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleCommand.class);

    commandHandlerMethods
        .forEach(method -> addCommandHandler(handler, method));
  }

  private void addCommandHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      commandHandlers.put(type, new CommandHandler(listener, method));
    }
  }


}
