package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.commandhandling.annotations.HandleCommand;
import io.github.alikelleci.eventify.util.HandlerUtils;
import lombok.experimental.Delegate;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

public class CommandHandlerMap implements Map<Class<?>, CommandHandler> {

  @Delegate(excludes = ExcludedMethods.class)
  private Map<Class<?>, CommandHandler> map;

  @Override
  public CommandHandler put(Class<?> type, CommandHandler handler) {
    List<Method> eventHandlerMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), HandleCommand.class);
    eventHandlerMethods
        .forEach(method -> addCommandHandler(handler, method));

    return handler;
  }

  private void addCommandHandler(Object listener, Method method) {
    if (method.getParameterCount() == 2 || method.getParameterCount() == 3) {
      Class<?> type = method.getParameters()[0].getType();
      this.put(type, new CommandHandler(listener, method));
    }
  }


  /**
   * Excluded methods that Lombok will not implement, we will implement/override these methods.
   */
  private abstract class ExcludedMethods {

    public abstract boolean put(Class<?> type, CommandHandler handler);
  }
}
