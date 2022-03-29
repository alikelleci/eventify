package io.github.alikelleci.eventify.delegate;

import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.util.HandlerUtils;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.lang.reflect.Method;
import java.util.List;

public class Upcasters {
  private final MultiValuedMap<String, Upcaster> upcasters = new ArrayListValuedHashMap<>();

  public void registerHandler(Object handler) {
    List<Method> upcasterMethods = HandlerUtils.findMethodsWithAnnotation(handler.getClass(), Upcast.class);

    upcasterMethods
        .forEach(method -> addUpcaster(handler, method));
  }

  private void addUpcaster(Object listener, Method method) {
    if (method.getParameterCount() == 1) {
      String type = method.getAnnotation(Upcast.class).type();
      upcasters.put(type, new Upcaster(listener, method));
    }
  }

}
