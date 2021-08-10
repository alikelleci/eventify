package io.github.alikelleci.eventify.constants;

import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;

import java.util.HashMap;
import java.util.Map;

public class Handlers {
  public static MultiValuedMap<String, Upcaster> UPCASTERS = new ArrayListValuedHashMap<>();
  public static Map<Class<?>, CommandHandler> COMMAND_HANDLERS = new HashMap<>();
  public static Map<Class<?>, EventSourcingHandler> EVENTSOURCING_HANDLERS = new HashMap<>();
  public static MultiValuedMap<Class<?>, ResultHandler> RESULT_HANDLERS = new ArrayListValuedHashMap<>();
  public static MultiValuedMap<Class<?>, EventHandler> EVENT_HANDLERS = new ArrayListValuedHashMap<>();
}
