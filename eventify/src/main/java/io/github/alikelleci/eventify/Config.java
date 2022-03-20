package io.github.alikelleci.eventify;

import io.github.alikelleci.eventify.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.messaging.upcasting.Upcaster;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.kafka.streams.KafkaStreams.StateListener;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

@Slf4j
public class Config {

  public final Handlers handlers = new Handlers();
  public final Topics topics = new Topics();

  public StateListener stateListener = (newState, oldState) ->
      log.warn("State changed from {} to {}", oldState, newState);

  public StreamsUncaughtExceptionHandler uncaughtExceptionHandler = (throwable) ->
      StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;

  public boolean deleteEventsOnSnapshot;


  public static class Handlers {
    public final MultiValuedMap<String, Upcaster> UPCASTERS = new ArrayListValuedHashMap<>();
    public final Map<Class<?>, CommandHandler> COMMAND_HANDLERS = new HashMap<>();
    public final Map<Class<?>, EventSourcingHandler> EVENT_SOURCING_HANDLERS = new HashMap<>();
    public final MultiValuedMap<Class<?>, ResultHandler> RESULT_HANDLERS = new ArrayListValuedHashMap<>();
    public final MultiValuedMap<Class<?>, EventHandler> EVENT_HANDLERS = new ArrayListValuedHashMap<>();
  }

  static class Topics {
    public final Set<String> COMMANDS = new HashSet<>();
    public final Set<String> EVENTS = new HashSet<>();
    public final Set<String> RESULTS = new HashSet<>();
  }
}
