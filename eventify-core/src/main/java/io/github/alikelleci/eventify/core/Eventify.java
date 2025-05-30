package io.github.alikelleci.eventify.core;

import io.github.alikelleci.eventify.core.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandHandler;
import io.github.alikelleci.eventify.core.messaging.eventhandling.EventHandler;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.EventSourcingHandler;
import io.github.alikelleci.eventify.core.messaging.resulthandling.ResultHandler;
import io.github.alikelleci.eventify.core.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.core.util.AnnotationUtils;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.MultiValuedMap;


public interface Eventify {

  void start();

  void stop();

  Map<Class<?>, CommandHandler> getCommandHandlers();

  MultiValuedMap<Class<?>, EventHandler> getEventHandlers();

  Map<Class<?>, EventSourcingHandler> getEventSourcingHandlers();

  MultiValuedMap<Class<?>, ResultHandler> getResultHandlers();

  MultiValuedMap<String, Upcaster> getUpcasters();

  default Set<String> getCommandTopics() {
    return getCommandHandlers().keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  default Set<String> getEventTopics() {
    return Stream.of(
            getEventHandlers().keySet(),
            getEventSourcingHandlers().keySet()
        )
        .flatMap(Collection::stream)
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .collect(Collectors.toSet());
  }

  default Set<String> getResultTopics() {
    return getResultHandlers().keySet().stream()
        .map(aClass -> AnnotationUtils.findAnnotation(aClass, TopicInfo.class))
        .filter(Objects::nonNull)
        .map(TopicInfo::value)
        .map(topic -> topic.concat(".results"))
        .collect(Collectors.toSet());
  }
}
