package io.github.alikelleci.eventify.spring.starter;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.command.gateway.CommandGateway;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.upcasting.Upcasters;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.util.ClassUtils;

import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@AutoConfiguration
public class EventifyAutoConfiguration {

  /** A new builder per injection point, with the handler beans already registered. */
  @Bean
  @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  @ConditionalOnMissingBean
  public Eventify.EventifyBuilder eventifyBuilder(ListableBeanFactory beanFactory) {
    Eventify.EventifyBuilder builder = Eventify.builder();
    EventifyHandlerBeans.of(beanFactory).forEach(builder::registerHandler);
    return builder;
  }

  /** A new builder per injection point. */
  @Bean
  @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
  @ConditionalOnMissingBean
  public CommandGateway.CommandGatewayBuilder commandGatewayBuilder() {
    return CommandGateway.builder();
  }

  /**
   * How events are read outside Eventify: by {@code @KafkaListener} methods and by projections of their own, e.g. a
   * Kafka Streams application. With one Eventify bean, as that bean reads them: its object mapper and upcasters, also
   * those registered with {@code registerHandler(...)} that are not beans.
   *
   * <p>Without an Eventify bean, the {@code @Upcaster} methods of the beans. With several, those too: which Eventify
   * bean's upcasters apply to a topic is not known here.
   *
   * <p>Lazy: made only when used, so an application that reads no events this way is left as it is.
   */
  @Bean
  @Lazy
  @ConditionalOnMissingBean(EventSerde.class)
  public EventSerde eventifyEventSerde(ObjectProvider<Eventify> apps, ListableBeanFactory beanFactory) {
    // Counted, not getIfUnique(): that picks a @Primary one and would leave out the upcasters of the others.
    List<Eventify> all = apps.stream().toList();
    if (all.size() == 1) {
      return new EventSerde(all.get(0).getObjectMapper(), all.get(0).getUpcasters());
    }
    if (all.size() > 1) {
      log.warn("Multiple Eventify beans found, the EventSerde bean uses only @Upcaster methods on Spring beans. "
          + "Upcasters registered only via registerHandler(...) are ignored.");
    }
    return new EventSerde(EventifyObjectMapper.create(), Upcasters.of(EventifyHandlerBeans.of(beanFactory)));
  }

  @Bean
  @ConditionalOnBean(Eventify.class)
  public SmartLifecycle eventifyLifecycle(List<Eventify> apps) {
    return new SmartLifecycle() {
      private volatile boolean running = false;

      @Override
      public void start() {
        requireOneEventifyPerCommand(apps);
        requireOneEventifyPerAggregate(apps);
        requireOneEventifyPerEventHandler(apps);
        apps.forEach(Eventify::start);
        running = true;
      }

      @Override
      public void stop() {
        apps.forEach(Eventify::stop);
        running = false;
      }

      @Override
      public boolean isRunning() {
        return running;
      }

      @Override
      public int getPhase() {
        return Integer.MAX_VALUE;
      }
    };
  }

  /** Otherwise the handler would handle each event once per Eventify bean. */
  private static void requireOneEventifyPerEventHandler(List<Eventify> apps) {
    Map<Object, Eventify> owners = new IdentityHashMap<>();
    for (Eventify app : apps) {
      for (Object handler : app.getEventHandlers()) {
        if (owners.putIfAbsent(handler, app) != null) {
          throw new IllegalStateException(ClassUtils.getUserClass(handler).getName() + " is registered on more than one Eventify bean; register it on one.");
        }
      }
    }
  }

  /** Otherwise each Eventify bean would keep its own history of the aggregate. */
  private static void requireOneEventifyPerAggregate(List<Eventify> apps) {
    Map<String, Eventify> owners = new HashMap<>();
    for (Eventify app : apps) {
      for (String aggregateType : app.getAggregateTypes()) {
        if (!app.getCommandTopics(aggregateType).isEmpty() && owners.putIfAbsent(aggregateType, app) != null) {
          throw new IllegalStateException("Aggregate '" + aggregateType + "' is handled by more than one Eventify bean; register all its command handlers on one.");
        }
      }
    }
  }

  /** Otherwise each Eventify bean would handle the command. */
  private static void requireOneEventifyPerCommand(List<Eventify> apps) {
    Map<Class<?>, Eventify> owners = new HashMap<>();
    for (Eventify app : apps) {
      for (Class<?> command : app.getCommandClasses()) {
        if (owners.putIfAbsent(command, app) != null) {
          throw new IllegalStateException(command.getName() + " is handled by more than one Eventify bean; register its handler on one.");
        }
      }
    }
  }
}
