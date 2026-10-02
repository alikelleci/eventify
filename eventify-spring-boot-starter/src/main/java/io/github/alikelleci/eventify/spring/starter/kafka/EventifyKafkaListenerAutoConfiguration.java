package io.github.alikelleci.eventify.spring.starter.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.kafka.KafkaClientConfigs;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.upcasting.Upcasters;
import io.github.alikelleci.eventify.spring.starter.EventifyHandlerBeans;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.ConfigurationCondition;
import org.springframework.kafka.annotation.KafkaListenerConfigurer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * Lets {@code @KafkaListener} methods read Eventify's event topics, next to or instead of {@code @EventHandler}: each
 * listener with its own consumer group, concurrency and Spring Kafka error handling. An exception in a listener then
 * doesn't stop Kafka Streams, and command handling with it.
 *
 * <p>Commands are not handled this way: their handling needs Eventify's event store, and writes the events and the
 * result in one transaction with it.
 *
 * <p>Needs {@code @EnableKafka}, which Spring Boot's Kafka auto-configuration already sets.
 */
@Slf4j
@AutoConfiguration(afterName = "org.springframework.boot.kafka.autoconfigure.KafkaAutoConfiguration")
@ConditionalOnClass(KafkaListenerContainerFactory.class)
public class EventifyKafkaListenerAutoConfiguration {

  @Bean
  public KafkaListenerConfigurer eventifyKafkaListenerConfigurer() {
    return registrar -> registrar.setCustomMethodArgumentResolvers(new EventifyArgumentResolver());
  }

  /** Used with {@code @KafkaListener(containerFactory = "eventifyListenerContainerFactory")}. */
  @Bean
  @ConditionalOnMissingBean(name = "eventifyListenerContainerFactory")
  @Conditional(KafkaIsConfigured.class)
  public ConcurrentKafkaListenerContainerFactory<String, Event> eventifyListenerContainerFactory(
      ObjectProvider<Eventify> apps,
      ObjectProvider<ConsumerFactory<?, ?>> consumerFactories,
      ObjectProvider<CommonErrorHandler> errorHandlers,
      ListableBeanFactory beanFactory) {
    ObjectMapper objectMapper = objectMapper(apps);
    return containerFactory(consumerConfig(apps, consumerFactories), errorHandlers,
        new EventSerde(objectMapper, upcasters(apps, beanFactory)).deserializer());
  }

  /**
   * The Eventify bean's own upcasters, so listeners upcast as Kafka Streams does: also those registered with
   * {@code Eventify.builder().registerHandler(...)} that are not beans.
   *
   * <p>Without an Eventify bean, the {@code @Upcaster} methods of the beans. With several, those too: which Eventify
   * bean's upcasters apply to a topic is not known here.
   */
  private static Upcasters upcasters(ObjectProvider<Eventify> apps, ListableBeanFactory beanFactory) {
    Eventify eventify = apps.getIfUnique();
    if (eventify != null) {
      return eventify.getUpcasters();
    }
    if (apps.stream().findAny().isPresent()) {
      log.warn("More than one Eventify bean: @KafkaListener methods upcast only with @Upcaster beans, "
          + "not with upcasters registered via registerHandler(...).");
    }
    return Upcasters.of(EventifyHandlerBeans.of(beanFactory));
  }

  private static <T> ConcurrentKafkaListenerContainerFactory<String, T> containerFactory(
      Map<String, Object> consumerConfig, ObjectProvider<CommonErrorHandler> errorHandlers, Deserializer<T> deserializer) {
    // A record that can't be read goes to the error handler, instead of being read again and again.
    ConsumerFactory<String, T> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig,
        StringDeserializer::new, () -> new ErrorHandlingDeserializer<>(deserializer));

    ConcurrentKafkaListenerContainerFactory<String, T> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory);
    factory.setRecordMessageConverter(new EventifyRecordMessageConverter());
    errorHandlers.ifUnique(factory::setCommonErrorHandler);
    return factory;
  }

  /**
   * The connection of the Eventify bean, which writes the events: its bootstrap servers, security settings and
   * {@code consumer.}-prefixed settings. Without one, the connection of Spring Kafka's consumer factory
   * ({@code spring.kafka.*}).
   */
  private static Map<String, Object> consumerConfig(ObjectProvider<Eventify> apps,
                                                    ObjectProvider<ConsumerFactory<?, ?>> consumerFactories) {
    Map<String, Object> config = new HashMap<>();
    Eventify eventify = apps.getIfUnique();
    if (eventify != null && eventify.getStreamsConfig() != null) {
      config.putAll(KafkaClientConfigs.consumerSettingsOf(eventify.getStreamsConfig()));
    } else {
      ConsumerFactory<?, ?> consumerFactory = consumerFactories.getIfUnique();
      if (consumerFactory != null) {
        config.putAll(consumerFactory.getConfigurationProperties());
      }
    }

    // Eventify writes a command's events in a transaction. With Kafka's default, read_uncommitted, a listener also
    // gets the events of commands that failed and were rolled back: events of things that never happened.
    String readCommitted = IsolationLevel.READ_COMMITTED.toString();
    Object isolationLevel = config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, readCommitted);
    if (isolationLevel != null && !readCommitted.equalsIgnoreCase(isolationLevel.toString())) {
      log.warn("'{}' is set by Eventify to '{}' for @KafkaListener methods; the configured value '{}' is not used.",
          ConsumerConfig.ISOLATION_LEVEL_CONFIG, readCommitted, isolationLevel);
    }
    // From the first event, as Eventify's own event handlers read them: a new projection is built from all events.
    config.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    config.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    return config;
  }

  /** The Eventify bean's, so events are read as they were written; one of Eventify's own without one. */
  private static ObjectMapper objectMapper(ObjectProvider<Eventify> apps) {
    Eventify eventify = apps.getIfUnique();
    return eventify != null ? eventify.getObjectMapper() : EventifyObjectMapper.create();
  }

  /** Where to connect to: an Eventify bean, or Spring Kafka's consumer factory. */
  static class KafkaIsConfigured extends AnyNestedCondition {

    KafkaIsConfigured() {
      super(ConfigurationCondition.ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnBean(Eventify.class)
    static class EventifyBean {
    }

    @ConditionalOnBean(ConsumerFactory.class)
    static class ConsumerFactoryBean {
    }
  }
}
