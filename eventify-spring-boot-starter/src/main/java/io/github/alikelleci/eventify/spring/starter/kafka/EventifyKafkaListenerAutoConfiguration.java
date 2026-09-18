package io.github.alikelleci.eventify.spring.starter.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.upcasting.Upcaster;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MultiValuedMap;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
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
import java.util.Properties;
import java.util.Set;

/**
 * Lets {@code @KafkaListener} methods read Eventify's event topics, next to or instead of {@code @HandleEvent}: each
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

  /** Static: a post-processor created from an instance method would first create this configuration, too early. */
  @Bean
  public static EventifyUpcasters eventifyUpcasters() {
    return new EventifyUpcasters();
  }

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
      EventifyUpcasters upcasters) {
    ObjectMapper objectMapper = objectMapper(apps);
    return containerFactory(consumerConfig(apps, consumerFactories), errorHandlers,
        new JsonDeserializer<>(Event.class, objectMapper, upcasters(apps, upcasters)));
  }

  /**
   * The Eventify bean's own upcasters, so listeners upcast as Kafka Streams does: also those registered with
   * {@code Eventify.builder().registerHandler(...)} that are not beans. Its map is complete before the listener
   * containers start: the upcaster beans are added to it once every singleton exists.
   *
   * <p>Without an Eventify bean, the {@code @Upcast} methods of the beans. With several, those too: which Eventify
   * bean's upcasters apply to a topic is not known here.
   */
  private static MultiValuedMap<String, Upcaster> upcasters(ObjectProvider<Eventify> apps, EventifyUpcasters beans) {
    Eventify eventify = apps.getIfUnique();
    if (eventify != null) {
      return eventify.getUpcasters();
    }
    if (apps.stream().findAny().isPresent()) {
      log.warn("There is more than one Eventify bean: @KafkaListener methods only upcast with the @Upcast methods of "
          + "beans, not with upcasters registered on an Eventify bean with Eventify.builder().registerHandler(...).");
    }
    return beans.getUpcasters();
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
      config.putAll(consumerSettingsOf(eventify.getStreamsConfig()));
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

  /** The settings of a Kafka Streams configuration that are also consumer settings; not its client id. */
  private static Map<String, Object> consumerSettingsOf(Properties streamsConfig) {
    Set<String> consumerSettings = ConsumerConfig.configNames();
    Map<String, Object> config = new HashMap<>();
    streamsConfig.forEach((key, value) -> {
      String name = key.toString();
      if (consumerSettings.contains(name) && !name.equals(ConsumerConfig.CLIENT_ID_CONFIG)) {
        config.put(name, value);
      }
    });
    // Consumer overrides of Kafka Streams ("consumer.max.poll.records") win, as they do in Kafka Streams.
    streamsConfig.forEach((key, value) -> {
      String name = key.toString();
      if (name.startsWith("consumer.") && consumerSettings.contains(name.substring("consumer.".length()))) {
        config.put(name.substring("consumer.".length()), value);
      }
    });
    return config;
  }

  /** The Eventify bean's, so events are read as they were written; one of Eventify's own without one. */
  private static ObjectMapper objectMapper(ObjectProvider<Eventify> apps) {
    Eventify eventify = apps.getIfUnique();
    return eventify != null ? eventify.getObjectMapper() : JacksonUtils.enhancedObjectMapper();
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
