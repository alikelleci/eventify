package io.github.alikelleci.eventify.spring.starter.kafka;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.kafka.KafkaClientConfigs;
import lombok.extern.slf4j.Slf4j;
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
import java.util.function.Supplier;

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

  /** Used with {@code @KafkaListener(containerFactory = "eventifyListenerContainerFactory")}; reads with the EventSerde bean. */
  @Bean
  @ConditionalOnMissingBean(name = "eventifyListenerContainerFactory")
  @Conditional(KafkaIsConfigured.class)
  public ConcurrentKafkaListenerContainerFactory<String, Event> eventifyListenerContainerFactory(
      ObjectProvider<Eventify> apps,
      ObjectProvider<ConsumerFactory<?, ?>> consumerFactories,
      ObjectProvider<CommonErrorHandler> errorHandlers,
      ObjectProvider<EventSerde> eventSerde) {
    return containerFactory(consumerConfig(apps, consumerFactories), errorHandlers, () -> eventSerde.getObject().deserializer());
  }

  private static <T> ConcurrentKafkaListenerContainerFactory<String, T> containerFactory(
      Map<String, Object> consumerConfig, ObjectProvider<CommonErrorHandler> errorHandlers, Supplier<Deserializer<T>> deserializer) {
    // A record that can't be read goes to the error handler, instead of being read again and again.
    ConsumerFactory<String, T> consumerFactory = new DefaultKafkaConsumerFactory<>(consumerConfig,
        StringDeserializer::new, () -> new ErrorHandlingDeserializer<>(deserializer.get()));

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
      log.warn("Overriding Kafka setting for @KafkaListener: name={}, configured={}, required={}",
          ConsumerConfig.ISOLATION_LEVEL_CONFIG, isolationLevel, readCommitted);
    }
    // From the first event, as Eventify's own event handlers read them: a new projection is built from all events.
    config.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    config.remove(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG);
    config.remove(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG);
    return config;
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
