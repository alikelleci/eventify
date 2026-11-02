package io.github.alikelleci.eventify.spring.starter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.event.Event;
import io.github.alikelleci.eventify.core.event.EventSerde;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Revision;
import io.github.alikelleci.eventify.core.serialization.EventifyObjectMapper;
import io.github.alikelleci.eventify.core.serialization.JsonSerializer;
import io.github.alikelleci.eventify.core.upcasting.annotation.Upcaster;
import lombok.Builder;
import lombok.Value;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** The EventSerde bean reads events with the upcasters of the application, for 0, 1 and several Eventify beans. */
@DisplayName("EventSerde bean")
class EventifyEventSerdeTest {

  private static final String TYPE = "io.github.alikelleci.eventify.spring.starter.EventifyEventSerdeTest$Renamed";

  /** Revision 1: {@code name}. Revision 2: {@code fullName}. */
  @Value
  @Builder
  @Revision(2)
  public static class Renamed {
    @AggregateId
    String id;
    String name;
    String fullName;
  }

  public static class NameToFullName {
    @Upcaster(type = TYPE, revision = 1)
    public JsonNode upcast(ObjectNode payload) {
      payload.set("fullName", payload.remove("name"));
      return payload;
    }
  }

  @Configuration
  static class UpcasterBeanWithoutEventify {
    @Bean
    NameToFullName nameToFullName() {
      return new NameToFullName();
    }
  }

  @Configuration
  static class UpcasterRegisteredOnlyOnEventify {
    @Bean
    Eventify eventify() {
      return Eventify.builder().streamsConfig(streamsConfig("only")).registerHandler(new NameToFullName()).build();
    }
  }

  @Configuration
  static class UpcasterBeanWithAPrimaryEventify {
    @Bean
    NameToFullName nameToFullName() {
      return new NameToFullName();
    }

    @Bean
    @Primary
    Eventify first() {
      return Eventify.builder().streamsConfig(streamsConfig("first")).build();
    }

    @Bean
    Eventify second(NameToFullName nameToFullName) {
      return Eventify.builder().streamsConfig(streamsConfig("second")).registerHandler(nameToFullName).build();
    }
  }

  @Configuration
  static class OwnEventSerde {
    @Bean
    EventSerde myEventSerde() {
      return new EventSerde();
    }
  }

  private final ApplicationContextRunner runner = new ApplicationContextRunner()
      .withConfiguration(AutoConfigurations.of(EventifyAutoConfiguration.class));

  @Test
  @DisplayName("Should upcast with the @Upcaster beans without an Eventify bean")
  void upcastsWithTheUpcasterBeansWithoutAnEventifyBean() {
    runner.withUserConfiguration(UpcasterBeanWithoutEventify.class).run(context ->
        assertThat(read(context.getBean(EventSerde.class)).getRevision()).isEqualTo(2));
  }

  @Test
  @DisplayName("Should upcast with the upcasters of the one Eventify bean, also those that are not beans")
  void upcastsWithTheUpcastersOfTheOneEventifyBean() {
    runner.withUserConfiguration(UpcasterRegisteredOnlyOnEventify.class).run(context ->
        assertThat(read(context.getBean(EventSerde.class)).getRevision()).isEqualTo(2));
  }

  @Test
  @DisplayName("Should upcast with the @Upcaster beans when one of several Eventify beans is @Primary")
  void upcastsWithTheUpcasterBeansWhenOneEventifyBeanIsPrimary() {
    runner.withUserConfiguration(UpcasterBeanWithAPrimaryEventify.class).run(context ->
        assertThat(read(context.getBean(EventSerde.class)).getRevision()).isEqualTo(2));
  }

  /** Otherwise every application would build it, and log the warning for several Eventify beans, without using it. */
  @Test
  @DisplayName("Should make the EventSerde bean only when it is used")
  void makesTheEventSerdeOnlyWhenUsed() {
    runner.withUserConfiguration(UpcasterBeanWithAPrimaryEventify.class).run(context -> {
      assertThat(context.getBeanFactory().containsSingleton("eventifyEventSerde")).isFalse();
      context.getBean(EventSerde.class);
      assertThat(context.getBeanFactory().containsSingleton("eventifyEventSerde")).isTrue();
    });
  }

  @Test
  @DisplayName("Should back off when the application has an EventSerde bean")
  void backsOffForAnEventSerdeOfTheApplication() {
    runner.withUserConfiguration(OwnEventSerde.class).run(context -> {
      assertThat(context).hasSingleBean(EventSerde.class);
      assertThat(context).hasBean("myEventSerde");
    });
  }

  private static Event read(EventSerde serde) {
    Event event = Event.builder().aggregateType("profile").payload(Renamed.builder().id("ada").name("Ada Lovelace").build()).sequence(1).build();
    String stored = new String(new JsonSerializer<Event>(EventifyObjectMapper.create()).serialize("events", event))
        .replace("\"revision\":2", "\"revision\":1");
    Event read = serde.deserializer().deserialize("events", stored.getBytes());
    assertThat(read.getPayload()).isEqualTo(Renamed.builder().id("ada").fullName("Ada Lovelace").build());
    return read;
  }

  private static Properties streamsConfig(String applicationId) {
    Properties properties = new Properties();
    properties.put("application.id", applicationId);
    properties.put("bootstrap.servers", "localhost:9092");
    return properties;
  }
}
