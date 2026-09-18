package io.github.alikelleci.eventify.spring.starter.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.common.annotations.AggregateId;
import io.github.alikelleci.eventify.core.common.annotations.MessageId;
import io.github.alikelleci.eventify.core.common.annotations.MetadataValue;
import io.github.alikelleci.eventify.core.common.annotations.Revision;
import io.github.alikelleci.eventify.core.common.annotations.Timestamp;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.upcasting.annotations.Upcast;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import io.github.alikelleci.eventify.core.support.serialization.json.JsonDeserializer;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.util.backoff.FixedBackOff;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** {@code @KafkaListener} methods reading the records Eventify writes, from a real broker. */
@Testcontainers
@DisplayName("@KafkaListener on Eventify topics (real broker)")
class EventifyKafkaListenerIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final ObjectMapper objectMapper = JacksonUtils.enhancedObjectMapper();

  /** What the listeners got, in order. */
  static final BlockingQueue<Object> received = new LinkedBlockingQueue<>();

  /** Revision 1: {@code amount}. Revision 2: {@code total}. */
  @Value
  @Builder
  @Revision(2)
  public static class OrderPlaced {
    @AggregateId
    String id;
    int total;
  }

  @Value
  @Builder
  public static class OrderShipped {
    @AggregateId
    String id;
  }

  public static class OrderPlacedUpcaster {
    @Upcast(type = "io.github.alikelleci.eventify.spring.starter.kafka.EventifyKafkaListenerIT$OrderPlaced", revision = 1)
    public JsonNode amountToTotal(ObjectNode payload) {
      payload.set("total", payload.remove("amount"));
      return payload;
    }
  }

  // ---------------------------------------------------------------------------------------------------------------

  public static class ParameterListener {
    @KafkaListener(topics = "parameters", groupId = "parameters", containerFactory = "eventifyListenerContainerFactory")
    public void on(OrderPlaced event, Metadata metadata, @Timestamp Instant timestamp, @MessageId String id,
                   @MetadataValue("user") String user) {
      received.add(List.of(event, metadata.getCorrelationId(), timestamp, id, user));
    }
  }

  @Test
  @DisplayName("Should give the payload, upcasted, and the parameters Eventify handlers can have")
  void payloadAndParameters() throws Exception {
    Event event = Event.builder().payload(OrderPlaced.builder().id("order-1").total(10).build())
        .metadata("user", "ali").build();
    send("parameters", atRevision1(event));

    run(ParameterListener.class, () -> {
      List<?> got = (List<?>) take();
      assertThat(got.get(0)).isEqualTo(OrderPlaced.builder().id("order-1").total(10).build());
      assertThat(got.get(1)).isEqualTo(event.getMetadata().getCorrelationId());
      assertThat(got.get(2)).isEqualTo(event.getTimestamp());
      assertThat(got.get(3)).isEqualTo(event.getId());
      assertThat(got.get(4)).isEqualTo("ali");
    }, OrderPlacedUpcaster.class);
  }

  // ---------------------------------------------------------------------------------------------------------------

  @KafkaListener(topics = "dispatch", groupId = "dispatch", containerFactory = "eventifyListenerContainerFactory")
  public static class DispatchingListener {
    @KafkaHandler
    public void on(OrderPlaced event, Metadata metadata) {
      received.add("placed " + event.getId());
    }

    @KafkaHandler
    public void on(OrderShipped event) {
      received.add("shipped " + event.getId());
    }
  }

  @Test
  @DisplayName("Should pick the @KafkaHandler method by the type of the payload")
  void dispatchByPayloadType() throws Exception {
    send("dispatch", Event.builder().payload(OrderPlaced.builder().id("order-1").total(10).build()).build());
    send("dispatch", Event.builder().payload(OrderShipped.builder().id("order-1").build()).build());

    run(DispatchingListener.class, () -> {
      assertThat(take()).isEqualTo("placed order-1");
      assertThat(take()).isEqualTo("shipped order-1");
    });
  }

  // ---------------------------------------------------------------------------------------------------------------

  @KafkaListener(topics = "dispatch-with-event", groupId = "dispatch-with-event",
      containerFactory = "eventifyListenerContainerFactory")
  public static class DispatchingListenerWithEvent {
    @KafkaHandler
    public void on(OrderPlaced payload, Event event) {
      received.add("placed " + payload.getId() + " " + event.getId());
    }

    @KafkaHandler(isDefault = true)
    public void other(Event event) {
      received.add("other " + event.getType());
    }
  }

  @Test
  @DisplayName("Should give the Event next to the payload, and to the default @KafkaHandler method")
  void dispatchWithTheWholeEvent() throws Exception {
    Event placed = Event.builder().payload(OrderPlaced.builder().id("order-1").total(10).build()).build();
    send("dispatch-with-event", placed);
    send("dispatch-with-event", Event.builder().payload(OrderShipped.builder().id("order-1").build()).build());

    run(DispatchingListenerWithEvent.class, () -> {
      assertThat(take()).isEqualTo("placed order-1 " + placed.getId());
      assertThat(take()).isEqualTo("other OrderShipped");
    });
  }

  // ---------------------------------------------------------------------------------------------------------------

  public static class EventListener {
    @KafkaListener(topics = "whole-event", groupId = "whole-event", containerFactory = "eventifyListenerContainerFactory")
    public void on(Event event) {
      received.add(event);
    }
  }

  @Test
  @DisplayName("Should give the whole event to a listener that asks for it")
  void wholeEvent() throws Exception {
    Event event = Event.builder().payload(OrderShipped.builder().id("order-1").build()).build();
    send("whole-event", event);

    run(EventListener.class, () -> {
      Event got = (Event) take();
      assertThat(got.getId()).isEqualTo(event.getId());
      assertThat(got.getPayload()).isEqualTo(event.getPayload());
    });
  }

  // ---------------------------------------------------------------------------------------------------------------

  public static class CommittedListener {
    @KafkaListener(topics = "committed", groupId = "committed", containerFactory = "eventifyListenerContainerFactory")
    public void on(OrderShipped event) {
      received.add(event.getId());
    }
  }

  @Test
  @DisplayName("Should skip the events of a transaction that was rolled back, as Eventify does for a failed command")
  void onlyCommittedEvents() throws Exception {
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(Map.of(
        ProducerConfig.TRANSACTIONAL_ID_CONFIG, "committed-test")))) {
      producer.initTransactions();
      producer.beginTransaction();
      producer.send(record("committed", Event.builder().payload(OrderShipped.builder().id("rolled-back").build()).build()));
      producer.flush();
      producer.abortTransaction();
      producer.beginTransaction();
      producer.send(record("committed", Event.builder().payload(OrderShipped.builder().id("committed").build()).build()));
      producer.commitTransaction();
    }

    run(CommittedListener.class, () -> {
      assertThat(take()).isEqualTo("committed");
      assertThat(received.poll(2, TimeUnit.SECONDS)).isNull();
    });
  }

  // ---------------------------------------------------------------------------------------------------------------

  public static class FailingListener {
    @KafkaListener(topics = "failing", groupId = "failing", containerFactory = "eventifyListenerContainerFactory")
    public void on(OrderShipped event) {
      throw new IllegalStateException("the read model is down");
    }
  }

  /**
   * As in the docs: events are written with Eventify's serializer, so the dead-letter topic can be read as the event
   * topic. A record that could not be read has no event: its bytes are written as they were.
   */
  @Configuration
  static class DeadLetterTopic {
    @Bean
    DefaultErrorHandler errorHandler() {
      Map<Class<?>, Serializer<?>> serializers = new LinkedHashMap<>();
      serializers.put(byte[].class, new ByteArraySerializer());
      serializers.put(Event.class, new JsonSerializer<>(objectMapper));
      KafkaTemplate<String, Object> template = new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
          Map.of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
          new StringSerializer(), new DelegatingByTypeSerializer(serializers)));
      return new DefaultErrorHandler(new DeadLetterPublishingRecoverer(template), new FixedBackOff(0, 0));
    }
  }

  @Test
  @DisplayName("Should send a failed event, and a record that could not be read, to the dead-letter topic as they were")
  void deadLetterTopic() throws Exception {
    Event event = Event.builder().payload(OrderShipped.builder().id("order-1").build()).build();
    send("failing", event);
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(Map.of()))) {
      producer.send(new ProducerRecord<>("failing", "order-1", "not an event")).get();
    }

    received.clear();
    ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(EventifyKafkaListenerAutoConfiguration.class))
        .withUserConfiguration(ListenersOnly.class, DeadLetterTopic.class)
        .withBean(FailingListener.class);
    runner.run(context -> {
      assertThat(context).hasNotFailed();
      List<byte[]> deadLetters = read("failing-dlt", 2);

      Event deadLetter = new JsonDeserializer<>(Event.class, objectMapper).deserialize("failing-dlt", deadLetters.get(0));
      assertThat(deadLetter.getId()).isEqualTo(event.getId());
      assertThat(deadLetter.getPayload()).isEqualTo(event.getPayload());
      assertThat(new String(deadLetters.get(1), StandardCharsets.UTF_8)).isEqualTo("not an event");
    });
  }

  private static List<byte[]> read(String topic, int count) {
    Properties properties = new Properties();
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, "reader-" + topic);
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(properties, new StringDeserializer(), new ByteArrayDeserializer())) {
      consumer.subscribe(List.of(topic));
      List<byte[]> values = new ArrayList<>();
      long deadline = System.currentTimeMillis() + 30_000;
      while (values.size() < count && System.currentTimeMillis() < deadline) {
        consumer.poll(Duration.ofMillis(500)).forEach(record -> values.add(record.value()));
      }
      assertThat(values).as("records on " + topic).hasSize(count);
      return values;
    }
  }

  // ---------------------------------------------------------------------------------------------------------------

  @Configuration
  static class WithEventifyBean {
    @Bean
    Eventify eventify() {
      Properties properties = new Properties();
      properties.put("application.id", "listener-test");
      properties.put("bootstrap.servers", kafka.getBootstrapServers());
      return Eventify.builder().streamsConfig(properties).build();
    }
  }

  @Test
  @DisplayName("Should connect as the Eventify bean does, without Spring Kafka's consumer factory")
  void connectionOfTheEventifyBean() throws Exception {
    received.clear();
    send("whole-event-2", Event.builder().payload(OrderShipped.builder().id("order-2").build()).build());

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(EventifyKafkaListenerAutoConfiguration.class))
        .withUserConfiguration(EnableKafkaOnly.class, WithEventifyBean.class)
        .withBean(EventListenerOnSecondTopic.class)
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(((Event) take()).getPayload()).isEqualTo(OrderShipped.builder().id("order-2").build());
        });
  }

  public static class EventListenerOnSecondTopic {
    @KafkaListener(topics = "whole-event-2", groupId = "whole-event-2", containerFactory = "eventifyListenerContainerFactory")
    public void on(Event event) {
      received.add(event);
    }
  }

  // ---------------------------------------------------------------------------------------------------------------

  /** The upcaster is registered on the Eventify bean only: it is not a bean. */
  @Configuration
  static class WithUpcasterOnEventifyBean {
    @Bean
    Eventify eventify() {
      Properties properties = new Properties();
      properties.put("application.id", "listener-test");
      properties.put("bootstrap.servers", kafka.getBootstrapServers());
      return Eventify.builder().streamsConfig(properties).registerHandler(new OrderPlacedUpcaster()).build();
    }
  }

  public static class UpcastedListener {
    @KafkaListener(topics = "upcasted", groupId = "upcasted", containerFactory = "eventifyListenerContainerFactory")
    public void on(OrderPlaced event) {
      received.add(event);
    }
  }

  @Test
  @DisplayName("Should upcast with the upcasters registered on the Eventify bean, also when they are not beans")
  void upcastersOfTheEventifyBean() throws Exception {
    received.clear();
    send("upcasted", atRevision1(Event.builder().payload(OrderPlaced.builder().id("order-3").total(30).build()).build()));

    new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(EventifyKafkaListenerAutoConfiguration.class))
        .withUserConfiguration(EnableKafkaOnly.class, WithUpcasterOnEventifyBean.class)
        .withBean(UpcastedListener.class)
        .run(context -> {
          assertThat(context).hasNotFailed();
          assertThat(take()).isEqualTo(OrderPlaced.builder().id("order-3").total(30).build());
        });
  }

  // ---------------------------------------------------------------------------------------------------------------

  @EnableKafka
  @Configuration
  static class EnableKafkaOnly {
  }

  /** An application with only listeners: Spring Kafka's consumer factory, no Eventify bean. */
  @EnableKafka
  @Configuration
  static class ListenersOnly {
    @Bean
    ConsumerFactory<Object, Object> consumerFactory() {
      return new DefaultKafkaConsumerFactory<>(Map.of(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
          ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
          ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class));
    }
  }

  interface Assertions {
    void run() throws Exception;
  }

  private static void run(Class<?> listener, Assertions assertions, Class<?>... beans) {
    received.clear();
    ApplicationContextRunner runner = new ApplicationContextRunner()
        .withConfiguration(AutoConfigurations.of(EventifyKafkaListenerAutoConfiguration.class))
        .withUserConfiguration(ListenersOnly.class)
        .withBean(listener);
    for (Class<?> bean : beans) {
      runner = runner.withBean(bean);
    }
    runner.run(context -> {
      assertThat(context).hasNotFailed();
      assertions.run();
    });
  }

  private static Object take() throws InterruptedException {
    Object got = received.poll(30, TimeUnit.SECONDS);
    assertThat(got).as("a record handled by the listener").isNotNull();
    return got;
  }

  /** As an older version of the application wrote it: revision 1, with {@code amount}. */
  private static JsonNode atRevision1(Event event) {
    ObjectNode json = objectMapper.valueToTree(event);
    ObjectNode payload = (ObjectNode) json.get("payload");
    payload.set("amount", payload.remove("total"));
    json.put("revision", 1);
    return json;
  }

  private static void send(String topic, Object value) throws Exception {
    try (KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig(Map.of()))) {
      producer.send(record(topic, value)).get();
    }
  }

  private static ProducerRecord<String, String> record(String topic, Object value) throws Exception {
    return new ProducerRecord<>(topic, "order-1", objectMapper.writeValueAsString(value));
  }

  private static Properties producerConfig(Map<String, Object> extra) {
    Properties properties = new Properties();
    properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    properties.putAll(extra);
    return properties;
  }
}
