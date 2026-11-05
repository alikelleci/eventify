package io.github.alikelleci.eventify.core.event;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.aggregate.annotation.AggregateRoot;
import io.github.alikelleci.eventify.core.aggregate.annotation.EventSourcingHandler;
import io.github.alikelleci.eventify.core.command.Command;
import io.github.alikelleci.eventify.core.command.Scheduled;
import io.github.alikelleci.eventify.core.command.CommandSerde;
import io.github.alikelleci.eventify.core.command.annotation.CommandHandler;
import io.github.alikelleci.eventify.core.command.gateway.CommandGateway;
import io.github.alikelleci.eventify.core.command.internal.HeaderNames;
import io.github.alikelleci.eventify.core.event.annotation.EventHandler;
import io.github.alikelleci.eventify.core.handler.annotation.Timestamp;
import io.github.alikelleci.eventify.core.message.Metadata;
import io.github.alikelleci.eventify.core.message.MetadataKeys;
import io.github.alikelleci.eventify.core.message.annotation.AggregateId;
import io.github.alikelleci.eventify.core.message.annotation.Topic;
import lombok.Builder;
import lombok.Value;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Commands from event handlers against a real broker: an event handler returns commands, sent in the transaction that
 * reads the event.
 * Every topic has three partitions, so a command is routed by its own aggregate id, not by the event's.
 * Tests share topics; each uses its own aggregate ids and application ids, and checks only its own records.
 */
@Testcontainers
@DisplayName("Commands from event handlers (exactly-once, real broker)")
class EventHandlerCommandsIT {

  @Container
  static final KafkaContainer kafka = new KafkaContainer("apache/kafka-native:3.9.1");

  private static final int PARTITIONS = 3;
  private static final String REPLY_TOPIC = "handler.replies";

  @Topic("handler.orders")
  public record PlaceOrder(@AggregateId String id, String paymentId, int amount) {
  }

  @Topic("handler.order-events")
  public record OrderPlaced(@AggregateId String id, String paymentId, int amount) {
  }

  @AggregateRoot("order")
  public record Order(@AggregateId String id) {
  }

  @Topic("handler.payments")
  public record RequestPayment(@AggregateId String id, String orderId, int amount) {
  }

  @Topic("handler.payment-events")
  public record PaymentRequested(@AggregateId String id, String orderId, int amount) {
  }

  @AggregateRoot("payment")
  public record Payment(@AggregateId String id) {
  }

  public static class OrderHandler {
    @CommandHandler
    public OrderPlaced handle(PlaceOrder command, Order state) {
      return new OrderPlaced(command.id(), command.paymentId(), command.amount());
    }

    @EventSourcingHandler
    public Order handle(OrderPlaced event, Order state) {
      return new Order(event.id());
    }
  }

  public static class PaymentHandler {
    @CommandHandler
    public PaymentRequested handle(RequestPayment command, Payment state) {
      if (command.amount() <= 0) {
        throw new IllegalStateException("amount must be positive");
      }
      return new PaymentRequested(command.id(), command.orderId(), command.amount());
    }

    @EventSourcingHandler
    public Payment handle(PaymentRequested event, Payment state) {
      return new Payment(event.id());
    }
  }

  public static class PaymentEventHandler {
    @EventHandler
    public RequestPayment on(OrderPlaced event) {
      return new RequestPayment(event.paymentId(), event.id(), event.amount());
    }
  }

  /** A command now and, a few seconds after the event, a command with its own metadata. */
  public static class MixingEventHandler {
    static final Duration LATER = Duration.ofSeconds(4);

    @EventHandler
    public List<Object> on(OrderPlaced event, @Timestamp Instant placedAt) {
      return List.of(
          new RequestPayment(event.paymentId(), event.id(), event.amount()),
          Scheduled.of(Command.builder()
              .payload(new RequestPayment("later-" + event.paymentId(), event.id(), event.amount()))
              .metadata(Metadata.of("reason", "later"))
              .build(), placedAt.plus(LATER)));
    }
  }

  // Failure scenarios: each its own event topic, so no other application reads the event that makes it fail.

  @Topic("handler.risky")
  public record PlaceThrowingOrder(@AggregateId String id) {
  }

  @Topic("handler.risky")
  public record PlaceFlakyOrder(@AggregateId String id) {
  }

  @Topic("handler.throwing-events")
  public record ThrowingOrderPlaced(@AggregateId String id) {
  }

  @Topic("handler.flaky-events")
  public record FlakyOrderPlaced(@AggregateId String id) {
  }

  @AggregateRoot("risky")
  public record RiskyOrder(@AggregateId String id) {
  }

  public static class RiskyHandler {
    @CommandHandler
    public ThrowingOrderPlaced handle(PlaceThrowingOrder command, RiskyOrder state) {
      return new ThrowingOrderPlaced(command.id());
    }

    @CommandHandler
    public FlakyOrderPlaced handle(PlaceFlakyOrder command, RiskyOrder state) {
      return new FlakyOrderPlaced(command.id());
    }

    @EventSourcingHandler
    public RiskyOrder handle(ThrowingOrderPlaced event, RiskyOrder state) {
      return new RiskyOrder(event.id());
    }

    @EventSourcingHandler
    public RiskyOrder handle(FlakyOrderPlaced event, RiskyOrder state) {
      return new RiskyOrder(event.id());
    }
  }

  /** Returns two commands, and then fails for an id starting with "poison". */
  public static class ThrowingEventHandler {
    @EventHandler
    public List<Object> on(ThrowingOrderPlaced event) {
      if (event.id().startsWith("poison")) {
        throw new IllegalStateException("event handler failed");
      }
      return List.of(new RequestPayment(event.id() + "-pay", event.id(), 1), new RequestPayment(event.id() + "-pay-2", event.id(), 1));
    }
  }

  /** Fails on the {@link #FAIL_ON_WRITE}th JSON write; set to the last one, it fails when sent, after the command before it. */
  @Topic("handler.flaky")
  @Value
  @Builder
  public static class FlakyCommand {
    static final AtomicInteger TIMES_WRITTEN = new AtomicInteger();
    static volatile int FAIL_ON_WRITE = Integer.MAX_VALUE;
    static volatile String COUNTED_ID;

    @AggregateId
    String id;

    public String getContent() {
      if (id.equals(COUNTED_ID) && TIMES_WRITTEN.incrementAndGet() == FAIL_ON_WRITE) {
        throw new IllegalStateException("cannot be written this time");
      }
      return "content";
    }
  }

  /** A valid command first, then the flaky one. */
  public static class FlakyEventHandler {
    @EventHandler
    public List<Object> on(FlakyOrderPlaced event) {
      return List.of(new RequestPayment(event.id() + "-pay", event.id(), 1), FlakyCommand.builder().id(event.id()).build());
    }
  }

  @TempDir
  Path stateDir;

  private final List<Eventify> started = new ArrayList<>();
  private CommandGateway gateway;

  @BeforeAll
  static void createTopics() throws Exception {
    try (AdminClient admin = AdminClient.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(
          "handler.orders", "handler.orders.results", "handler.order-events",
          "handler.payments", "handler.payments.results", "handler.payment-events",
          "handler.risky", "handler.risky.results", "handler.throwing-events", "handler.flaky-events", "handler.flaky",
          "handler.orders.scheduled", "handler.payments.scheduled", "handler.risky.scheduled", "handler.flaky.scheduled",
          REPLY_TOPIC).stream().map(name -> new NewTopic(name, PARTITIONS, (short) 1)).toList()).all().get();
    }
  }

  @AfterEach
  void tearDown() {
    started.forEach(Eventify::stop);
    if (gateway != null) {
      gateway.close();
    }
    FlakyCommand.COUNTED_ID = null;
    FlakyCommand.FAIL_ON_WRITE = Integer.MAX_VALUE;
  }

  @Test
  @DisplayName("Should send each command from an event handler once, routed by its own id, with the flow's correlation and no reply topic")
  void anEventHandlerLeadsFromOneAggregateToAnother() throws Exception {
    start("handler-flow", new OrderHandler(), new PaymentHandler(), new PaymentEventHandler());
    gateway = gateway();
    int orders = 20;

    List<Command> sent = new ArrayList<>();
    List<CompletableFuture<List<Event>>> replies = new ArrayList<>();
    for (int i = 0; i < orders; i++) {
      Command command = Command.builder()
          .payload(new PlaceOrder("flow-order-" + i, "flow-payment-" + i, 10 + i))
          .metadata(Metadata.of("tenant", "acme"))
          .build();
      sent.add(command);
      replies.add(gateway.send(command));
    }

    // The sender gets the result of its own command: the order's event, nothing of the payment.
    for (int i = 0; i < orders; i++) {
      List<Event> events = replies.get(i).get(60, java.util.concurrent.TimeUnit.SECONDS);
      assertThat(events).singleElement().satisfies(event -> assertThat(event.getPayload()).isInstanceOf(OrderPlaced.class));
    }
    await("every payment to be requested", () -> committedEvents("handler.payment-events", "flow-").size() == orders);
    settle();

    List<ConsumerRecord<String, Command>> commands = committedCommands("handler.payments", "flow-");
    assertThat(commands).as("one command per order, no duplicates").hasSize(orders);
    List<ConsumerRecord<String, Event>> orderEvents = committedEvents("handler.order-events", "flow-");
    for (ConsumerRecord<String, Command> record : commands) {
      Command command = record.value();
      RequestPayment payment = (RequestPayment) command.getPayload();
      Command original = sent.stream().filter(c -> ((PlaceOrder) c.getPayload()).id().equals(payment.orderId())).findFirst().orElseThrow();
      Event cause = orderEvents.stream().filter(e -> e.key().equals(payment.orderId())).findFirst().orElseThrow().value();

      assertThat(record.key()).isEqualTo(payment.id());
      assertThat(record.partition()).as("routed by the payment id").isEqualTo(partitionOf(payment.id()));
      assertThat(record.headers().lastHeader(HeaderNames.REPLY_TO)).as("no reply topic").isNull();
      assertThat(payment.amount()).isEqualTo(((PlaceOrder) original.getPayload()).amount());
      assertThat(command.getMetadata())
          .containsEntry(MetadataKeys.CORRELATION_ID, original.getMetadata().getCorrelationId())
          .containsEntry(MetadataKeys.CAUSATION_ID, cause.getId())
          .containsEntry("tenant", "acme");
    }

    // The payment aggregate handled every command; its events carry the same flow.
    for (ConsumerRecord<String, Event> record : committedEvents("handler.payment-events", "flow-")) {
      Command command = commands.stream().filter(c -> c.key().equals(record.key())).findFirst().orElseThrow().value();
      assertThat(record.partition()).isEqualTo(partitionOf(record.key()));
      assertThat(record.value().getMetadata().getCorrelationId()).isEqualTo(command.getMetadata().getCorrelationId());
      assertThat(record.value().getMetadata().getCausationId()).isEqualTo(command.getId());
    }
    assertThat(committedStrings("handler.payments.results", "flow-")).hasSize(orders).allMatch(EventHandlerCommandsIT::isSuccess);

    // Only the gateway's own commands were answered.
    List<ConsumerRecord<String, String>> answered = committedStrings(REPLY_TOPIC, "flow-");
    assertThat(answered).hasSize(orders).allMatch(record -> record.key().startsWith("flow-order-"));
  }

  /** The scheduled one goes through the scheduled topic and the scheduler, under exactly-once, like the gateway's. */
  @Test
  @DisplayName("Should send a command now and deliver a scheduled one once at its time, both in the event's flow")
  void anEventHandlerSendsOneNowAndSchedulesAnother() throws Exception {
    start("handler-mixed", new OrderHandler(), new PaymentHandler(), new MixingEventHandler());
    gateway = gateway();
    Command placeOrder = Command.builder()
        .payload(new PlaceOrder("mixed-order-1", "mixed-payment-1", 5))
        .metadata(Metadata.of("tenant", "acme"))
        .build();

    gateway.send(placeOrder).get(60, java.util.concurrent.TimeUnit.SECONDS);
    await("the command sent now", () -> !committedCommands("handler.payments", "mixed-payment-1").isEmpty());
    Event orderPlaced = committedEvents("handler.order-events", "mixed-order-1").get(0).value();
    assertThat(committedCommands("handler.payments", "later-mixed-payment-1")).as("delivered right away").isEmpty();

    await("the scheduled command", () -> !committedCommands("handler.payments", "later-mixed-payment-1").isEmpty());
    settle();

    assertThat(committedCommands("handler.payments", "mixed-payment-1")).as("the command sent now").hasSize(1);
    List<ConsumerRecord<String, Command>> later = committedCommands("handler.payments", "later-mixed-payment-1");
    assertThat(later).as("the scheduled command, once").hasSize(1);
    assertThat(later.get(0).timestamp()).as("not before its time")
        .isGreaterThanOrEqualTo(orderPlaced.getTimestamp().plus(MixingEventHandler.LATER).toEpochMilli());
    assertThat(later.get(0).value().getMetadata())
        .containsEntry("reason", "later")
        .containsEntry("tenant", "acme")
        .containsEntry(MetadataKeys.CORRELATION_ID, placeOrder.getMetadata().getCorrelationId())
        .containsEntry(MetadataKeys.CAUSATION_ID, orderPlaced.getId());
    assertThat(committedStrings("handler.payments.results", "later-mixed-payment-1")).singleElement().matches(EventHandlerCommandsIT::isSuccess);
  }

  @Test
  @DisplayName("Should record a rejected command from an event handler as failed, answer no one, and keep running")
  void aRejectedCommandFromAnEventHandlerIsOnlyAFailedResult() throws Exception {
    Eventify eventify = start("handler-rejected", new OrderHandler(), new PaymentHandler(), new PaymentEventHandler());
    gateway = gateway();

    assertThat(gateway.send(new PlaceOrder("rejected-order-1", "rejected-payment-1", 0)).get(60, java.util.concurrent.TimeUnit.SECONDS))
        .as("the order itself is accepted").hasSize(1);
    gateway.send(new PlaceOrder("rejected-order-2", "rejected-payment-2", 5)).get(60, java.util.concurrent.TimeUnit.SECONDS);
    await("the next payment", () -> !committedEvents("handler.payment-events", "rejected-payment-2").isEmpty());
    settle();

    assertThat(eventify.getKafkaStreams().state()).isEqualTo(KafkaStreams.State.RUNNING);
    assertThat(committedStrings("handler.payments.results", "rejected-payment-1")).singleElement()
        .satisfies(record -> assertThat(record.value()).contains("\"result\":\"failure\"").contains("amount must be positive"));
    assertThat(committedEvents("handler.payment-events", "rejected-payment-1")).isEmpty();
    assertThat(committedStrings(REPLY_TOPIC, "rejected-")).hasSize(2).allMatch(record -> record.key().startsWith("rejected-order-"));
  }

  @Test
  @DisplayName("Should send the commands once over a restart, and again when the events are read under a new application id")
  void anEventHandlerInItsOwnApplicationSendsOnceAndAgainOnReplay() throws Exception {
    start("handler-aggregates", new OrderHandler(), new PaymentHandler());
    Eventify handlers = start("handler-only", new PaymentEventHandler());
    gateway = gateway();
    int orders = 5;
    for (int i = 0; i < orders; i++) {
      gateway.send(new PlaceOrder("own-order-" + i, "own-payment-" + i, 1)).get(60, java.util.concurrent.TimeUnit.SECONDS);
    }
    await("every payment command", () -> committedCommands("handler.payments", "own-").size() == orders);
    settle();

    // The same application again: it goes on from its committed offsets.
    handlers.stop();
    started.remove(handlers);
    start("handler-only", new PaymentEventHandler());
    gateway.send(new PlaceOrder("own-order-after-restart", "own-payment-after-restart", 1)).get(60, java.util.concurrent.TimeUnit.SECONDS);
    await("the command after the restart", () -> !committedCommands("handler.payments", "own-payment-after-restart").isEmpty());
    settle();
    assertThat(committedCommands("handler.payments", "own-")).as("nothing sent twice over a restart").hasSize(orders + 1);

    // A new application id reads all events from the start, so it sends every command again.
    start("handler-only-replayed", new PaymentEventHandler());
    await("the replayed commands", () -> committedCommands("handler.payments", "own-").size() == 2 * (orders + 1));
    settle();
    assertThat(committedCommands("handler.payments", "own-")).hasSize(2 * (orders + 1));
    await("the aggregate to handle them", () -> committedStrings("handler.payments.results", "own-").size() == 2 * (orders + 1));
  }

  @Test
  @DisplayName("Should commit none of an event's commands when the event handler fails, and stop")
  void aFailingEventHandlerCommitsNothingOfTheEvent() {
    Eventify eventify = start("handler-throwing", new RiskyHandler(), new ThrowingEventHandler());
    send(new PlaceThrowingOrder("throwing-ok"));
    await("the commands of the first event", () -> committedCommands("handler.payments", "throwing-ok").size() == 2);

    send(new PlaceThrowingOrder("poison-1"));
    await("Eventify to stop", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.ERROR);

    assertThat(committedEvents("handler.throwing-events", "poison-1")).as("the aggregate's event stays").hasSize(1);
    assertThat(committedCommands("handler.payments", "poison-1")).as("commands of the failed event").isEmpty();
    assertThat(committedCommands("handler.payments", "throwing-ok")).as("commands of the event before it").hasSize(2);
  }

  @Test
  @DisplayName("Should abort a command already sent when the next command of the same event fails to be sent")
  void aFailureAfterTheFirstCommandWasSentAbortsIt() {
    Eventify eventify = start("handler-flaky", new RiskyHandler(), new FlakyEventHandler());

    // First one that succeeds, to count how often the flaky command is written: the last time is when it is sent.
    FlakyCommand.COUNTED_ID = "flaky-count";
    FlakyCommand.TIMES_WRITTEN.set(0);
    send(new PlaceFlakyOrder("flaky-count"));
    await("the counted commands", () -> committedStrings("handler.flaky", "flaky-count").size() == 1);
    int writesPerCommand = FlakyCommand.TIMES_WRITTEN.get();
    assertThat(committedCommands("handler.payments", "flaky-count-pay")).hasSize(1);

    // The same again, now failing on that last write: after the payment command before it was forwarded.
    FlakyCommand.COUNTED_ID = "flaky-fail";
    FlakyCommand.TIMES_WRITTEN.set(0);
    FlakyCommand.FAIL_ON_WRITE = writesPerCommand;
    send(new PlaceFlakyOrder("flaky-fail"));
    await("Eventify to stop", () -> eventify.getKafkaStreams().state() == KafkaStreams.State.ERROR);

    assertThat(FlakyCommand.TIMES_WRITTEN.get()).as("failed on the send itself").isGreaterThanOrEqualTo(writesPerCommand);
    assertThat(committedEvents("handler.flaky-events", "flaky-fail")).hasSize(1);
    assertThat(committedCommands("handler.payments", "flaky-fail-pay")).as("the command sent before the failure").isEmpty();
    assertThat(committedStrings("handler.flaky", "flaky-fail")).isEmpty();
  }

  private Eventify start(String applicationId, Object... handlers) {
    Properties properties = new Properties();
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    properties.put(StreamsConfig.STATE_DIR_CONFIG, stateDir.resolve(applicationId).toString());
    properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
    // A stopped instance doesn't leave the group: the broker waits this long before reassigning its partitions.
    properties.put(StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), 6000);
    Eventify.EventifyBuilder builder = Eventify.builder().streamsConfig(properties);
    for (Object handler : handlers) {
      builder.registerHandler(handler);
    }
    Eventify instance = builder.build();
    instance.start();
    started.add(instance);
    await("Eventify to run", () -> instance.getKafkaStreams().state() == KafkaStreams.State.RUNNING);
    return instance;
  }

  private static CommandGateway gateway() {
    Properties producerConfig = new Properties();
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    return CommandGateway.builder().producerConfig(producerConfig).replyTopic(REPLY_TOPIC).build();
  }

  /** Without a reply topic, as another application would. */
  private static void send(Object payload) {
    Command command = Command.builder().payload(payload).build();
    try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<>(
        Map.<String, Object>of(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()),
        new org.apache.kafka.common.serialization.StringSerializer(), new CommandSerde().serializer())) {
      producer.send(new org.apache.kafka.clients.producer.ProducerRecord<>(command.getTopic().value(), command.getAggregateId(), command));
    }
  }

  private static int partitionOf(String key) {
    return Utils.toPositive(Utils.murmur2(key.getBytes(StandardCharsets.UTF_8))) % PARTITIONS;
  }

  private static boolean isSuccess(ConsumerRecord<String, String> record) {
    return record.value().contains("\"result\":\"success\"");
  }

  private static List<ConsumerRecord<String, Command>> committedCommands(String topic, String keyPrefix) {
    return committed(topic, keyPrefix, new CommandSerde().deserializer());
  }

  private static List<ConsumerRecord<String, Event>> committedEvents(String topic, String keyPrefix) {
    return committed(topic, keyPrefix, new EventSerde().deserializer());
  }

  private static List<ConsumerRecord<String, String>> committedStrings(String topic, String keyPrefix) {
    return committed(topic, keyPrefix, new StringDeserializer());
  }

  /** What a {@code read_committed} consumer sees on all partitions, for keys starting with {@code keyPrefix}. */
  private static <V> List<ConsumerRecord<String, V>> committed(String topic, String keyPrefix, Deserializer<V> deserializer) {
    List<ConsumerRecord<String, V>> found = new ArrayList<>();
    try (KafkaConsumer<String, V> consumer = new KafkaConsumer<>(Map.of(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
        ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed",
        ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false), new StringDeserializer(), deserializer)) {
      List<TopicPartition> partitions = consumer.partitionsFor(topic).stream()
          .map((PartitionInfo info) -> new TopicPartition(topic, info.partition())).toList();
      consumer.assign(partitions);
      consumer.seekToBeginning(partitions);
      Map<TopicPartition, Long> end = consumer.endOffsets(partitions);
      while (partitions.stream().anyMatch(partition -> consumer.position(partition) < end.get(partition))) {
        for (ConsumerRecord<String, V> record : consumer.poll(Duration.ofMillis(500))) {
          TopicPartition partition = new TopicPartition(record.topic(), record.partition());
          if (record.offset() < end.get(partition) && record.key() != null && record.key().startsWith(keyPrefix)) {
            found.add(record);
          }
        }
      }
    }
    return found;
  }

  /** Gives a duplicate the time to show up: a few commit intervals. */
  private static void settle() throws InterruptedException {
    Thread.sleep(Duration.ofSeconds(3).toMillis());
  }

  private static void await(String what, BooleanSupplier condition) {
    Instant deadline = Instant.now().plusSeconds(90);
    while (!condition.getAsBoolean()) {
      if (Instant.now().isAfter(deadline)) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new AssertionError(e);
      }
    }
  }
}
