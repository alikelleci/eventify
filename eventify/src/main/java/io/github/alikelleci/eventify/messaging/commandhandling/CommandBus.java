package io.github.alikelleci.eventify.messaging.commandhandling;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.constants.Config;
import io.github.alikelleci.eventify.constants.Handlers;
import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import io.github.alikelleci.eventify.support.serializer.JsonDeserializer;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import io.github.alikelleci.eventify.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.state.Stores;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class CommandBus {

  private Properties producerConfig;
  private Producer<String, Command> producer;

  private Properties consumerConfig;
  private Consumer<String, Command> consumer;

  private Properties streamsConfig;
  private KafkaStreams kafkaStreams;

  //private Map<String, CompletableFuture<Object>> futures = new ConcurrentHashMap<>();
  private Cache<String, CompletableFuture<Object>> cache;


  public CommandBus(Properties producerConfig, Properties consumerConfig, Properties streamsConfig) {
    this.producerConfig = producerConfig;
    setAdditionalProducerConfigs(this.producerConfig);
    this.producer = new KafkaProducer<>(this.producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());

    this.consumerConfig = consumerConfig;
    this.consumer = new KafkaConsumer<>(this.consumerConfig,
        new StringDeserializer(),
        new JsonDeserializer<>());

    this.streamsConfig = streamsConfig;
    this.kafkaStreams = new KafkaStreams(topology(), this.streamsConfig);

    this.cache = Caffeine.newBuilder()
        .expireAfterWrite(Duration.ofMinutes(5))
        .build();
  }

  protected Topology topology() {
    StreamsBuilder builder = new StreamsBuilder();

    /*
     *************************************************************************************
     * Command Handling
     *************************************************************************************
     */

    if (!Handlers.COMMAND_HANDLERS.isEmpty()) {

      // Event store
      builder.addStateStore(Stores
          .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("event-store"), Serdes.String(), CustomSerdes.Json(Event.class))
          .withLoggingEnabled(Collections.emptyMap()));

      // Snapshot Store
      builder.addStateStore(Stores
          .timestampedKeyValueStoreBuilder(Stores.persistentTimestampedKeyValueStore("snapshot-store"), Serdes.String(), CustomSerdes.Json(Aggregate.class))
          .withLoggingEnabled(Collections.emptyMap()));

      // --> Commands
      KStream<String, Command> commands = builder.stream(Topics.COMMANDS, Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
          .filter((key, command) -> key != null)
          .filter((key, command) -> command != null);

      // Commands --> Results
      KStream<String, CommandResult> commandResults = commands
          .transformValues(CommandTransformer::new, "event-store", "snapshot-store")
          .filter((key, result) -> result != null);

      // Results --> Push
      commandResults
          .mapValues(CommandResult::getCommand)
          .to((key, command, recordContext) -> CommonUtils.getTopicInfo(command.getPayload()).value().concat(".results"),
              Produced.with(Serdes.String(), CustomSerdes.Json(Command.class)));

      // Events --> Push
      commandResults
          .filter((key, result) -> result instanceof CommandResult.Success)
          .mapValues((key, result) -> (CommandResult.Success) result)
          .flatMapValues(CommandResult.Success::getEvents)
          .filter((key, event) -> event != null)
          .to((key, event, recordContext) -> CommonUtils.getTopicInfo(event.getPayload()).value(),
              Produced.with(Serdes.String(), CustomSerdes.Json(Event.class)));

      // --> Events --> Event Store
      KStream<String, Event> events = builder.stream(Topics.EVENTS, Consumed.with(Serdes.String(), CustomSerdes.Json(Event.class)))
          .filter((key, event) -> key != null)
          .filter((key, event) -> event != null);
    }


    /*
     *************************************************************************************
     * Result Handling
     *************************************************************************************
     */

    // --> Results
    KStream<String, Command> results = builder.stream(Topics.RESULTS, Consumed.with(Serdes.String(), CustomSerdes.Json(Command.class)))
        .filter((key, command) -> key != null)
        .filter((key, command) -> command != null);


    return builder.build();
  }

  public void start1() {
    this.kafkaStreams = new KafkaStreams(topology(), this.streamsConfig);

    Topology topology = topology();
    if (topology.describe().subtopologies().isEmpty()) {
      log.info("Eventify is not started: consumer is not subscribed to any topics or assigned any partitions");
      return;
    }

    this.kafkaStreams = new KafkaStreams(topology, Config.streamsConfig);
    setUpListeners();

    log.info("Command Bus is starting...");
    kafkaStreams.start();
  }

  public void start2() {
    AtomicBoolean closed = new AtomicBoolean(false);
    Thread thread = new Thread(() -> {
      try {
        this.consumer.subscribe(Collections.singletonList("some-results-topic"));
        while (!closed.get()) {
          ConsumerRecords<String, Command> consumerRecords = this.consumer.poll(Duration.ofMillis(1000));
          onResult(consumerRecords);
        }
      } catch (WakeupException e) {
        // Ignore exception if closing
        if (!closed.get()) throw e;
      } finally {
        this.consumer.close();
      }
    });

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      closed.set(true);
      consumer.wakeup();
    }));
    thread.start();
  }

  public void onResult(ConsumerRecords<String, Command> consumerRecords) {
    consumerRecords.forEach(record -> {
      String messageId = record.value().getId();
      if (StringUtils.isBlank(messageId)) {
        return;
      }
      // CompletableFuture<Object> future = futures.remove(messageId);
      CompletableFuture<Object> future = cache.getIfPresent(messageId);
      if (future != null) {
        Exception exception = checkForErrors(record);
        if (exception == null) {
          future.complete(record.value().getPayload());
        } else {
          future.completeExceptionally(exception);
        }
        cache.invalidate(messageId);
      }
    });
  }

  private Exception checkForErrors(ConsumerRecord<String, Command> record) {
    Command command = record.value();
    Metadata metadata = command.getMetadata();

    if (metadata.get(Metadata.RESULT).equals("failure")) {
      return new CommandExecutionException(metadata.get(Metadata.CAUSE));
    }

    return null;
  }


  public CompletableFuture<Object> dispatch(Command command) {
    //validatePayload(payload);

    String aggregateId = command.getAggregateId();
    long timestamp = command.getTimestamp().toEpochMilli();
    Object payload = command.getPayload();
    String topic = CommonUtils.getTopicInfo(payload).value();

    ProducerRecord<String, Command> record = new ProducerRecord<>(topic, null, timestamp, aggregateId, command);

    log.debug("Sending command: {} ({})", payload.getClass().getSimpleName(), aggregateId);
    producer.send(record);

    CompletableFuture<Object> future = new CompletableFuture<>();
    cache.put(command.getId(), future);

    return future;
  }

  public void dispatch(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    String aggregateId = CommonUtils.getAggregateId(payload);
    String messageId = CommonUtils.createMessageId(aggregateId);
    Instant timestamp = Instant.now();
    String correlationId = UUID.randomUUID().toString();

    Command command = Command.builder()
        .aggregateId(aggregateId)
        .id(messageId)
        .timestamp(timestamp)
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(Metadata.CORRELATION_ID, correlationId)
            .entry("replyTO", "SOME-VALUE")
            .build())
        .build();

    dispatch(command);
  }

  public void dispatch(Object payload) {
    dispatch(payload, null);
  }

  public void setAdditionalProducerConfigs(Properties producerConfig) {
    producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
    producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
    producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//    interceptors.add(TracingProducerInterceptor.class.getName());
//
//    this.producerConfig.putIfAbsent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);
  }

  private void setUpListeners() {
    kafkaStreams.setStateListener(Config.stateListener);
    kafkaStreams.setUncaughtExceptionHandler(Config.uncaughtExceptionHandler);

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      log.info("Eventify is shutting down...");
      kafkaStreams.close(Duration.ofMillis(1000));
    }));

    kafkaStreams.setGlobalStateRestoreListener(new StateRestoreListener() {
      @Override
      public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset, long endingOffset) {
        log.debug("State restoration started: topic={}, partition={}, store={}, endingOffset={}", topicPartition.topic(), topicPartition.partition(), storeName, endingOffset);
      }

      @Override
      public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset, long numRestored) {
        log.debug("State restoration in progress: topic={}, partition={}, store={}, numRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, numRestored);
      }

      @Override
      public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
        log.debug("State restoration ended: topic={}, partition={}, store={}, totalRestored={}", topicPartition.topic(), topicPartition.partition(), storeName, totalRestored);
      }
    });
  }

}
