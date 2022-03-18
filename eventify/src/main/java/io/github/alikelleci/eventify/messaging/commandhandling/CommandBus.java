package io.github.alikelleci.eventify.messaging.commandhandling;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.constants.Handlers;
import io.github.alikelleci.eventify.constants.Topics;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.support.serializer.CustomSerdes;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import io.github.alikelleci.eventify.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class CommandBus {

  private Properties producerConfig;
  private Producer<String, Message> producer;
  private Cache<String, CompletableFuture<Object>> cache;

  private Properties streamsConfig;
  private KafkaStreams kafkaStreams;

  public CommandBus(Properties producerConfig, Properties streamsConfig) {
    this.producerConfig = producerConfig;
    setAdditionalProducerConfigs(this.producerConfig);

    this.producer = new KafkaProducer<>(this.producerConfig,
        new StringSerializer(),
        new JsonSerializer<>());

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

    }


    /*
     *************************************************************************************
     * Event Handling
     *************************************************************************************
     */

    if (!Handlers.EVENT_HANDLERS.isEmpty()) {
      // --> Events
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

  public void subscribe() {
    this.kafkaStreams = new KafkaStreams(topology(), this.streamsConfig);
  }


  public void dispatch(Command command) {
    //validatePayload(payload);

    String aggregateId = command.getAggregateId();
    long timestamp = command.getTimestamp().toEpochMilli();
    Object payload = command.getPayload();
    String topic = CommonUtils.getTopicInfo(payload).value();

    ProducerRecord<String, Message> record = new ProducerRecord<>(topic, null, timestamp, aggregateId, command);

    log.debug("Sending command: {} ({})", payload.getClass().getSimpleName(), aggregateId);
    producer.send(record);
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
}
