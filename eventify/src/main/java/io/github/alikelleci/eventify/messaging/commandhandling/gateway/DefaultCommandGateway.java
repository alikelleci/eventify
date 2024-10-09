package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResponse;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResponse.ReplyMode;
import io.github.alikelleci.eventify.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.eventify.support.serializer.JsonSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.Instant;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static io.github.alikelleci.eventify.messaging.Metadata.CORRELATION_ID;
import static io.github.alikelleci.eventify.messaging.Metadata.REPLY_MODE;
import static io.github.alikelleci.eventify.messaging.Metadata.REPLY_TO;

@Slf4j
public class DefaultCommandGateway extends AbstractReplyListener implements CommandGateway {

  //private final Map<String, CompletableFuture<Object>> futures = new ConcurrentHashMap<>();
  private final Cache<String, CompletableFuture<Object>> cache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofMinutes(5))
      .build();

  private final Producer<String, Command> producer;
  private final ReplyMode replyMode;

  protected DefaultCommandGateway(Properties producerConfig,
                                  Properties consumerConfig,
                                  String replyTopic,
                                  ReplyMode replyMode,
                                  ObjectMapper objectMapper) {
    super(consumerConfig, replyTopic, objectMapper);

    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>(Command.class, objectMapper));

    this.replyMode = replyMode;
  }

  @Override
  public <R> CompletableFuture<R> send(Object payload, Metadata metadata, Instant timestamp) {
    String correlationId = UUID.randomUUID().toString();

    Command command = Command.builder()
        .timestamp(timestamp)
        .payload(payload)
        .metadata(Metadata.builder()
            .addAll(metadata)
            .add(CORRELATION_ID, correlationId)
            .add(REPLY_TO, getReplyTopic())
            .add(REPLY_MODE, replyMode.toString())
            .build())
        .build();

    ProducerRecord<String, Command> producerRecord = new ProducerRecord<>(command.getTopicInfo().value(), null, command.getTimestamp().toEpochMilli(), command.getAggregateId(), command);

    log.debug("Sending command: {} ({})", command.getType(), command.getAggregateId());
    producer.send(producerRecord);

    CompletableFuture<Object> future = new CompletableFuture<>();
    cache.put(correlationId, future);

    return (CompletableFuture<R>) future;
  }

  @Override
  protected void onMessage(ConsumerRecords<String, CommandResponse> consumerRecords) {
    consumerRecords.forEach(consumerRecord -> {
      String correlationId = consumerRecord.value().getCorrelationId();
      if (StringUtils.isBlank(correlationId)) {
        return;
      }
      // CompletableFuture<Object> future = futures.remove(correlationId);
      CompletableFuture<Object> future = cache.getIfPresent(correlationId);
      if (future != null) {
        Exception exception = checkForErrors(consumerRecord);
        if (exception == null) {
          future.complete(consumerRecord.value().getPayload());
        } else {
          future.completeExceptionally(exception);
        }
        cache.invalidate(correlationId);
      }
    });
  }

  private Exception checkForErrors(ConsumerRecord<String, CommandResponse> consumerRecord) {
    CommandResponse commandResponse = consumerRecord.value();
    if (!commandResponse.isSuccess()) {
      return new CommandExecutionException(commandResponse.getMessage());
    }
    return null;
  }

}
