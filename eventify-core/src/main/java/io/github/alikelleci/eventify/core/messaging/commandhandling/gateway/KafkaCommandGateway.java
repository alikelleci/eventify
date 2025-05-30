package io.github.alikelleci.eventify.core.messaging.commandhandling.gateway;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.core.messaging.Metadata;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.core.messaging.commandhandling.exceptions.CommandExecutionException;
import io.github.alikelleci.eventify.core.support.serialization.json.JsonSerializer;
import io.github.alikelleci.eventify.core.support.serialization.json.util.JacksonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static io.github.alikelleci.eventify.core.messaging.Metadata.CAUSE;
import static io.github.alikelleci.eventify.core.messaging.Metadata.REPLY_TO;
import static io.github.alikelleci.eventify.core.messaging.Metadata.RESULT;

@Slf4j
public class KafkaCommandGateway extends AbstractKafkaCommandResultListener implements CommandGateway {

  private final Cache<String, CompletableFuture<Object>> cache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofMinutes(5))
      .build();

  private final Producer<String, Command> producer;

  protected KafkaCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic, ObjectMapper objectMapper) {
    super(consumerConfig, replyTopic, objectMapper);

    this.producer = new KafkaProducer<>(producerConfig,
        new StringSerializer(),
        new JsonSerializer<>(objectMapper));
  }

  @Override
  public <R> CompletableFuture<R> send(Command command) {
    command.getMetadata().put(REPLY_TO, getReplyTopic());

    ProducerRecord<String, Command> producerRecord = new ProducerRecord<>(command.getTopicInfo().value(), null, command.getTimestamp().toEpochMilli(), command.getAggregateId(), command);

    log.debug("Sending command: {} ({})", command.getType(), command.getAggregateId());
    producer.send(producerRecord);

    CompletableFuture<Object> future = new CompletableFuture<>();
    cache.put(command.getId(), future);

    return (CompletableFuture<R>) future;
  }

  @Override
  protected void onMessage(ConsumerRecords<String, Command> consumerRecords) {
    consumerRecords.forEach(consumerRecord -> {
      String messageId = consumerRecord.value().getId();
      if (StringUtils.isBlank(messageId)) {
        return;
      }
      CompletableFuture<Object> future = cache.getIfPresent(messageId);
      if (future != null) {
        Exception exception = checkForErrors(consumerRecord);
        if (exception == null) {
          future.complete(consumerRecord.value().getPayload());
        } else {
          future.completeExceptionally(exception);
        }
        cache.invalidate(messageId);
      }
    });
  }

  private Exception checkForErrors(ConsumerRecord<String, Command> consumerRecord) {
    Command command = consumerRecord.value();
    Metadata metadata = command.getMetadata();

    if (metadata.get(RESULT).equals("failure")) {
      return new CommandExecutionException(metadata.get(CAUSE));
    }

    return null;
  }

  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Properties producerConfig;
    private Properties consumerConfig;
    private String replyTopic;
    private ObjectMapper objectMapper;

    public Builder producerConfig(Properties producerConfig) {
      this.producerConfig = producerConfig;
      this.producerConfig.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
      this.producerConfig.putIfAbsent(ProducerConfig.ACKS_CONFIG, "all");
      this.producerConfig.putIfAbsent(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
      this.producerConfig.putIfAbsent(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
      this.producerConfig.putIfAbsent(ProducerConfig.COMPRESSION_TYPE_CONFIG, "zstd");

//    ArrayList<String> interceptors = new ArrayList<>();
//    interceptors.add(CommonProducerInterceptor.class.getName());
//    interceptors.add(TracingProducerInterceptor.class.getName());
//
//    this.producerConfig.putIfAbsent(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

      return this;
    }

//    public Builder consumerConfig(Properties consumerConfig) {
//      this.consumerConfig = consumerConfig;
//      return this;
//    }

    public Builder replyTopic(String replyTopic) {
      this.replyTopic = replyTopic;
      return this;
    }

    public Builder objectMapper(ObjectMapper objectMapper) {
      this.objectMapper = objectMapper;
      return this;
    }

    public KafkaCommandGateway build() {
      this.consumerConfig = new Properties();

      String bootstrapServers = this.producerConfig.getProperty(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG);
      if (StringUtils.isNotBlank(bootstrapServers)) {
        this.consumerConfig.putIfAbsent(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      }

      String securityProtocol = this.producerConfig.getProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
      if (StringUtils.isNotBlank(securityProtocol)) {
        this.consumerConfig.putIfAbsent(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol);
      }

      if (this.objectMapper == null) {
        this.objectMapper = JacksonUtils.enhancedObjectMapper();
      }

      return new KafkaCommandGateway(
          this.producerConfig,
          this.consumerConfig,
          this.replyTopic,
          this.objectMapper);
    }
  }
}
