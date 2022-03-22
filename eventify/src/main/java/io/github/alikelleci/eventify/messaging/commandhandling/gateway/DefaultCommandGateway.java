package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.messaging.commandhandling.exceptions.CommandExecutionException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Slf4j
public class DefaultCommandGateway extends AbstractCommandGateway implements CommandGateway {

  //private final Map<String, CompletableFuture<Object>> futures = new ConcurrentHashMap<>();
  private final Cache<String, CompletableFuture<Object>> cache = Caffeine.newBuilder()
      .expireAfterWrite(Duration.ofMinutes(5))
      .build();

  private final String replyTopic;

  protected DefaultCommandGateway(Properties producerConfig, Properties consumerConfig, String replyTopic) {
    super(producerConfig, consumerConfig);
    this.replyTopic = replyTopic;

    listen(replyTopic);
  }

  @Override
  public CompletableFuture<Object> send(Object payload, Metadata metadata) {
    if (metadata == null) {
      metadata = new Metadata();
    }

    Command command = Command.builder()
        .payload(payload)
        .metadata(metadata.filter()
            .add(Metadata.CORRELATION_ID, UUID.randomUUID().toString())
            .add(Metadata.REPLY_TO, replyTopic))
        .build();

    validatePayload(command);
    super.dispatch(command);

    CompletableFuture<Object> future = new CompletableFuture<>();
    cache.put(command.getId(), future);

    return future;
  }

  @Override
  public void onMessage(ConsumerRecords<String, Command> consumerRecords) {
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
    Message message = record.value();
    Metadata metadata = message.getMetadata();

    if (metadata.get(Metadata.RESULT).equals("failure")) {
      return new CommandExecutionException(metadata.get(Metadata.CAUSE));
    }

    return null;
  }
}
