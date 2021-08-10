package io.github.alikelleci.eventify.messaging.commandhandling.gateway;

import io.github.alikelleci.eventify.messaging.Message;
import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.commandhandling.Command;
import io.github.alikelleci.eventify.util.CommonUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.UUID;

@Slf4j
public class DefaultCommandGateway implements CommandGateway {

  private final Producer<String, Message> producer;

  public DefaultCommandGateway(Producer<String, Message> producer) {
    this.producer = producer;
  }

  @Override
  public void send(Object payload, Metadata metadata) {
    validatePayload(payload);

    if (metadata == null) {
      metadata = Metadata.builder().build();
    }

    String aggregateId = CommonUtils.getAggregateId(payload);
    long timestamp = Instant.now().toEpochMilli();
    String messageId = CommonUtils.createMessageId(aggregateId, timestamp);
    String topic = CommonUtils.getTopicInfo(payload).value();

    Command command = Command.builder()
        .aggregateId(aggregateId)
        .messageId(messageId)
        .timestamp(timestamp)
        .payload(payload)
        .metadata(metadata.filter().toBuilder()
            .entry(Metadata.CORRELATION_ID, UUID.randomUUID().toString())
            .build())
        .build();

    ProducerRecord<String, Message> record = new ProducerRecord<>(topic, null, timestamp, aggregateId, command);

    log.debug("Sending command: {} ({})", payload.getClass().getSimpleName(), command.getAggregateId());
    producer.send(record);
  }
}
