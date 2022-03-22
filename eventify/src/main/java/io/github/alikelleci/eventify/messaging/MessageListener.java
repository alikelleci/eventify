package io.github.alikelleci.eventify.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface MessageListener<M extends Message> {

  void onMessage(ConsumerRecords<String, M> consumerRecords);

}
