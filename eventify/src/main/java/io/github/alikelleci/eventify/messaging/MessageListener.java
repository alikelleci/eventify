package io.github.alikelleci.eventify.messaging;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface MessageListener {

  void onMessage(ConsumerRecords<String, Message> consumerRecords);

}
