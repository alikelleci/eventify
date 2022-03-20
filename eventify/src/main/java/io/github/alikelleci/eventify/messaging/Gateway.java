package io.github.alikelleci.eventify.messaging;

import io.github.alikelleci.eventify.common.annotations.TopicInfo;
import io.github.alikelleci.eventify.common.exceptions.AggregateIdMissingException;
import io.github.alikelleci.eventify.common.exceptions.PayloadMissingException;
import io.github.alikelleci.eventify.common.exceptions.TopicInfoMissingException;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Instant;
import java.util.UUID;

public interface Gateway {

  default void validatePayload(Object payload) {
//    if (payload == null) {
//      throw new PayloadMissingException("You are trying to dispatch a message without a payload.");
//    }
//
//    TopicInfo topicInfo = CommonUtils.getTopicInfo(payload);
//    if (topicInfo == null) {
//      throw new TopicInfoMissingException("You are trying to dispatch a message without any topic information. Please annotate your message with @TopicInfo.");
//    }
//
//    String aggregateId = CommonUtils.getAggregateId(payload);
//    if (aggregateId == null) {
//      throw new AggregateIdMissingException("You are trying to dispatch a message without a proper identifier. Please annotate your field containing the identifier with @AggregateId.");
//    }
  }

}
