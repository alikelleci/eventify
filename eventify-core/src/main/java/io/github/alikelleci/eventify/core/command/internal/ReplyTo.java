package io.github.alikelleci.eventify.core.command.internal;

import io.github.alikelleci.eventify.core.command.CommandResult;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

import java.nio.charset.StandardCharsets;

/** The reply topic from the command's {@link HeaderNames#REPLY_TO} header; Kafka Streams keeps it on the result. */
@Slf4j
public final class ReplyTo {

  private ReplyTo() {
  }

  /** The topic to reply on; {@code null} when the sender waits for no reply or names an invalid topic. */
  public static String topic(Headers headers) {
    Header header = headers != null ? headers.lastHeader(HeaderNames.REPLY_TO) : null;
    if (header == null || header.value() == null) {
      return null;
    }
    String topic = new String(header.value(), StandardCharsets.UTF_8);
    if (StringUtils.isBlank(topic)) {
      return null;
    }
    try {
      Topic.validate(topic);
    } catch (InvalidTopicException e) {
      log.warn("Skipping reply, invalid reply topic: topic={}", topic);
      return null; // sending to it would stop Eventify
    }
    return topic;
  }

  /** Passes on only the results their sender waits for: the ones with a reply topic. */
  public static class OnlyAwaited implements FixedKeyProcessor<String, CommandResult, CommandResult> {

    private FixedKeyProcessorContext<String, CommandResult> context;

    @Override
    public void init(FixedKeyProcessorContext<String, CommandResult> context) {
      this.context = context;
    }

    @Override
    public void process(FixedKeyRecord<String, CommandResult> record) {
      if (topic(record.headers()) != null) {
        context.forward(record);
      }
    }
  }
}
