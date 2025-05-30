package io.github.alikelleci.eventify.core.messaging.resulthandling;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.commandhandling.Command;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;

@Slf4j
public class KafkaResultProcessor implements ResultProcessor, FixedKeyProcessor<String, Command, Command> {

  private final Eventify eventify;
  private FixedKeyProcessorContext<String, Command> context;

  public KafkaResultProcessor(Eventify eventify) {
    this.eventify = eventify;
  }

  @Override
  public Eventify getEventify() {
    return eventify;
  }

  @Override
  public void init(FixedKeyProcessorContext<String, Command> context) {
    this.context = context;
  }

  @Override
  public void process(FixedKeyRecord<String, Command> fixedKeyRecord) {
    processCommand(fixedKeyRecord.value());
    context.forward(fixedKeyRecord);
  }

  @Override
  public void close() {

  }
}
