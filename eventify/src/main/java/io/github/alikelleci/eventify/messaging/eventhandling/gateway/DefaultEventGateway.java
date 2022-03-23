package io.github.alikelleci.eventify.messaging.eventhandling.gateway;

import io.github.alikelleci.eventify.messaging.Metadata;
import io.github.alikelleci.eventify.messaging.eventhandling.Event;
import lombok.extern.slf4j.Slf4j;

import java.util.Properties;
import java.util.UUID;

@Slf4j
public class DefaultEventGateway extends AbstractEventGateway implements EventGateway {

  protected DefaultEventGateway(Properties producerConfig) {
    super(producerConfig);
  }

  @Override
  public void publish(Event event) {
    validate(event);
    super.dispatch(event);
  }

  @Override
  public void publish(Object payload, Metadata metadata) {
    Event event = Event.builder()
        .payload(payload)
        .metadata(new Metadata(metadata)
            .add(Metadata.CORRELATION_ID, UUID.randomUUID().toString()))
        .build();


  }
}
