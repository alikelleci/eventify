package io.github.alikelleci.eventify.engine.pulsar;

import io.github.alikelleci.eventify.core.Eventify;
import io.github.alikelleci.eventify.core.messaging.commandhandling.CommandProcessor;
import io.github.alikelleci.eventify.core.messaging.eventhandling.Event;
import io.github.alikelleci.eventify.core.messaging.eventsourcing.AggregateState;
import io.github.alikelleci.eventify.core.support.AutoCloseableIterator;
import java.util.Iterator;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Message;


@Slf4j
public class PulsarCommandProcessor extends CommandProcessor {

  private final Eventify eventify;

  public PulsarCommandProcessor(Eventify eventify) {
    this.eventify = eventify;
  }

  /**
   * What should we do with thrown exceptions?
   */
  public List<Event> process(Message<byte[]> msg) {
    throw new UnsupportedOperationException();

//    String key; //todo
//    Command command; //todo
//
//    return executeCommand(key, command);
  }

  @Override
  protected AggregateState loadFromSnapshot(String aggregateId) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected AutoCloseableIterator<Event> loadEvents(String aggregateId, String from, String to) {
    return null;
  }

  @Override
  protected void saveEvent(Event event) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected Eventify getEventify() {
    return eventify;
  }

  @Override
  protected void saveSnapshot(AggregateState state) {
    throw new UnsupportedOperationException();
  }

  @Override
  protected void deleteEvents(Iterator<Event> events) {
    throw new UnsupportedOperationException();
  }
}
