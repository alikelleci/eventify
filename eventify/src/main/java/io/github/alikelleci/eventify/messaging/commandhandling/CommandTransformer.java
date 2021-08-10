package io.github.alikelleci.eventify.messaging.commandhandling;

import io.github.alikelleci.eventify.constants.Handlers;
import io.github.alikelleci.eventify.messaging.Repository;
import io.github.alikelleci.eventify.messaging.commandhandling.CommandResult.Success;
import io.github.alikelleci.eventify.messaging.eventsourcing.Aggregate;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
import org.apache.kafka.streams.processor.ProcessorContext;


@Slf4j
public class CommandTransformer implements ValueTransformerWithKey<String, Command, CommandResult> {

  private Repository repository;

  @Override
  public void init(ProcessorContext processorContext) {
    this.repository = new Repository(processorContext);
  }

  @Override
  public CommandResult transform(String key, Command command) {
    CommandHandler commandHandler = Handlers.COMMAND_HANDLERS.get(command.getPayload().getClass());
    if (commandHandler == null) {
      return null;
    }

    // 1. Load aggregate state
    Aggregate aggregate = repository.loadAggregate(key);

    // 2. Validate command against aggregate
    CommandResult result = commandHandler.apply(aggregate, command);

    if (result instanceof Success) {
      // 3. Save events
      ((Success) result).getEvents().forEach(event ->
          repository.saveEvent(event));

      // 4. Save snapshot if needed
      if (aggregate != null && aggregate.getSnapshotTreshold() > 0) {
        if (aggregate.getVersion() % aggregate.getSnapshotTreshold() == 0) {
          log.debug("Creating new snapshot: {}", aggregate);
          repository.saveSnapshot(aggregate);
        }
      }
    }

    return result;
  }

  @Override
  public void close() {

  }
}
